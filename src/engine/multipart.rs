use super::{Engine, MAX_MULTIPART_PARTS, MIN_MULTIPART_PART, now};
use crate::limits::check_size;
use crate::model::{
    ObjectCondition, ObjectMetadata, ObjectRecord, PartRecord, UploadRecord, normalize_etag,
};
use anyhow::{Result, anyhow, bail};
use axum::body::Body;
use md5::{Digest, Md5};
use uuid::Uuid;

impl Engine {
    pub async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        metadata: ObjectMetadata,
    ) -> Result<String> {
        self.require_bucket(bucket).await?;
        let upload_id = Uuid::new_v4().to_string();
        self.db
            .create_upload(&UploadRecord {
                upload_id: upload_id.clone(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                metadata,
                kind: "multipart".to_string(),
                created_at: now(),
            })
            .await?;
        Ok(upload_id)
    }

    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        body: Body,
        expected_length: Option<i64>,
    ) -> Result<PartRecord> {
        if !(1..=10_000).contains(&part_number) {
            bail!("part number must be between 1 and 10,000");
        }
        let upload = self
            .db
            .get_upload(upload_id)
            .await?
            .ok_or_else(|| anyhow!("NoSuchUpload"))?;
        if upload.kind != "multipart" {
            bail!("NoSuchUpload");
        }
        check_size(expected_length, self.config.max_object_size)?;
        let (part, size) = self
            .upload_stream(upload_id, part_number, body, expected_length)
            .await?;
        let released_blocks = match self
            .db
            .replace_part(upload_id, part_number, size, &part.etag, &part.blocks)
            .await
        {
            Ok(released_blocks) => released_blocks,
            Err(error) => {
                crate::transfer::cleanup_block_refs(&self.db, &self.telegram, part.blocks.clone())
                    .await;
                return Err(error);
            }
        };
        self.reclaim_blocks(released_blocks).await;
        Ok(PartRecord {
            upload_id: upload_id.to_string(),
            part_number,
            size,
            etag: part.etag,
            blocks: part.blocks,
        })
    }

    pub async fn complete_multipart(
        &self,
        upload_id: &str,
        requested: &[(i32, String)],
    ) -> Result<ObjectRecord> {
        let upload = self
            .db
            .get_upload(upload_id)
            .await?
            .ok_or_else(|| anyhow!("NoSuchUpload"))?;
        if upload.kind != "multipart" {
            bail!("NoSuchUpload");
        }
        if requested.is_empty() || requested.len() > MAX_MULTIPART_PARTS {
            bail!("InvalidPart");
        }
        for window in requested.windows(2) {
            if window[0].0 >= window[1].0 {
                bail!("InvalidPartOrder");
            }
        }
        let stored = self.db.get_parts(upload_id, false).await?;
        if stored.len() != requested.len() {
            bail!("InvalidPart");
        }
        let mut ordered = Vec::with_capacity(requested.len());
        let mut total_size = 0_i64;
        let mut composite = Md5::new();
        for (index, (part_number, requested_etag)) in requested.iter().enumerate() {
            let part = stored.get(index).ok_or_else(|| anyhow!("InvalidPart"))?;
            if part.part_number != *part_number
                || normalize_etag(&part.etag) != normalize_etag(requested_etag)
            {
                bail!("InvalidPart");
            }
            if index + 1 != requested.len() && part.size < MIN_MULTIPART_PART {
                bail!("EntityTooSmall");
            }
            let digest =
                hex::decode(normalize_etag(&part.etag)).map_err(|_| anyhow!("InvalidPart"))?;
            if digest.len() != 16 {
                bail!("InvalidPart");
            }
            composite.update(digest);
            total_size = total_size
                .checked_add(part.size)
                .ok_or_else(|| anyhow!("multipart object is too large"))?;
            if total_size > self.config.max_object_size {
                bail!("EntityTooLarge");
            }
            ordered.push(part.clone());
        }
        let etag = format!("{:x}-{}", composite.finalize(), requested.len());
        let (object, released_blocks) = self
            .db
            .commit_upload(
                upload_id,
                total_size,
                &etag,
                &ordered,
                &ObjectCondition::default(),
            )
            .await?
            .ok_or_else(|| anyhow!("NoSuchUpload"))?;
        self.reclaim_blocks(released_blocks).await;
        Ok(object)
    }

    pub async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartRecord>> {
        if self.db.get_upload(upload_id).await?.is_none() {
            bail!("NoSuchUpload");
        }
        self.db.get_parts(upload_id, false).await
    }

    pub async fn list_uploads(&self, bucket: &str, key: Option<&str>) -> Result<Vec<UploadRecord>> {
        self.require_bucket(bucket).await?;
        self.db.list_uploads(bucket, key).await
    }

    pub async fn abort_multipart(&self, upload_id: &str) -> Result<bool> {
        let released_blocks = self.db.abort_upload_with_blocks(upload_id).await?;
        let Some(released_blocks) = released_blocks else {
            return Ok(false);
        };
        self.reclaim_blocks(released_blocks).await;
        Ok(true)
    }
}
