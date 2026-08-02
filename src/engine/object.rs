use super::{Engine, now};
use crate::limits::check_size;
use crate::model::{ObjectCondition, ObjectMetadata, ObjectRecord, UploadRecord};
use anyhow::{Result, anyhow, bail};
use axum::body::Body;
use std::collections::HashSet;
use uuid::Uuid;

impl Engine {
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Body,
        metadata: ObjectMetadata,
        expected_length: Option<i64>,
        condition: ObjectCondition,
    ) -> Result<ObjectRecord> {
        self.require_bucket(bucket).await?;
        check_size(expected_length, self.config.max_object_size)?;
        let upload_id = format!("put-{}", Uuid::new_v4());
        self.db
            .create_upload(&UploadRecord {
                upload_id: upload_id.clone(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                metadata,
                kind: "put".to_string(),
                created_at: now(),
            })
            .await?;
        let result = async {
            let (part, actual_length) = self
                .upload_stream(&upload_id, 0, body, expected_length)
                .await?;
            if let Some(expected) = expected_length
                && expected != actual_length
            {
                bail!(
                    "request Content-Length {expected} does not match body length {actual_length}"
                );
            }
            let released_blocks = match self
                .db
                .replace_part(&upload_id, 0, actual_length, &part.etag, &part.blocks)
                .await
            {
                Ok(released_blocks) => released_blocks,
                Err(error) => {
                    crate::transfer::cleanup_block_refs(
                        &self.db,
                        &self.telegram,
                        part.blocks.clone(),
                    )
                    .await;
                    return Err(error);
                }
            };
            self.reclaim_blocks(released_blocks).await;
            let etag = part.etag.clone();
            let (object, released_blocks) = self
                .db
                .commit_upload(&upload_id, actual_length, &etag, &[part], &condition)
                .await?
                .ok_or_else(|| anyhow!("upload disappeared"))?;
            self.reclaim_blocks(released_blocks).await;
            Ok(object)
        }
        .await;
        if result.is_err() {
            match self.db.abort_upload_with_blocks(&upload_id).await {
                Ok(Some(blocks)) => self.reclaim_blocks(blocks).await,
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(upload_id, error = %error, "failed to abort failed object upload");
                }
            }
        }
        result
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectRecord> {
        self.require_bucket(bucket).await?;
        let object = self
            .db
            .get_object(bucket, key)
            .await?
            .ok_or_else(|| anyhow!("NoSuchKey"))?;
        validate_object_layout(&object)?;
        Ok(object)
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        condition: &ObjectCondition,
    ) -> Result<bool> {
        self.require_bucket(bucket).await?;
        let deleted = self.db.delete_object(bucket, key, condition).await?;
        let Some(deleted) = deleted else {
            return Ok(false);
        };
        self.reclaim_blocks(deleted.blocks).await;
        Ok(true)
    }

    pub async fn copy_object(
        &self,
        source_bucket: &str,
        source_key: &str,
        bucket: &str,
        key: &str,
        metadata: &ObjectMetadata,
        condition: &ObjectCondition,
    ) -> Result<ObjectRecord> {
        self.require_bucket(bucket).await?;
        let source = self.get_object(source_bucket, source_key).await?;
        let (object, released_blocks) = self
            .db
            .copy_object(&source, bucket, key, metadata, condition)
            .await?;
        self.reclaim_blocks(released_blocks).await;
        Ok(object)
    }
}

pub(super) fn validate_object_layout(object: &ObjectRecord) -> Result<()> {
    if object.size < 0 {
        bail!("object storage layout is invalid");
    }
    let mut offset = 0_i64;
    let mut block_ids = HashSet::new();
    for block in &object.blocks {
        if block.size <= 0 || block.offset != offset || !block_ids.insert(block.id) {
            bail!("object storage layout is invalid");
        }
        offset = offset
            .checked_add(block.size)
            .ok_or_else(|| anyhow!("object storage layout is invalid"))?;
    }
    if offset != object.size {
        bail!("object storage layout is invalid");
    }
    Ok(())
}
