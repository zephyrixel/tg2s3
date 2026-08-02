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
            if let Err(error) = self
                .db
                .replace_part(&upload_id, 0, actual_length, &part.etag, &part.blocks)
                .await
            {
                crate::transfer::cleanup_block_refs(&self.db, &self.telegram, part.blocks.clone())
                    .await;
                return Err(error);
            }
            let etag = part.etag.clone();
            let committed = self
                .db
                .commit_upload(&upload_id, actual_length, &etag, &[part], &condition)
                .await?
                .ok_or_else(|| anyhow!("upload disappeared"))?;
            Ok(committed.0)
        }
        .await;
        if result.is_err() {
            if let Err(error) = self.db.abort_upload(&upload_id).await {
                tracing::error!(upload_id, error = %error, "failed to abort failed object upload");
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
        Ok(self
            .db
            .delete_object(bucket, key, condition)
            .await?
            .is_some())
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
        Ok(self
            .db
            .copy_object(&source, bucket, key, metadata, condition)
            .await?
            .0)
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
