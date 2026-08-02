use crate::model::{ListingRecord, ObjectCondition, ObjectMetadata, ObjectRecord};
use anyhow::{Result, anyhow, bail};
use sqlx::Row;

use super::Db;
use super::support::{
    decrement_block, ensure_blocks_not_queued, escape_glob, load_object_blocks,
    load_object_blocks_tx, now, parse_metadata,
};

impl Db {
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectRecord>> {
        let row = sqlx::query(
            "SELECT id, size, etag, metadata_json, created_at, modified_at
             FROM objects WHERE bucket = ?1 AND object_key = ?2",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let id: i64 = row.get("id");
        let blocks = load_object_blocks(&self.pool, id).await?;
        Ok(Some(ObjectRecord {
            id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: row.get("size"),
            etag: row.get("etag"),
            metadata: parse_metadata(&row.get::<String, _>("metadata_json"))?,
            created_at: row.get("created_at"),
            modified_at: row.get("modified_at"),
            blocks,
        }))
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<ListingRecord>> {
        let pattern = format!("{}*", escape_glob(prefix));
        let rows = sqlx::query(
            "SELECT object_key, size, etag, modified_at
             FROM objects
             WHERE bucket = ?1 AND object_key > ?2 AND object_key GLOB ?3
             ORDER BY object_key LIMIT ?4",
        )
        .bind(bucket)
        .bind(after)
        .bind(pattern)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| ListingRecord {
                key: row.get("object_key"),
                size: row.get("size"),
                etag: row.get("etag"),
                modified_at: row.get("modified_at"),
            })
            .collect())
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        condition: &ObjectCondition,
    ) -> Result<Option<ObjectRecord>> {
        let mut tx = self.pool.begin().await?;
        let object =
            sqlx::query("SELECT id, etag FROM objects WHERE bucket = ?1 AND object_key = ?2")
                .bind(bucket)
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?;
        let object_etag = object.as_ref().map(|row| row.get::<String, _>("etag"));
        if !condition.allows(object_etag.as_deref()) {
            bail!("PreconditionFailed");
        }
        let Some(object_id) = object else {
            tx.rollback().await?;
            return Ok(None);
        };
        let object_id: i64 = object_id.get("id");
        let old_blocks = load_object_blocks_tx(&mut tx, object_id).await?;
        sqlx::query("DELETE FROM objects WHERE id = ?1")
            .bind(object_id)
            .execute(&mut *tx)
            .await?;
        for block in &old_blocks {
            decrement_block(&mut tx, block.id).await?;
        }
        tx.commit().await?;
        Ok(Some(ObjectRecord {
            id: object_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: 0,
            etag: String::new(),
            metadata: ObjectMetadata::default(),
            created_at: 0,
            modified_at: 0,
            blocks: old_blocks,
        }))
    }

    pub async fn copy_object(
        &self,
        source: &ObjectRecord,
        bucket: &str,
        key: &str,
        metadata: &ObjectMetadata,
        condition: &ObjectCondition,
    ) -> Result<(ObjectRecord, Vec<crate::model::BlockRef>)> {
        let mut tx = self.pool.begin().await?;
        let source_lock = sqlx::query(
            "UPDATE objects SET modified_at = modified_at
             WHERE id = ?1 AND bucket = ?2 AND object_key = ?3",
        )
        .bind(source.id)
        .bind(&source.bucket)
        .bind(&source.key)
        .execute(&mut *tx)
        .await?;
        if source_lock.rows_affected() != 1 {
            bail!("NoSuchKey");
        }
        let source_block_ids = source
            .blocks
            .iter()
            .map(|block| block.id)
            .collect::<Vec<_>>();
        ensure_blocks_not_queued(&mut tx, &source_block_ids).await?;
        let mut old_blocks = Vec::new();
        let old = sqlx::query("SELECT id, etag FROM objects WHERE bucket = ?1 AND object_key = ?2")
            .bind(bucket)
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;
        let old_etag = old.as_ref().map(|row| row.get::<String, _>("etag"));
        if !condition.allows(old_etag.as_deref()) {
            bail!("PreconditionFailed");
        }
        if let Some(old_id) = old {
            let old_id: i64 = old_id.get("id");
            old_blocks = load_object_blocks_tx(&mut tx, old_id).await?;
            sqlx::query("DELETE FROM objects WHERE id = ?1")
                .bind(old_id)
                .execute(&mut *tx)
                .await?;
            for block in &old_blocks {
                decrement_block(&mut tx, block.id).await?;
            }
        }
        let timestamp = now();
        let object_row = sqlx::query(
            "INSERT INTO objects
             (bucket, object_key, size, etag, metadata_json, created_at, modified_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
             RETURNING id",
        )
        .bind(bucket)
        .bind(key)
        .bind(source.size)
        .bind(&source.etag)
        .bind(serde_json::to_string(metadata)?)
        .bind(timestamp)
        .bind(timestamp)
        .fetch_one(&mut *tx)
        .await?;
        let object_id: i64 = object_row.get("id");
        for block in &source.blocks {
            sqlx::query(
                "UPDATE telegram_blocks SET ref_count = ref_count + 1, state = 'committed'
                 WHERE id = ?1",
            )
            .bind(block.id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM gc_queue WHERE block_id = ?1")
                .bind(block.id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "INSERT INTO object_blocks
                 (object_id, ordinal, block_id, byte_offset, size)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
            )
            .bind(object_id)
            .bind(block.ordinal)
            .bind(block.id)
            .bind(block.offset)
            .bind(block.size)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        let object = self
            .get_object(bucket, key)
            .await?
            .ok_or_else(|| anyhow!("copy result disappeared"))?;
        Ok((object, old_blocks))
    }
}
