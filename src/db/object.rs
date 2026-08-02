use crate::model::{ListingRecord, ObjectCondition, ObjectMetadata, ObjectRecord, key_successor};
use anyhow::{Result, anyhow, bail};
use sqlx::Row;

use super::Db;
use super::support::{
    ensure_blocks_not_queued, load_object_blocks, now, parse_metadata, remove_existing_object,
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

    /// Lists keys under `prefix` starting from the `lower` bound (`>=` when
    /// `lower_inclusive`, `>` otherwise). Plain range comparisons keep the
    /// query on the `(bucket, object_key)` index; the prefix constraint is the
    /// half-open range `[prefix, key_successor(prefix))`.
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        lower: &str,
        lower_inclusive: bool,
        limit: usize,
    ) -> Result<Vec<ListingRecord>> {
        let (lower, lower_inclusive) = if lower < prefix {
            (prefix, true)
        } else {
            (lower, lower_inclusive)
        };
        let upper = if prefix.is_empty() {
            None
        } else {
            key_successor(prefix)
        };
        let comparison = if lower_inclusive { ">=" } else { ">" };
        let mut sql = format!(
            "SELECT object_key, size, etag, modified_at
             FROM objects
             WHERE bucket = ?1 AND object_key {comparison} ?2"
        );
        if upper.is_some() {
            sql.push_str(" AND object_key < ?3 ORDER BY object_key LIMIT ?4");
        } else {
            sql.push_str(" ORDER BY object_key LIMIT ?3");
        }
        let mut query = sqlx::query(&sql).bind(bucket).bind(lower);
        if let Some(upper) = &upper {
            query = query.bind(upper);
        }
        let rows = query.bind(limit as i64).fetch_all(&self.pool).await?;
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
        let Some((object_id, old_blocks)) =
            remove_existing_object(&mut tx, bucket, key, condition).await?
        else {
            tx.rollback().await?;
            return Ok(None);
        };
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
        let old_blocks = remove_existing_object(&mut tx, bucket, key, condition)
            .await?
            .map(|(_, blocks)| blocks)
            .unwrap_or_default();
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
