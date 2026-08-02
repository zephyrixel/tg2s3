use crate::model::{BlockRef, ObjectCondition, ObjectRecord, PartRecord, UploadRecord};
use anyhow::{Result, anyhow, bail};
use sqlx::Row;
use std::collections::HashMap;

use super::Db;
use super::support::{
    block_from_row, decrement_block, ensure_blocks_not_queued, load_object_blocks_tx, now,
    upload_from_row, validate_parts_layout,
};

impl Db {
    pub async fn create_upload(&self, upload: &UploadRecord) -> Result<()> {
        let result = sqlx::query(
            "INSERT INTO multipart_uploads
             (upload_id, bucket, object_key, metadata_json, kind, created_at)
             SELECT ?1, ?2, ?3, ?4, ?5, ?6
             WHERE EXISTS(SELECT 1 FROM buckets WHERE name = ?2)",
        )
        .bind(&upload.upload_id)
        .bind(&upload.bucket)
        .bind(&upload.key)
        .bind(serde_json::to_string(&upload.metadata)?)
        .bind(&upload.kind)
        .bind(upload.created_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            bail!("NoSuchBucket");
        }
        Ok(())
    }

    pub async fn get_upload(&self, upload_id: &str) -> Result<Option<UploadRecord>> {
        let row = sqlx::query(
            "SELECT upload_id, bucket, object_key, metadata_json, kind, created_at
             FROM multipart_uploads WHERE upload_id = ?1",
        )
        .bind(upload_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(upload_from_row).transpose()
    }

    pub async fn list_uploads(&self, bucket: &str, key: Option<&str>) -> Result<Vec<UploadRecord>> {
        let rows = if let Some(key) = key {
            sqlx::query(
                "SELECT upload_id, bucket, object_key, metadata_json, kind, created_at
                 FROM multipart_uploads WHERE bucket = ?1 AND object_key = ?2 ORDER BY created_at",
            )
            .bind(bucket)
            .bind(key)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT upload_id, bucket, object_key, metadata_json, kind, created_at
                 FROM multipart_uploads WHERE bucket = ?1 ORDER BY created_at",
            )
            .bind(bucket)
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(upload_from_row).collect()
    }

    pub async fn add_staged_block(&self, block: &BlockRef) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO telegram_blocks
             (chat_id, message_id, backend, document_id, file_id, file_unique_id, size, message_date, ref_count, state, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, 'staged', ?9)
             RETURNING id",
        )
        .bind(block.chat_id)
        .bind(block.message_id)
        .bind(block.backend.as_str())
        .bind(block.document_id)
        .bind(&block.file_id)
        .bind(&block.file_unique_id)
        .bind(block.size)
        .bind(block.message_date)
        .bind(now())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get("id"))
    }

    pub async fn replace_part(
        &self,
        upload_id: &str,
        part_number: i32,
        size: i64,
        etag: &str,
        block_ids: &[BlockRef],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let upload_lock = sqlx::query(
            "UPDATE multipart_uploads SET created_at = created_at WHERE upload_id = ?1",
        )
        .bind(upload_id)
        .execute(&mut *tx)
        .await?;
        if upload_lock.rows_affected() != 1 {
            bail!("NoSuchUpload");
        }
        let queued_ids = block_ids.iter().map(|block| block.id).collect::<Vec<_>>();
        ensure_blocks_not_queued(&mut tx, &queued_ids).await?;
        let old = sqlx::query(
            "SELECT block_id FROM multipart_part_blocks
             WHERE upload_id = ?1 AND part_number = ?2",
        )
        .bind(upload_id)
        .bind(part_number)
        .fetch_all(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = ?1 AND part_number = ?2")
            .bind(upload_id)
            .bind(part_number)
            .execute(&mut *tx)
            .await?;
        for row in old {
            decrement_block(&mut tx, row.get("block_id")).await?;
        }
        sqlx::query(
            "INSERT INTO multipart_parts(upload_id, part_number, size, etag)
             VALUES(?1, ?2, ?3, ?4)",
        )
        .bind(upload_id)
        .bind(part_number)
        .bind(size)
        .bind(etag)
        .execute(&mut *tx)
        .await?;
        for (ordinal, block) in block_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO multipart_part_blocks
                 (upload_id, part_number, ordinal, block_id, byte_offset, size)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .bind(upload_id)
            .bind(part_number)
            .bind(ordinal as i64)
            .bind(block.id)
            .bind(block.offset)
            .bind(block.size)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_parts(
        &self,
        upload_id: &str,
        include_internal: bool,
    ) -> Result<Vec<PartRecord>> {
        let query = if include_internal {
            "SELECT upload_id, part_number, size, etag FROM multipart_parts
             WHERE upload_id = ?1 ORDER BY part_number"
        } else {
            "SELECT upload_id, part_number, size, etag FROM multipart_parts
             WHERE upload_id = ?1 AND part_number > 0 ORDER BY part_number"
        };
        let rows = sqlx::query(query)
            .bind(upload_id)
            .fetch_all(&self.pool)
            .await?;
        let block_rows = sqlx::query(
            "SELECT b.id, pb.ordinal, pb.byte_offset, pb.size, b.chat_id, b.message_id,
                    b.backend, b.document_id, b.file_id, b.file_unique_id, b.message_date,
                    pb.part_number
             FROM multipart_part_blocks pb JOIN telegram_blocks b ON b.id = pb.block_id
             WHERE pb.upload_id = ?1 AND (?2 OR pb.part_number > 0)
             ORDER BY pb.part_number, pb.ordinal",
        )
        .bind(upload_id)
        .bind(include_internal)
        .fetch_all(&self.pool)
        .await?;
        let mut blocks_by_part: HashMap<i32, Vec<BlockRef>> = HashMap::new();
        for row in block_rows {
            let part_number: i32 = row.get("part_number");
            blocks_by_part
                .entry(part_number)
                .or_default()
                .push(block_from_row(row)?);
        }
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let part_number: i32 = row.get("part_number");
            result.push(PartRecord {
                blocks: blocks_by_part.remove(&part_number).unwrap_or_default(),
                upload_id: row.get("upload_id"),
                part_number,
                size: row.get("size"),
                etag: row.get("etag"),
            });
        }
        Ok(result)
    }

    pub async fn commit_upload(
        &self,
        upload_id: &str,
        size: i64,
        etag: &str,
        parts: &[PartRecord],
        condition: &ObjectCondition,
    ) -> Result<Option<(ObjectRecord, Vec<BlockRef>)>> {
        if validate_parts_layout(parts)? != size {
            bail!("object storage layout is invalid");
        }
        let mut tx = self.pool.begin().await?;
        let upload_lock = sqlx::query(
            "UPDATE multipart_uploads SET created_at = created_at WHERE upload_id = ?1",
        )
        .bind(upload_id)
        .execute(&mut *tx)
        .await?;
        if upload_lock.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(None);
        }
        let block_ids = parts
            .iter()
            .flat_map(|part| part.blocks.iter().map(|block| block.id))
            .collect::<Vec<_>>();
        ensure_blocks_not_queued(&mut tx, &block_ids).await?;
        let upload = sqlx::query(
            "SELECT upload_id, bucket, object_key, metadata_json, kind, created_at
             FROM multipart_uploads WHERE upload_id = ?1",
        )
        .bind(upload_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(upload) = upload else {
            tx.rollback().await?;
            return Ok(None);
        };
        let bucket: String = upload.get("bucket");
        let key: String = upload.get("object_key");
        let metadata_json: String = upload.get("metadata_json");
        let created_at: i64 = upload.get("created_at");
        let mut old_blocks = Vec::new();
        let old = sqlx::query("SELECT id, etag FROM objects WHERE bucket = ?1 AND object_key = ?2")
            .bind(&bucket)
            .bind(&key)
            .fetch_optional(&mut *tx)
            .await?;
        let old_etag = old.as_ref().map(|row| row.get::<String, _>("etag"));
        if !condition.allows(old_etag.as_deref()) {
            bail!("PreconditionFailed");
        }
        if let Some(old_id) = old {
            let object_id: i64 = old_id.get("id");
            old_blocks = load_object_blocks_tx(&mut tx, object_id).await?;
            sqlx::query("DELETE FROM objects WHERE id = ?1")
                .bind(object_id)
                .execute(&mut *tx)
                .await?;
            for block in &old_blocks {
                decrement_block(&mut tx, block.id).await?;
            }
        }
        let modified_at = now();
        let object_row = sqlx::query(
            "INSERT INTO objects
             (bucket, object_key, size, etag, metadata_json, created_at, modified_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
             RETURNING id",
        )
        .bind(&bucket)
        .bind(&key)
        .bind(size)
        .bind(etag)
        .bind(metadata_json)
        .bind(created_at)
        .bind(modified_at)
        .fetch_one(&mut *tx)
        .await?;
        let object_id: i64 = object_row.get("id");
        let mut ordinal = 0_i64;
        let mut offset = 0_i64;
        for part in parts {
            for block in &part.blocks {
                sqlx::query(
                    "INSERT INTO object_blocks
                     (object_id, ordinal, block_id, byte_offset, size)
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                )
                .bind(object_id)
                .bind(ordinal)
                .bind(block.id)
                .bind(offset)
                .bind(block.size)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "UPDATE telegram_blocks SET
                         ref_count = CASE WHEN ref_count < 1 THEN 1 ELSE ref_count END,
                         state = 'committed'
                     WHERE id = ?1",
                )
                .bind(block.id)
                .execute(&mut *tx)
                .await?;
                sqlx::query("DELETE FROM gc_queue WHERE block_id = ?1")
                    .bind(block.id)
                    .execute(&mut *tx)
                    .await?;
                ordinal += 1;
                offset += block.size;
            }
        }
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?1")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        let object = self
            .get_object(&bucket, &key)
            .await?
            .ok_or_else(|| anyhow!("object disappeared after commit"))?;
        Ok(Some((object, old_blocks)))
    }

    pub async fn abort_upload(&self, upload_id: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query("SELECT 1 FROM multipart_uploads WHERE upload_id = ?1")
            .bind(upload_id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
        if !exists {
            tx.rollback().await?;
            return Ok(false);
        }
        let ids = sqlx::query("SELECT block_id FROM multipart_part_blocks WHERE upload_id = ?1")
            .bind(upload_id)
            .fetch_all(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?1")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        for row in ids {
            decrement_block(&mut tx, row.get("block_id")).await?;
        }
        tx.commit().await?;
        Ok(true)
    }
}
