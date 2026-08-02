use crate::model::{BlockRef, ObjectCondition, ObjectRecord, PartRecord, UploadRecord};
use anyhow::{Result, anyhow, bail};
use sqlx::Row;
use std::collections::{HashMap, HashSet};

use super::Db;
use super::support::{
    block_from_row, decrement_block, ensure_blocks_not_queued, now, remove_existing_object,
    upload_from_row, validate_parts_layout, verify_parts_tx,
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
    ) -> Result<Vec<BlockRef>> {
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
        let mut new_block_ids = HashSet::with_capacity(block_ids.len());
        if block_ids
            .iter()
            .any(|block| !new_block_ids.insert(block.id))
        {
            bail!("InvalidPart");
        }
        let old = sqlx::query(
            "SELECT b.id, pb.ordinal, pb.byte_offset, pb.size, b.chat_id, b.message_id,
                    b.backend, b.document_id, b.file_id, b.file_unique_id, b.message_date
             FROM multipart_part_blocks pb JOIN telegram_blocks b ON b.id = pb.block_id
             WHERE pb.upload_id = ?1 AND pb.part_number = ?2 ORDER BY pb.ordinal",
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
        let mut released = Vec::new();
        for row in old {
            let block = block_from_row(row)?;
            if !new_block_ids.contains(&block.id) {
                decrement_block(&mut tx, block.id).await?;
                released.push(block);
            }
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
        Ok(released)
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
        verify_parts_tx(&mut tx, upload_id, parts).await?;
        let block_ids = parts
            .iter()
            .flat_map(|part| part.blocks.iter().map(|block| block.id))
            .collect::<Vec<_>>();
        ensure_blocks_not_queued(&mut tx, &block_ids).await?;
        let bucket: String = upload.get("bucket");
        let key: String = upload.get("object_key");
        let metadata_json: String = upload.get("metadata_json");
        let created_at: i64 = upload.get("created_at");
        let mut old_blocks = remove_existing_object(&mut tx, &bucket, &key, condition)
            .await?
            .map(|(_, blocks)| blocks)
            .unwrap_or_default();
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
        // Release blocks of uploaded parts that the completion did not use so
        // they are garbage collected instead of leaking in Telegram.
        let used_parts: HashSet<i32> = parts.iter().map(|part| part.part_number).collect();
        let unused_rows = sqlx::query(
            "SELECT b.id, pb.ordinal, pb.byte_offset, pb.size, b.chat_id, b.message_id,
                    b.backend, b.document_id, b.file_id, b.file_unique_id, b.message_date,
                    pb.part_number
             FROM multipart_part_blocks pb JOIN telegram_blocks b ON b.id = pb.block_id
             WHERE pb.upload_id = ?1 ORDER BY pb.part_number, pb.ordinal",
        )
        .bind(upload_id)
        .fetch_all(&mut *tx)
        .await?;
        for row in unused_rows {
            let part_number: i32 = row.get("part_number");
            if used_parts.contains(&part_number) {
                continue;
            }
            let block = block_from_row(row)?;
            decrement_block(&mut tx, block.id).await?;
            old_blocks.push(block);
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
        Ok(self.abort_upload_with_blocks(upload_id).await?.is_some())
    }

    pub async fn abort_upload_with_blocks(&self, upload_id: &str) -> Result<Option<Vec<BlockRef>>> {
        let mut tx = self.pool.begin().await?;
        let upload_lock = sqlx::query(
            "UPDATE multipart_uploads SET created_at = created_at WHERE upload_id = ?1",
        )
        .bind(upload_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if upload_lock != 1 {
            tx.rollback().await?;
            return Ok(None);
        }
        let blocks = sqlx::query(
            "SELECT b.id, pb.ordinal, pb.byte_offset, pb.size, b.chat_id, b.message_id,
                    b.backend, b.document_id, b.file_id, b.file_unique_id, b.message_date
             FROM multipart_part_blocks pb JOIN telegram_blocks b ON b.id = pb.block_id
             WHERE pb.upload_id = ?1 ORDER BY pb.part_number, pb.ordinal",
        )
        .bind(upload_id)
        .fetch_all(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?1")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        let mut released = Vec::with_capacity(blocks.len());
        for row in blocks {
            let block = block_from_row(row)?;
            decrement_block(&mut tx, block.id).await?;
            released.push(block);
        }
        tx.commit().await?;
        Ok(Some(released))
    }
}
