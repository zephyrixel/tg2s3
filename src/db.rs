use crate::model::{
    BlockRef, BucketRecord, CorsConfiguration, GarbageRecord, ListingRecord, ObjectCondition,
    ObjectMetadata, ObjectRecord, PartRecord, TelegramBackend, UploadRecord,
};
use anyhow::{Context, Result, anyhow, bail};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct Db {
    pool: SqlitePool,
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

impl Db {
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .with_context(|| format!("connect SQLite {}", path.display()))?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("run SQLite migrations")?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn integrity_check(&self) -> Result<String> {
        Ok(sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(&self.pool)
            .await?)
    }

    pub async fn create_bucket(&self, name: &str) -> Result<bool> {
        let result = sqlx::query("INSERT OR IGNORE INTO buckets(name, created_at) VALUES(?1, ?2)")
            .bind(name)
            .bind(now())
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn bucket_exists(&self, name: &str) -> Result<bool> {
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT EXISTS(SELECT 1 FROM buckets WHERE name = ?1)")
                .bind(name)
                .fetch_one(&self.pool)
                .await?;
        Ok(exists != 0)
    }

    pub async fn has_backend(&self, backend: TelegramBackend) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM telegram_blocks WHERE backend = ?1)",
        )
        .bind(backend.as_str())
        .fetch_one(&self.pool)
        .await?;
        Ok(exists != 0)
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketRecord>> {
        let rows = sqlx::query("SELECT name, created_at FROM buckets ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| BucketRecord {
                name: row.get("name"),
                created_at: row.get("created_at"),
            })
            .collect())
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<Option<bool>> {
        let mut tx = self.pool.begin().await?;
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT EXISTS(SELECT 1 FROM buckets WHERE name = ?1)")
                .bind(name)
                .fetch_one(&mut *tx)
                .await?;
        if exists == 0 {
            tx.rollback().await?;
            return Ok(None);
        }
        let occupied = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE bucket = ?1)
             OR EXISTS(SELECT 1 FROM multipart_uploads WHERE bucket = ?1)",
        )
        .bind(name)
        .fetch_one(&mut *tx)
        .await?;
        if occupied != 0 {
            tx.rollback().await?;
            return Ok(Some(false));
        }
        sqlx::query("DELETE FROM bucket_cors WHERE bucket = ?1")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM buckets WHERE name = ?1")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(true))
    }

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
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let upload_id: String = row.get("upload_id");
            let part_number: i32 = row.get("part_number");
            result.push(PartRecord {
                blocks: load_part_blocks(&self.pool, &upload_id, part_number).await?,
                upload_id,
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
    ) -> Result<(ObjectRecord, Vec<BlockRef>)> {
        let mut tx = self.pool.begin().await?;
        let source_exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                 SELECT 1 FROM objects
                 WHERE id = ?1 AND bucket = ?2 AND object_key = ?3
             )",
        )
        .bind(source.id)
        .bind(&source.bucket)
        .bind(&source.key)
        .fetch_one(&mut *tx)
        .await?;
        if source_exists == 0 {
            bail!("NoSuchKey");
        }
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

    pub async fn gc_candidates(&self, timestamp: i64, limit: usize) -> Result<Vec<GarbageRecord>> {
        let limit = i64::try_from(limit).context("GC limit exceeds SQLite integer range")?;
        let rows = sqlx::query(
            "SELECT q.block_id, b.chat_id, b.message_id, b.backend, b.document_id,
                    b.file_id, b.file_unique_id, b.message_date,
                    q.attempts, q.next_attempt, q.last_error
             FROM gc_queue q JOIN telegram_blocks b ON b.id = q.block_id
             WHERE q.state = 'pending' AND b.ref_count = 0 AND q.next_attempt <= ?1
             ORDER BY q.next_attempt LIMIT ?2",
        )
        .bind(timestamp)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let backend = parse_backend(row.get("backend"))?;
                Ok(GarbageRecord {
                    block_id: row.get("block_id"),
                    chat_id: row.get("chat_id"),
                    message_id: row.get("message_id"),
                    backend,
                    document_id: row.get("document_id"),
                    file_id: row.get("file_id"),
                    file_unique_id: row.get("file_unique_id"),
                    message_date: row.get("message_date"),
                    attempts: row.get("attempts"),
                    next_attempt: row.get("next_attempt"),
                    last_error: row.get("last_error"),
                })
            })
            .collect()
    }

    pub async fn gc_success(&self, block_id: i64) -> Result<()> {
        sqlx::query("DELETE FROM telegram_blocks WHERE id = ?1 AND ref_count = 0")
            .bind(block_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn gc_failure(&self, block_id: i64, error: &str, next_attempt: i64) -> Result<()> {
        sqlx::query(
            "UPDATE gc_queue SET attempts = attempts + 1, last_error = ?2, next_attempt = ?3
             WHERE block_id = ?1",
        )
        .bind(block_id)
        .bind(error)
        .bind(next_attempt)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn gc_orphan(&self, block_id: i64, error: &str) -> Result<()> {
        sqlx::query("UPDATE gc_queue SET state = 'orphan', last_error = ?2 WHERE block_id = ?1")
            .bind(block_id)
            .bind(error)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn expire_uploads(&self, before: i64) -> Result<usize> {
        let ids = sqlx::query("SELECT upload_id FROM multipart_uploads WHERE created_at < ?1")
            .bind(before)
            .fetch_all(&self.pool)
            .await?;
        let mut count = 0;
        for row in ids {
            if self.abort_upload(row.get("upload_id")).await? {
                count += 1;
            }
        }
        Ok(count)
    }

    pub async fn stale_blocks(&self, before: i64) -> Result<Vec<GarbageRecord>> {
        let rows = sqlx::query(
            "SELECT b.id, b.chat_id, b.message_id, b.backend, b.document_id,
                    b.file_id, b.file_unique_id, b.message_date
             FROM telegram_blocks b
             LEFT JOIN object_blocks ob ON ob.block_id = b.id
             LEFT JOIN multipart_part_blocks pb ON pb.block_id = b.id
             WHERE b.state = 'staged' AND b.created_at < ?1
               AND ob.block_id IS NULL AND pb.block_id IS NULL",
        )
        .bind(before)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let backend = parse_backend(row.get("backend"))?;
                Ok(GarbageRecord {
                    block_id: row.get("id"),
                    chat_id: row.get("chat_id"),
                    message_id: row.get("message_id"),
                    backend,
                    document_id: row.get("document_id"),
                    file_id: row.get("file_id"),
                    file_unique_id: row.get("file_unique_id"),
                    message_date: row.get("message_date"),
                    attempts: 0,
                    next_attempt: 0,
                    last_error: None,
                })
            })
            .collect()
    }

    pub async fn delete_stale_block(&self, block_id: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE telegram_blocks SET ref_count = 0
             WHERE id = ?1 AND state = 'staged'",
        )
        .bind(block_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("INSERT OR IGNORE INTO gc_queue(block_id, next_attempt) VALUES(?1, ?2)")
            .bind(block_id)
            .bind(now())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_bucket_cors(&self, bucket: &str) -> Result<Option<CorsConfiguration>> {
        let row = sqlx::query("SELECT configuration_json FROM bucket_cors WHERE bucket = ?1")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            serde_json::from_str(&row.get::<String, _>("configuration_json"))
                .context("decode stored bucket CORS configuration")
        })
        .transpose()
    }

    pub async fn set_bucket_cors(
        &self,
        bucket: &str,
        configuration: &CorsConfiguration,
    ) -> Result<()> {
        let timestamp = now();
        sqlx::query(
            "INSERT INTO bucket_cors(bucket, configuration_json, created_at, updated_at)
             VALUES(?1, ?2, ?3, ?3)
             ON CONFLICT(bucket) DO UPDATE SET
               configuration_json = excluded.configuration_json,
               updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(serde_json::to_string(configuration)?)
        .bind(timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_bucket_cors(&self, bucket: &str) -> Result<()> {
        sqlx::query("DELETE FROM bucket_cors WHERE bucket = ?1")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

fn parse_metadata(value: &str) -> Result<ObjectMetadata> {
    serde_json::from_str(value).context("decode stored metadata")
}

fn escape_glob(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '*' => escaped.push_str("[*]"),
            '?' => escaped.push_str("[?]"),
            '[' => escaped.push_str("[[]"),
            ']' => escaped.push_str("[]]"),
            character => escaped.push(character),
        }
    }
    escaped
}

fn upload_from_row(row: sqlx::sqlite::SqliteRow) -> Result<UploadRecord> {
    Ok(UploadRecord {
        upload_id: row.get("upload_id"),
        bucket: row.get("bucket"),
        key: row.get("object_key"),
        metadata: parse_metadata(&row.get::<String, _>("metadata_json"))?,
        kind: row.get("kind"),
        created_at: row.get("created_at"),
    })
}

fn validate_parts_layout(parts: &[PartRecord]) -> Result<i64> {
    let mut total = 0_i64;
    let mut block_ids = HashSet::new();
    for part in parts {
        let mut offset = 0_i64;
        for block in &part.blocks {
            if block.size <= 0 || block.offset != offset || !block_ids.insert(block.id) {
                bail!("object storage layout is invalid");
            }
            offset = offset
                .checked_add(block.size)
                .ok_or_else(|| anyhow!("object storage layout is invalid"))?;
        }
        if offset != part.size {
            bail!("object storage layout is invalid");
        }
        total = total
            .checked_add(offset)
            .ok_or_else(|| anyhow!("object storage layout is invalid"))?;
    }
    Ok(total)
}

async fn load_object_blocks(pool: &SqlitePool, object_id: i64) -> Result<Vec<BlockRef>> {
    let rows = sqlx::query(
        "SELECT b.id, ob.ordinal, ob.byte_offset, ob.size, b.chat_id, b.message_id,
                b.backend, b.document_id, b.file_id, b.file_unique_id, b.message_date
         FROM object_blocks ob JOIN telegram_blocks b ON b.id = ob.block_id
         WHERE ob.object_id = ?1 ORDER BY ob.ordinal",
    )
    .bind(object_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(block_from_row).collect()
}

async fn load_object_blocks_tx(
    tx: &mut Transaction<'_, Sqlite>,
    object_id: i64,
) -> Result<Vec<BlockRef>> {
    let rows = sqlx::query(
        "SELECT b.id, ob.ordinal, ob.byte_offset, ob.size, b.chat_id, b.message_id,
                b.backend, b.document_id, b.file_id, b.file_unique_id, b.message_date
         FROM object_blocks ob JOIN telegram_blocks b ON b.id = ob.block_id
         WHERE ob.object_id = ?1 ORDER BY ob.ordinal",
    )
    .bind(object_id)
    .fetch_all(&mut **tx)
    .await?;
    rows.into_iter().map(block_from_row).collect()
}

async fn load_part_blocks(
    pool: &SqlitePool,
    upload_id: &str,
    part_number: i32,
) -> Result<Vec<BlockRef>> {
    let rows = sqlx::query(
        "SELECT b.id, pb.ordinal, pb.byte_offset, pb.size, b.chat_id, b.message_id,
                b.backend, b.document_id, b.file_id, b.file_unique_id, b.message_date
         FROM multipart_part_blocks pb JOIN telegram_blocks b ON b.id = pb.block_id
         WHERE pb.upload_id = ?1 AND pb.part_number = ?2 ORDER BY pb.ordinal",
    )
    .bind(upload_id)
    .bind(part_number)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(block_from_row).collect()
}

fn block_from_row(row: sqlx::sqlite::SqliteRow) -> Result<BlockRef> {
    let backend = parse_backend(row.get(6))?;
    Ok(BlockRef {
        id: row.get(0),
        ordinal: row.get(1),
        offset: row.get(2),
        size: row.get(3),
        chat_id: row.get(4),
        message_id: row.get(5),
        backend,
        document_id: row.get(7),
        file_id: row.get(8),
        file_unique_id: row.get(9),
        message_date: row.get(10),
    })
}

fn parse_backend(value: String) -> Result<TelegramBackend> {
    value
        .parse()
        .map_err(|error| anyhow!("invalid Telegram block backend: {error}"))
}

async fn decrement_block(tx: &mut Transaction<'_, Sqlite>, block_id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE telegram_blocks SET ref_count = CASE
         WHEN ref_count > 0 THEN ref_count - 1 ELSE 0 END WHERE id = ?1",
    )
    .bind(block_id)
    .execute(&mut **tx)
    .await?;
    let zero = sqlx::query(
        "SELECT chat_id, message_id, message_date FROM telegram_blocks
         WHERE id = ?1 AND ref_count = 0",
    )
    .bind(block_id)
    .fetch_optional(&mut **tx)
    .await?;
    if zero.is_some() {
        sqlx::query(
            "INSERT OR IGNORE INTO gc_queue(block_id, next_attempt, state)
             VALUES(?1, ?2, 'pending')",
        )
        .bind(block_id)
        .bind(now())
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TelegramBackend;
    use tempfile::tempdir;

    fn block(message_id: i64, size: i64) -> BlockRef {
        BlockRef {
            id: 0,
            ordinal: 0,
            offset: 0,
            size,
            chat_id: -100,
            message_id,
            backend: TelegramBackend::BotApi,
            document_id: None,
            file_id: format!("file-{message_id}"),
            file_unique_id: format!("unique-{message_id}"),
            message_date: now(),
        }
    }

    #[test]
    fn rejects_inconsistent_part_layouts() {
        let part = PartRecord {
            upload_id: "upload".to_string(),
            part_number: 1,
            size: 4,
            etag: "etag".to_string(),
            blocks: vec![block(1, 3)],
        };
        assert!(validate_parts_layout(&[part]).is_err());
    }

    #[tokio::test]
    async fn object_commit_copy_delete_and_gc_preserve_references() -> Result<()> {
        let dir = tempdir()?;
        let db = Db::open(&dir.path().join("test.sqlite3")).await?;
        assert!(db.create_bucket("bucket").await?);

        let upload = UploadRecord {
            upload_id: "upload-1".to_string(),
            bucket: "bucket".to_string(),
            key: "one".to_string(),
            metadata: ObjectMetadata::default(),
            kind: "put".to_string(),
            created_at: now(),
        };
        db.create_upload(&upload).await?;
        let staged = BlockRef {
            backend: TelegramBackend::Grammers,
            document_id: Some(42),
            ..block(1, 3)
        };
        let id = db.add_staged_block(&staged).await?;
        let staged = BlockRef { id, ..staged };
        let part = PartRecord {
            upload_id: upload.upload_id.clone(),
            part_number: 0,
            size: 3,
            etag: "900150983cd24fb0d6963f7d28e17f72".to_string(),
            blocks: vec![staged.clone()],
        };
        db.replace_part(&upload.upload_id, 0, 3, &part.etag, &part.blocks)
            .await?;
        let etag = part.etag.clone();
        db.commit_upload(
            &upload.upload_id,
            3,
            &etag,
            &[part],
            &ObjectCondition::default(),
        )
        .await?
        .expect("commit");

        let loaded = db
            .get_object("bucket", "one")
            .await?
            .expect("loaded object");
        assert_eq!(loaded.blocks[0].backend, TelegramBackend::Grammers);
        assert_eq!(loaded.blocks[0].document_id, Some(42));

        let source = db
            .get_object("bucket", "one")
            .await?
            .expect("source object");
        assert_eq!(source.blocks.len(), 1);

        let conditional_upload = UploadRecord {
            upload_id: "conditional-upload".to_string(),
            bucket: "bucket".to_string(),
            key: "one".to_string(),
            metadata: ObjectMetadata::default(),
            kind: "put".to_string(),
            created_at: now(),
        };
        db.create_upload(&conditional_upload).await?;
        let replacement = block(2, 3);
        let replacement_id = db.add_staged_block(&replacement).await?;
        let replacement = BlockRef {
            id: replacement_id,
            ..replacement
        };
        let replacement_part = PartRecord {
            upload_id: conditional_upload.upload_id.clone(),
            part_number: 0,
            size: 3,
            etag: "f561aaf6ef0bf14d4208bb46a22e7d9f".to_string(),
            blocks: vec![replacement],
        };
        db.replace_part(
            &conditional_upload.upload_id,
            0,
            3,
            &replacement_part.etag,
            &replacement_part.blocks,
        )
        .await?;
        let stale_condition = ObjectCondition {
            if_match: Some("\"stale-etag\"".to_string()),
            if_none_match: None,
        };
        assert!(
            db.commit_upload(
                &conditional_upload.upload_id,
                3,
                "replacement-etag",
                &[replacement_part],
                &stale_condition,
            )
            .await
            .is_err()
        );
        assert!(db.abort_upload(&conditional_upload.upload_id).await?);
        let aborted = db.gc_candidates(now(), 10).await?;
        assert_eq!(aborted.len(), 1);
        db.gc_success(aborted[0].block_id).await?;
        assert_eq!(
            db.get_object("bucket", "one").await?.unwrap().etag,
            source.etag
        );

        assert!(
            db.copy_object(
                &source,
                "bucket",
                "two",
                &ObjectMetadata::default(),
                &stale_condition,
            )
            .await
            .is_err()
        );
        assert!(db.get_object("bucket", "two").await?.is_none());
        assert!(
            db.delete_object("bucket", "one", &stale_condition)
                .await
                .is_err()
        );
        assert!(db.get_object("bucket", "one").await?.is_some());

        db.copy_object(
            &source,
            "bucket",
            "two",
            &ObjectMetadata::default(),
            &ObjectCondition::default(),
        )
        .await?;
        db.delete_object("bucket", "one", &ObjectCondition::default())
            .await?
            .expect("delete source");
        assert!(db.get_object("bucket", "two").await?.is_some());
        assert!(
            db.copy_object(
                &source,
                "bucket",
                "stale-copy",
                &ObjectMetadata::default(),
                &ObjectCondition::default(),
            )
            .await
            .is_err()
        );
        assert!(db.gc_candidates(now(), 10).await?.is_empty());

        db.delete_object("bucket", "two", &ObjectCondition::default())
            .await?
            .expect("delete copy");
        let garbage = db.gc_candidates(now(), 10).await?;
        assert_eq!(garbage.len(), 1);
        db.gc_success(garbage[0].block_id).await?;
        assert!(db.gc_candidates(now(), 10).await?.is_empty());
        assert_eq!(db.integrity_check().await?, "ok");
        Ok(())
    }

    #[tokio::test]
    async fn baseline_adopts_existing_database_without_losing_metadata() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("legacy.sqlite3");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let legacy = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE buckets(name TEXT PRIMARY KEY, created_at INTEGER NOT NULL)")
            .execute(&legacy)
            .await?;
        sqlx::query(
            "CREATE TABLE objects(
                id INTEGER PRIMARY KEY,
                bucket TEXT NOT NULL,
                object_key TEXT NOT NULL,
                size INTEGER NOT NULL,
                etag TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                modified_at INTEGER NOT NULL,
                UNIQUE(bucket, object_key)
            )",
        )
        .execute(&legacy)
        .await?;
        sqlx::query("INSERT INTO buckets(name, created_at) VALUES('legacy', 1)")
            .execute(&legacy)
            .await?;
        sqlx::query(
            "INSERT INTO objects
             (bucket, object_key, size, etag, metadata_json, created_at, modified_at)
             VALUES('legacy', 'file.txt', 3, 'etag', '{}', 1, 1)",
        )
        .execute(&legacy)
        .await?;
        legacy.close().await;

        let db = Db::open(&path).await?;
        assert!(db.bucket_exists("legacy").await?);
        let object = db.get_object("legacy", "file.txt").await?.expect("object");
        assert_eq!(object.size, 3);
        assert_eq!(object.etag, "etag");
        assert!(db.get_bucket_cors("legacy").await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn bucket_delete_rejects_active_uploads() -> Result<()> {
        let dir = tempdir()?;
        let db = Db::open(&dir.path().join("test.sqlite3")).await?;
        db.create_bucket("bucket").await?;
        db.create_upload(&UploadRecord {
            upload_id: "multipart-1".to_string(),
            bucket: "bucket".to_string(),
            key: "pending".to_string(),
            metadata: ObjectMetadata::default(),
            kind: "multipart".to_string(),
            created_at: now(),
        })
        .await?;

        assert_eq!(db.delete_bucket("bucket").await?, Some(false));
        assert!(db.abort_upload("multipart-1").await?);
        assert_eq!(db.delete_bucket("bucket").await?, Some(true));
        Ok(())
    }

    #[tokio::test]
    async fn upload_creation_requires_an_existing_bucket() -> Result<()> {
        let dir = tempdir()?;
        let db = Db::open(&dir.path().join("test.sqlite3")).await?;
        let upload = UploadRecord {
            upload_id: "missing-bucket-upload".to_string(),
            bucket: "missing".to_string(),
            key: "object".to_string(),
            metadata: ObjectMetadata::default(),
            kind: "put".to_string(),
            created_at: now(),
        };
        assert!(db.create_upload(&upload).await.is_err());
        assert!(db.get_upload(&upload.upload_id).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn committing_a_stale_block_restores_its_reference() -> Result<()> {
        let dir = tempdir()?;
        let db = Db::open(&dir.path().join("test.sqlite3")).await?;
        db.create_bucket("bucket").await?;
        let upload = UploadRecord {
            upload_id: "upload-stale".to_string(),
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            metadata: ObjectMetadata::default(),
            kind: "put".to_string(),
            created_at: now(),
        };
        db.create_upload(&upload).await?;
        let staged = block(1, 3);
        let id = db.add_staged_block(&staged).await?;
        let staged = BlockRef { id, ..staged };
        db.delete_stale_block(id).await?;
        let part = PartRecord {
            upload_id: upload.upload_id.clone(),
            part_number: 0,
            size: 3,
            etag: "900150983cd24fb0d6963f7d28e17f72".to_string(),
            blocks: vec![staged],
        };
        db.replace_part(&upload.upload_id, 0, 3, &part.etag, &part.blocks)
            .await?;
        let etag = part.etag.clone();
        db.commit_upload(
            &upload.upload_id,
            3,
            &etag,
            &[part],
            &ObjectCondition::default(),
        )
        .await?
        .ok_or_else(|| anyhow!("upload disappeared"))?;

        let refs: i64 = sqlx::query_scalar("SELECT ref_count FROM telegram_blocks WHERE id = ?1")
            .bind(id)
            .fetch_one(db.pool())
            .await?;
        assert_eq!(refs, 1);
        assert!(db.gc_candidates(now(), 10).await?.is_empty());
        Ok(())
    }
}
