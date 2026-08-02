use crate::model::GarbageRecord;
use anyhow::{Context, Result};
use sqlx::Row;

use super::Db;
use super::support::{now, parse_backend};

impl Db {
    pub async fn gc_candidates(&self, timestamp: i64, limit: usize) -> Result<Vec<GarbageRecord>> {
        let limit = i64::try_from(limit).context("GC limit exceeds SQLite integer range")?;
        let rows = sqlx::query(
            "SELECT q.block_id, b.chat_id, b.message_id, b.backend, b.document_id,
                    b.file_id, b.file_unique_id, b.message_date,
                    q.attempts, q.next_attempt, q.last_error
             FROM gc_queue q JOIN telegram_blocks b ON b.id = q.block_id
             WHERE q.state = 'pending' AND b.ref_count = 0 AND q.next_attempt <= ?1
               AND NOT EXISTS(SELECT 1 FROM object_blocks WHERE block_id = b.id)
               AND NOT EXISTS(SELECT 1 FROM multipart_part_blocks WHERE block_id = b.id)
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
        sqlx::query(
            "DELETE FROM telegram_blocks
             WHERE id = ?1 AND ref_count = 0
               AND NOT EXISTS(SELECT 1 FROM object_blocks WHERE block_id = telegram_blocks.id)
               AND NOT EXISTS(SELECT 1 FROM multipart_part_blocks WHERE block_id = telegram_blocks.id)",
        )
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

    pub async fn stale_blocks(&self, before: i64, limit: usize) -> Result<Vec<GarbageRecord>> {
        let limit =
            i64::try_from(limit).context("stale block limit exceeds SQLite integer range")?;
        let rows = sqlx::query(
            "SELECT b.id, b.chat_id, b.message_id, b.backend, b.document_id,
                    b.file_id, b.file_unique_id, b.message_date
             FROM telegram_blocks b
             LEFT JOIN object_blocks ob ON ob.block_id = b.id
             LEFT JOIN multipart_part_blocks pb ON pb.block_id = b.id
             WHERE b.state = 'staged' AND b.created_at < ?1
               AND ob.block_id IS NULL AND pb.block_id IS NULL
             ORDER BY b.created_at, b.id LIMIT ?2",
        )
        .bind(before)
        .bind(limit)
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

    /// Claims an unreferenced staged block for Telegram message cleanup.
    pub async fn queue_stale_block(&self, block_id: i64) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE telegram_blocks SET ref_count = 0
             WHERE id = ?1 AND state = 'staged' AND ref_count > 0
               AND NOT EXISTS(SELECT 1 FROM object_blocks WHERE block_id = telegram_blocks.id)
               AND NOT EXISTS(SELECT 1 FROM multipart_part_blocks WHERE block_id = telegram_blocks.id)",
        )
        .bind(block_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO gc_queue(block_id, next_attempt)
             SELECT ?1, ?2 FROM telegram_blocks b
             WHERE b.id = ?1 AND b.state = 'staged' AND b.ref_count = 0
               AND NOT EXISTS(SELECT 1 FROM object_blocks WHERE block_id = b.id)
               AND NOT EXISTS(SELECT 1 FROM multipart_part_blocks WHERE block_id = b.id)",
        )
        .bind(block_id)
        .bind(now())
        .execute(&mut *tx)
        .await?;
        let queued = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                 SELECT 1 FROM telegram_blocks b
                 JOIN gc_queue q ON q.block_id = b.id
                 WHERE b.id = ?1 AND b.state = 'staged' AND b.ref_count = 0
                   AND NOT EXISTS(SELECT 1 FROM object_blocks WHERE block_id = b.id)
                   AND NOT EXISTS(SELECT 1 FROM multipart_part_blocks WHERE block_id = b.id)
             )",
        )
        .bind(block_id)
        .fetch_one(&mut *tx)
        .await?
            != 0;
        tx.commit().await?;
        Ok(queued)
    }
}
