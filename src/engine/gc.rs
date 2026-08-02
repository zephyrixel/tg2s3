use super::{Engine, now};
use crate::config::MAX_GC_LIMIT;
use crate::telegram::is_missing_message;
use anyhow::{Result, bail};

impl Engine {
    pub async fn run_gc(&self, limit: usize) -> Result<usize> {
        if limit == 0 || limit > MAX_GC_LIMIT {
            bail!("GC limit must be between 1 and {MAX_GC_LIMIT}");
        }
        let timestamp = now();
        let _ = self.db.expire_uploads(timestamp - 7 * 24 * 3600).await?;
        let mut processed = 0;
        for stale in self.db.stale_blocks(timestamp - 3600, limit).await? {
            self.db.queue_stale_block(stale.block_id).await?;
            processed += 1;
        }
        let remaining = limit.saturating_sub(processed);
        if remaining == 0 {
            return Ok(processed);
        }
        let candidates = self.db.gc_candidates(timestamp, remaining).await?;
        for candidate in candidates {
            if candidate.message_date > 0 && candidate.message_date < timestamp - 48 * 3600 {
                self.db
                    .gc_orphan(candidate.block_id, "Telegram deleteMessage 48 hour limit")
                    .await?;
                processed += 1;
                continue;
            }
            let block = candidate.as_block_ref();
            match self.telegram.delete_message(&block).await {
                Ok(()) => self.db.gc_success(candidate.block_id).await?,
                Err(error) if is_missing_message(&error) => {
                    self.db.gc_success(candidate.block_id).await?;
                }
                Err(error) => {
                    self.db
                        .gc_failure(candidate.block_id, &error.to_string(), timestamp + 300)
                        .await?
                }
            }
            processed += 1;
        }
        Ok(processed)
    }
}
