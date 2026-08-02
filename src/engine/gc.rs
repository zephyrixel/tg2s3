use super::{Engine, now};
use crate::config::MAX_GC_LIMIT;
use crate::model::{BlockRef, GarbageRecord, TelegramBackend};
use crate::telegram::is_missing_message;
use anyhow::{Result, bail};

impl Engine {
    pub async fn run_gc(&self, limit: usize) -> Result<usize> {
        if limit == 0 || limit > MAX_GC_LIMIT {
            bail!("GC limit must be between 1 and {MAX_GC_LIMIT}");
        }
        let _gc_guard = self.gc_lock.lock().await;
        let timestamp = now();
        let _ = self.db.expire_uploads(timestamp - 7 * 24 * 3600).await?;
        let mut processed = 0;
        for stale in self.db.stale_blocks(timestamp - 3600, limit).await? {
            if self.db.queue_stale_block(stale.block_id).await? {
                processed += 1;
            }
        }
        let remaining = limit.saturating_sub(processed);
        if remaining == 0 {
            return Ok(processed);
        }
        let candidates = self.db.gc_candidates(timestamp, remaining).await?;
        processed += self.process_gc_candidates(candidates, timestamp).await?;
        Ok(processed)
    }

    /// Schedules released blocks for immediate best-effort Telegram cleanup in
    /// the background. The gc_queue rows written by the releasing transaction
    /// guarantee a retry through `run_gc` if this task never completes, so the
    /// caller does not have to wait for Telegram.
    pub(super) fn reclaim_blocks(&self, blocks: Vec<BlockRef>) {
        if blocks.is_empty() {
            return;
        }
        let engine = self.clone();
        tokio::spawn(async move {
            engine.reclaim_blocks_now(blocks).await;
        });
    }

    async fn reclaim_blocks_now(&self, blocks: Vec<BlockRef>) {
        let _gc_guard = self.gc_lock.lock().await;
        let timestamp = now();
        let mut candidates = Vec::with_capacity(blocks.len());
        for block in blocks {
            match self.db.gc_candidate(block.id, timestamp).await {
                Ok(Some(candidate)) => candidates.push(candidate),
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        block_id = block.id,
                        error = %error,
                        "failed to load released Telegram block for immediate cleanup"
                    );
                }
            }
        }
        if let Err(error) = self.process_gc_candidates(candidates, timestamp).await {
            tracing::warn!(
                error = %error,
                "failed to immediately clean up released Telegram blocks"
            );
        }
    }

    /// Marks expired Bot API candidates as orphans, then deletes the rest in
    /// per-backend batches, falling back to per-message deletion when a batch
    /// call fails (for example against an older local Bot API server).
    async fn process_gc_candidates(
        &self,
        candidates: Vec<GarbageRecord>,
        timestamp: i64,
    ) -> Result<usize> {
        let mut processed = 0;
        let mut groups: Vec<(TelegramBackend, Vec<GarbageRecord>)> = Vec::new();
        for candidate in candidates {
            if bot_api_delete_window_expired(candidate.backend, candidate.message_date, timestamp) {
                self.db
                    .gc_orphan(candidate.block_id, "Telegram deleteMessage 48 hour limit")
                    .await?;
                processed += 1;
                continue;
            }
            match groups
                .iter_mut()
                .find(|(backend, _)| *backend == candidate.backend)
            {
                Some((_, group)) => group.push(candidate),
                None => groups.push((candidate.backend, vec![candidate])),
            }
        }
        for (backend, group) in groups {
            processed += self
                .delete_candidate_group(backend, group, timestamp)
                .await?;
        }
        Ok(processed)
    }

    async fn delete_candidate_group(
        &self,
        backend: TelegramBackend,
        group: Vec<GarbageRecord>,
        timestamp: i64,
    ) -> Result<usize> {
        if group.len() > 1 {
            let message_ids: Vec<i64> = group.iter().map(|record| record.message_id).collect();
            match self.telegram.delete_messages(backend, &message_ids).await {
                Ok(()) => {
                    let mut processed = 0;
                    for candidate in group {
                        self.db.gc_success(candidate.block_id).await?;
                        processed += 1;
                    }
                    return Ok(processed);
                }
                Err(error) => {
                    tracing::warn!(
                        count = message_ids.len(),
                        error = %error,
                        "batch Telegram message deletion failed; retrying per message"
                    );
                }
            }
        }
        let mut processed = 0;
        for candidate in group {
            self.process_gc_candidate(candidate, timestamp).await?;
            processed += 1;
        }
        Ok(processed)
    }

    async fn process_gc_candidate(&self, candidate: GarbageRecord, timestamp: i64) -> Result<()> {
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
        Ok(())
    }
}

fn bot_api_delete_window_expired(
    backend: TelegramBackend,
    message_date: i64,
    timestamp: i64,
) -> bool {
    backend == TelegramBackend::BotApi && message_date > 0 && message_date < timestamp - 48 * 3600
}

#[cfg(test)]
mod tests {
    use super::bot_api_delete_window_expired;
    use crate::model::TelegramBackend;

    #[test]
    fn only_bot_api_messages_are_limited_by_the_48_hour_window() {
        let now = 200_000;
        assert!(bot_api_delete_window_expired(
            TelegramBackend::BotApi,
            now - 48 * 3600 - 1,
            now
        ));
        assert!(!bot_api_delete_window_expired(
            TelegramBackend::BotApi,
            now - 48 * 3600,
            now
        ));
        assert!(!bot_api_delete_window_expired(
            TelegramBackend::Grammers,
            now - 90 * 24 * 3600,
            now
        ));
    }
}
