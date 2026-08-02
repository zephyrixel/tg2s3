use crate::config::Config;
use crate::db::Db;
use crate::limits::TransferLimits;
use crate::telegram::TelegramClient;
use anyhow::Result;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;

mod download;
mod gc;
mod multipart;
mod object;
#[cfg(test)]
mod tests;
mod transfer;

pub const MIN_MULTIPART_PART: i64 = 5 * 1024 * 1024;
pub const MAX_MULTIPART_PARTS: usize = 10_000;

#[derive(Clone)]
pub struct Engine {
    pub db: Db,
    pub telegram: TelegramClient,
    pub config: Arc<Config>,
    pub limits: Arc<TransferLimits>,
    upload_slots: Arc<Semaphore>,
    download_slots: Arc<Semaphore>,
}

impl Engine {
    pub fn new(config: Config, db: Db, telegram: TelegramClient) -> Self {
        let limits = Arc::new(TransferLimits::new(&config));
        let slots = Arc::new(Semaphore::new(config.upload_concurrency));
        let download_slots = Arc::new(Semaphore::new(config.download_concurrency));
        Self {
            db,
            telegram,
            config: Arc::new(config),
            limits,
            upload_slots: slots,
            download_slots,
        }
    }

    async fn require_bucket(&self, bucket: &str) -> Result<()> {
        if !self.db.bucket_exists(bucket).await? {
            anyhow::bail!("NoSuchBucket");
        }
        Ok(())
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
