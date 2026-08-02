use crate::config::Config;
use anyhow::{Result, bail};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::sleep;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferDirection {
    Upload,
    Download,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitError {
    SlowDown,
    EntityTooLarge,
}

impl fmt::Display for LimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SlowDown => "SlowDown",
            Self::EntityTooLarge => "EntityTooLarge",
        })
    }
}

impl std::error::Error for LimitError {}

#[derive(Clone)]
pub struct AdmissionController {
    global: Arc<Semaphore>,
    wait: Duration,
}

pub struct AdmissionPermit {
    _global: OwnedSemaphorePermit,
}

impl AdmissionController {
    pub fn new(config: &Config) -> Self {
        Self {
            global: Arc::new(Semaphore::new(config.max_active_transfers)),
            wait: Duration::from_secs(config.limit_wait_secs),
        }
    }

    pub async fn acquire(&self) -> Result<AdmissionPermit> {
        if self.wait.is_zero() {
            let global = self
                .global
                .clone()
                .try_acquire_owned()
                .map_err(|_| anyhow::anyhow!(LimitError::SlowDown))?;
            return Ok(AdmissionPermit { _global: global });
        }
        let global = tokio::time::timeout(self.wait, self.global.clone().acquire_owned())
            .await
            .map_err(|_| anyhow::anyhow!(LimitError::SlowDown))?
            .map_err(|_| anyhow::anyhow!("transfer admission controller closed"))?;
        Ok(AdmissionPermit { _global: global })
    }
}

#[derive(Clone)]
pub struct BandwidthLimiter {
    global_upload: Arc<Mutex<RateSchedule>>,
    global_download: Arc<Mutex<RateSchedule>>,
    upload_rate: u64,
    download_rate: u64,
    wait: Duration,
}

#[derive(Default)]
struct RateSchedule {
    next: Option<Instant>,
}

impl BandwidthLimiter {
    pub fn new(config: &Config) -> Self {
        Self {
            global_upload: Arc::new(Mutex::new(RateSchedule::default())),
            global_download: Arc::new(Mutex::new(RateSchedule::default())),
            upload_rate: config.upload_rate_bps,
            download_rate: config.download_rate_bps,
            wait: Duration::from_secs(config.limit_wait_secs),
        }
    }

    pub async fn throttle(
        &self,
        direction: TransferDirection,
        bytes: usize,
        bounded: bool,
    ) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let (rate, schedule) = match direction {
            TransferDirection::Upload => (self.upload_rate, self.global_upload.clone()),
            TransferDirection::Download => (self.download_rate, self.global_download.clone()),
        };
        let wait = reserve(&schedule, rate, bytes);
        if bounded && wait > self.wait {
            bail!(LimitError::SlowDown);
        }
        if !wait.is_zero() {
            sleep(wait).await;
        }
        Ok(())
    }
}

fn reserve(schedule: &Arc<Mutex<RateSchedule>>, rate: u64, bytes: usize) -> Duration {
    if rate == 0 {
        return Duration::ZERO;
    }
    let duration = Duration::from_secs_f64(bytes as f64 / rate as f64);
    let now = Instant::now();
    let Ok(mut state) = schedule.lock() else {
        return duration;
    };
    let start = state.next.unwrap_or(now).max(now);
    state.next = Some(start + duration);
    start.saturating_duration_since(now)
}

pub fn check_size(length: Option<i64>, max_size: i64) -> Result<()> {
    if let Some(length) = length {
        if length < 0 || length > max_size {
            bail!(LimitError::EntityTooLarge);
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct TransferLimits {
    pub admission: AdmissionController,
    pub bandwidth: BandwidthLimiter,
    pub max_object_size: i64,
}

impl TransferLimits {
    pub fn new(config: &Config) -> Self {
        Self {
            admission: AdmissionController::new(config),
            bandwidth: BandwidthLimiter::new(config),
            max_object_size: config.max_object_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_configured_object_size() {
        assert!(check_size(Some(10), 10).is_ok());
        assert!(
            check_size(Some(11), 10)
                .unwrap_err()
                .to_string()
                .contains("EntityTooLarge")
        );
        assert!(check_size(Some(-1), 10).is_err());
        assert!(check_size(None, 10).is_ok());
    }
}
