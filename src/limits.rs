use crate::config::Config;
use anyhow::{Result, bail};
use axum::http::HeaderMap;
use ipnet::IpNet;
use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::sleep;

const MAX_IP_STATES: usize = 8_192;
const IP_STATE_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferDirection {
    Upload,
    Download,
}

impl TransferDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Download => "download",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TransferContext {
    pub request_id: String,
    pub client_ip: IpAddr,
    pub direction: TransferDirection,
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
    upload_per_ip: Arc<PerIpSemaphores>,
    download_per_ip: Arc<PerIpSemaphores>,
    wait: Duration,
}

pub struct AdmissionPermit {
    _global: OwnedSemaphorePermit,
    _per_ip: OwnedSemaphorePermit,
}

impl AdmissionController {
    pub fn new(config: &Config) -> Self {
        Self {
            global: Arc::new(Semaphore::new(config.max_active_transfers)),
            upload_per_ip: Arc::new(PerIpSemaphores::new(config.max_active_uploads_per_ip)),
            download_per_ip: Arc::new(PerIpSemaphores::new(config.max_active_downloads_per_ip)),
            wait: Duration::from_secs(config.limit_wait_secs),
        }
    }

    pub async fn acquire(
        &self,
        direction: TransferDirection,
        ip: IpAddr,
    ) -> Result<AdmissionPermit> {
        let per_ip = match direction {
            TransferDirection::Upload => self.upload_per_ip.get(ip),
            TransferDirection::Download => self.download_per_ip.get(ip),
        };
        if self.wait.is_zero() {
            let global = self
                .global
                .clone()
                .try_acquire_owned()
                .map_err(|_| anyhow::anyhow!(LimitError::SlowDown))?;
            let per_ip = per_ip
                .try_acquire_owned()
                .map_err(|_| anyhow::anyhow!(LimitError::SlowDown))?;
            return Ok(AdmissionPermit {
                _global: global,
                _per_ip: per_ip,
            });
        }
        let acquire = async {
            let global = self
                .global
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("transfer admission controller closed"))?;
            let per_ip = per_ip
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("per-IP admission controller closed"))?;
            Ok::<_, anyhow::Error>(AdmissionPermit {
                _global: global,
                _per_ip: per_ip,
            })
        };
        tokio::time::timeout(self.wait, acquire)
            .await
            .map_err(|_| anyhow::anyhow!(LimitError::SlowDown))?
    }
}

struct PerIpSemaphores {
    limit: usize,
    entries: Mutex<HashMap<IpAddr, IpSemaphore>>,
    overflow: Arc<Semaphore>,
}

struct IpSemaphore {
    semaphore: Arc<Semaphore>,
    last_seen: Instant,
}

impl PerIpSemaphores {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            entries: Mutex::new(HashMap::new()),
            overflow: Arc::new(Semaphore::new(limit)),
        }
    }

    fn get(&self, ip: IpAddr) -> Arc<Semaphore> {
        let Ok(mut entries) = self.entries.lock() else {
            return self.overflow.clone();
        };
        let now = Instant::now();
        entries.retain(|_, entry| {
            now.duration_since(entry.last_seen) <= IP_STATE_TTL
                || entry.semaphore.available_permits() < self.limit
        });
        if let Some(entry) = entries.get_mut(&ip) {
            entry.last_seen = now;
            return entry.semaphore.clone();
        }
        if entries.len() >= MAX_IP_STATES {
            return self.overflow.clone();
        }
        let semaphore = Arc::new(Semaphore::new(self.limit));
        entries.insert(
            ip,
            IpSemaphore {
                semaphore: semaphore.clone(),
                last_seen: now,
            },
        );
        semaphore
    }
}

#[derive(Clone)]
pub struct BandwidthLimiter {
    global_upload: Arc<Mutex<RateSchedule>>,
    global_download: Arc<Mutex<RateSchedule>>,
    upload_per_ip: Arc<PerIpRateSchedules>,
    download_per_ip: Arc<PerIpRateSchedules>,
    upload_rate: u64,
    download_rate: u64,
    upload_rate_per_ip: u64,
    download_rate_per_ip: u64,
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
            upload_per_ip: Arc::new(PerIpRateSchedules::default()),
            download_per_ip: Arc::new(PerIpRateSchedules::default()),
            upload_rate: config.upload_rate_bps,
            download_rate: config.download_rate_bps,
            upload_rate_per_ip: config.upload_rate_bps_per_ip,
            download_rate_per_ip: config.download_rate_bps_per_ip,
            wait: Duration::from_secs(config.limit_wait_secs),
        }
    }

    pub async fn throttle(
        &self,
        direction: TransferDirection,
        ip: IpAddr,
        bytes: usize,
        bounded: bool,
    ) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let (global_rate, ip_rate, global, per_ip) = match direction {
            TransferDirection::Upload => (
                self.upload_rate,
                self.upload_rate_per_ip,
                self.global_upload.clone(),
                self.upload_per_ip.schedule(ip, self.upload_rate_per_ip),
            ),
            TransferDirection::Download => (
                self.download_rate,
                self.download_rate_per_ip,
                self.global_download.clone(),
                self.download_per_ip.schedule(ip, self.download_rate_per_ip),
            ),
        };
        let global_wait = reserve(&global, global_rate, bytes);
        let ip_wait = reserve(&per_ip, ip_rate, bytes);
        let wait = global_wait.max(ip_wait);
        if bounded && wait > self.wait {
            bail!(LimitError::SlowDown);
        }
        if !wait.is_zero() {
            sleep(wait).await;
        }
        Ok(())
    }
}

#[derive(Default)]
struct PerIpRateSchedules {
    entries: Mutex<HashMap<IpAddr, RateEntry>>,
    overflow: Arc<Mutex<RateSchedule>>,
}

struct RateEntry {
    schedule: Arc<Mutex<RateSchedule>>,
    last_seen: Instant,
}

impl PerIpRateSchedules {
    fn schedule(&self, ip: IpAddr, rate: u64) -> Arc<Mutex<RateSchedule>> {
        if rate == 0 {
            return Arc::new(Mutex::new(RateSchedule::default()));
        }
        let Ok(mut entries) = self.entries.lock() else {
            return self.overflow.clone();
        };
        let now = Instant::now();
        entries.retain(|_, entry| now.duration_since(entry.last_seen) <= IP_STATE_TTL);
        if let Some(entry) = entries.get_mut(&ip) {
            entry.last_seen = now;
            return entry.schedule.clone();
        }
        if entries.len() >= MAX_IP_STATES {
            return self.overflow.clone();
        }
        let schedule = Arc::new(Mutex::new(RateSchedule::default()));
        entries.insert(
            ip,
            RateEntry {
                schedule: schedule.clone(),
                last_seen: now,
            },
        );
        schedule
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
    trusted_proxies: Arc<Vec<IpNet>>,
}

impl TransferLimits {
    pub fn new(config: &Config) -> Self {
        Self {
            admission: AdmissionController::new(config),
            bandwidth: BandwidthLimiter::new(config),
            max_object_size: config.max_object_size,
            trusted_proxies: Arc::new(config.trusted_proxy_cidrs.clone()),
        }
    }

    pub fn client_ip(&self, peer: Option<SocketAddr>, headers: &HeaderMap) -> IpAddr {
        client_ip(peer, headers, &self.trusted_proxies)
    }
}

pub fn client_ip(
    peer: Option<SocketAddr>,
    headers: &HeaderMap,
    trusted_proxies: &[IpNet],
) -> IpAddr {
    let fallback = peer
        .map(|address| address.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    if !is_trusted(fallback, trusted_proxies) {
        return fallback;
    }
    let candidates = header_candidates(headers);
    for candidate in candidates.iter().rev() {
        if !is_trusted(*candidate, trusted_proxies) {
            return *candidate;
        }
    }
    candidates.first().copied().unwrap_or(fallback)
}

fn is_trusted(ip: IpAddr, trusted: &[IpNet]) -> bool {
    trusted.iter().any(|network| network.contains(&ip))
}

fn header_candidates(headers: &HeaderMap) -> Vec<IpAddr> {
    if let Some(value) = headers
        .get("forwarded")
        .and_then(|value| value.to_str().ok())
    {
        let values = value
            .split(',')
            .flat_map(|element| element.split(';'))
            .filter_map(|part| {
                let (name, value) = part.trim().split_once('=')?;
                name.trim()
                    .eq_ignore_ascii_case("for")
                    .then_some(value.trim())
            })
            .filter_map(parse_forwarded_ip)
            .collect::<Vec<_>>();
        if !values.is_empty() {
            return values;
        }
    }
    if let Some(value) = headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
    {
        let values = value.split(',').filter_map(parse_ip).collect::<Vec<_>>();
        if !values.is_empty() {
            return values;
        }
    }
    headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_ip)
        .into_iter()
        .collect()
}

fn parse_forwarded_ip(value: &str) -> Option<IpAddr> {
    parse_ip(value.trim().trim_matches('"'))
}

fn parse_ip(value: &str) -> Option<IpAddr> {
    let value = value.trim().trim_matches('"');
    if value == "unknown" || value.starts_with('_') {
        return None;
    }
    if let Some(value) = value.strip_prefix('[') {
        return value.split(']').next().and_then(|value| value.parse().ok());
    }
    if let Ok(ip) = value.parse() {
        return Some(ip);
    }
    value.rsplit_once(':')?.0.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn only_trusted_peers_can_select_forwarded_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("198.51.100.10"));
        let trusted = ["192.0.2.0/24".parse().unwrap()];
        assert_eq!(
            client_ip(
                Some("203.0.113.4:9000".parse().unwrap()),
                &headers,
                &trusted
            ),
            "203.0.113.4".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            client_ip(Some("192.0.2.4:9000".parse().unwrap()), &headers, &trusted),
            "198.51.100.10".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn parses_forwarded_chain_from_right_to_left() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "forwarded",
            HeaderValue::from_static("for=198.51.100.10;proto=https,for=192.0.2.8"),
        );
        let trusted = ["192.0.2.0/24".parse().unwrap()];
        assert_eq!(
            client_ip(Some("192.0.2.4:9000".parse().unwrap()), &headers, &trusted),
            "198.51.100.10".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn parses_forwarded_elements_and_ignores_non_for_parameters() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "forwarded",
            HeaderValue::from_static(
                "for=198.51.100.10;proto=https, for=192.0.2.8;host=storage.example",
            ),
        );
        let trusted = ["192.0.2.0/24".parse().unwrap()];
        assert_eq!(
            client_ip(Some("192.0.2.4:9000".parse().unwrap()), &headers, &trusted,),
            "198.51.100.10".parse::<IpAddr>().unwrap()
        );
    }

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
