use crate::model::{CorsConfiguration, TelegramBackend};
use anyhow::{Context, Result, bail};
use clap::Args;
use std::{net::SocketAddr, path::PathBuf};
use url::Url;

const MAX_TELEGRAM_CONCURRENCY: usize = 1024;
const MAX_TELEGRAM_TIMEOUT_SECS: u64 = 24 * 60 * 60;
const MAX_GC_INTERVAL_SECS: u64 = 365 * 24 * 60 * 60;
const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024;
const MAX_ACTIVE_TRANSFERS: usize = 1024;
const MAX_LIMIT_WAIT_SECS: u64 = 60 * 60;
const MAX_RATE_BPS: u64 = 1024 * 1024 * 1024 * 1024;
pub const MAX_GC_LIMIT: usize = 100_000;
pub const DEFAULT_BOT_API_CHUNK_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_GRAMMERS_CHUNK_SIZE: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_OBJECT_SIZE: i64 = 50 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, Args)]
pub struct ConfigArgs {
    #[arg(long, env = "TG2S3_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,

    #[arg(long, env = "TG2S3_DB_PATH")]
    pub db_path: Option<PathBuf>,

    #[arg(long, env = "TG2S3_LISTEN", default_value = "127.0.0.1:9000")]
    pub listen: SocketAddr,

    #[arg(long, env = "TG2S3_BOT_TOKEN")]
    pub bot_token: Option<String>,

    #[arg(long, env = "TG2S3_CHAT_ID")]
    pub chat_id: Option<i64>,

    #[arg(long, env = "TG2S3_TELEGRAM_BACKEND", default_value = "bot_api")]
    pub telegram_backend: String,

    #[arg(
        long,
        env = "TG2S3_TELEGRAM_API_URL",
        default_value = "https://api.telegram.org"
    )]
    pub telegram_api_url: String,

    #[arg(long, env = "TG2S3_LOCAL_BOT_API", default_value_t = false)]
    pub local_bot_api: bool,

    #[arg(long, env = "TG2S3_CHUNK_SIZE")]
    pub chunk_size: Option<usize>,

    #[arg(long, env = "TG2S3_UPLOAD_CONCURRENCY", default_value_t = 4)]
    pub upload_concurrency: usize,

    #[arg(long, env = "TG2S3_DOWNLOAD_CONCURRENCY", default_value_t = 4)]
    pub download_concurrency: usize,

    #[arg(long, env = "TG2S3_TELEGRAM_TIMEOUT_SECS", default_value_t = 300)]
    pub telegram_timeout_secs: u64,

    #[arg(long, env = "TG2S3_TELEGRAM_API_ID")]
    pub grammers_api_id: Option<i32>,

    #[arg(long, env = "TG2S3_TELEGRAM_API_HASH")]
    pub grammers_api_hash: Option<String>,

    #[arg(long, env = "TG2S3_GRAMMERS_SESSION_PATH")]
    pub grammers_session_path: Option<PathBuf>,

    #[arg(long, env = "TG2S3_GRAMMERS_CHAT_USERNAME")]
    pub grammers_chat_username: Option<String>,

    #[arg(long, env = "TG2S3_GRAMMERS_CHAT_ACCESS_HASH")]
    pub grammers_chat_access_hash: Option<i64>,

    #[arg(long, env = "TG2S3_GRAMMERS_MAX_FLOOD_WAIT_SECS", default_value_t = 30)]
    pub grammers_max_flood_wait_secs: u64,

    #[arg(long, env = "TG2S3_ACCESS_KEY")]
    pub access_key: Option<String>,

    #[arg(long, env = "TG2S3_SECRET_KEY")]
    pub secret_key: Option<String>,

    #[arg(long, env = "TG2S3_ALLOW_ANONYMOUS", default_value_t = false)]
    pub allow_anonymous: bool,

    #[arg(long, env = "TG2S3_REGION", default_value = "us-east-1")]
    pub region: String,

    #[arg(long, env = "TG2S3_PUBLIC_HOST")]
    pub public_host: Option<String>,

    #[arg(long, env = "TG2S3_INIT_BUCKETS", default_value = "")]
    pub init_buckets: String,

    #[arg(long, env = "TG2S3_CORS_ALLOWED_ORIGINS", default_value = "*")]
    pub cors_allowed_origins: String,

    #[arg(
        long,
        env = "TG2S3_CORS_ALLOWED_METHODS",
        default_value = "GET,POST,PUT,DELETE,HEAD"
    )]
    pub cors_allowed_methods: String,

    #[arg(long, env = "TG2S3_CORS_ALLOWED_HEADERS", default_value = "*")]
    pub cors_allowed_headers: String,

    #[arg(long, env = "TG2S3_CORS_EXPOSE_HEADERS", default_value = "ETag")]
    pub cors_expose_headers: String,

    #[arg(long, env = "TG2S3_CORS_MAX_AGE", default_value_t = 3600)]
    pub cors_max_age: u64,

    #[arg(long, env = "TG2S3_GC_INTERVAL", default_value_t = 300)]
    pub gc_interval: u64,

    #[arg(long, env = "TG2S3_GC_LIMIT", default_value_t = 100)]
    pub gc_limit: usize,

    #[arg(
        long,
        env = "TG2S3_MAX_OBJECT_SIZE",
        default_value_t = DEFAULT_MAX_OBJECT_SIZE as u64
    )]
    pub max_object_size: u64,

    #[arg(long, env = "TG2S3_MAX_ACTIVE_TRANSFERS", default_value_t = 16)]
    pub max_active_transfers: usize,

    #[arg(long, env = "TG2S3_LIMIT_WAIT_SECS", default_value_t = 5)]
    pub limit_wait_secs: u64,

    #[arg(long, env = "TG2S3_UPLOAD_RATE_BPS", default_value_t = 0)]
    pub upload_rate_bps: u64,

    #[arg(long, env = "TG2S3_DOWNLOAD_RATE_BPS", default_value_t = 0)]
    pub download_rate_bps: u64,
}

#[derive(Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub listen: SocketAddr,
    pub bot_token: String,
    pub chat_id: i64,
    pub telegram_backend: TelegramBackend,
    pub telegram_api_url: String,
    pub local_bot_api: bool,
    pub chunk_size: usize,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
    pub telegram_timeout_secs: u64,
    pub grammers_api_id: Option<i32>,
    pub grammers_api_hash: Option<String>,
    pub grammers_session_path: PathBuf,
    pub grammers_chat_username: Option<String>,
    pub grammers_chat_access_hash: Option<i64>,
    pub grammers_max_flood_wait_secs: u64,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub allow_anonymous: bool,
    pub region: String,
    pub public_host: Option<String>,
    pub init_buckets: Vec<String>,
    pub cors: CorsConfiguration,
    pub gc_interval: u64,
    pub gc_limit: usize,
    pub max_object_size: i64,
    pub max_active_transfers: usize,
    pub limit_wait_secs: u64,
    pub upload_rate_bps: u64,
    pub download_rate_bps: u64,
}

impl ConfigArgs {
    pub fn into_config(self, require_telegram: bool, require_auth: bool) -> Result<Config> {
        let telegram_backend = self
            .telegram_backend
            .parse::<TelegramBackend>()
            .map_err(|error| anyhow::anyhow!("TG2S3_TELEGRAM_BACKEND: {error}"))?;
        let chunk_size = self.chunk_size.unwrap_or(match telegram_backend {
            TelegramBackend::BotApi => DEFAULT_BOT_API_CHUNK_SIZE,
            TelegramBackend::Grammers => DEFAULT_GRAMMERS_CHUNK_SIZE,
        });
        let db_path = self
            .db_path
            .clone()
            .unwrap_or_else(|| self.data_dir.join("tg2s3.sqlite3"));
        if chunk_size == 0 || chunk_size > 2_000_000_000 {
            bail!("chunk size must be between 1 and 2,000,000,000 bytes");
        }
        let region = self.region.trim().to_string();
        if region.is_empty() {
            bail!("TG2S3_REGION must not be empty");
        }
        if telegram_backend == TelegramBackend::BotApi
            && !self.local_bot_api
            && chunk_size > 20 * 1024 * 1024
        {
            bail!(
                "public Bot API mode requires chunk size <= 20 MiB; enable TG2S3_LOCAL_BOT_API for larger chunks"
            );
        }
        if self.upload_concurrency == 0
            || self.download_concurrency == 0
            || self.upload_concurrency > MAX_TELEGRAM_CONCURRENCY
            || self.download_concurrency > MAX_TELEGRAM_CONCURRENCY
        {
            bail!(
                "upload and download concurrency must be between 1 and {MAX_TELEGRAM_CONCURRENCY}"
            );
        }
        if self.telegram_timeout_secs == 0 || self.telegram_timeout_secs > MAX_TELEGRAM_TIMEOUT_SECS
        {
            bail!("TG2S3_TELEGRAM_TIMEOUT_SECS must be between 1 and {MAX_TELEGRAM_TIMEOUT_SECS}");
        }
        if self.grammers_max_flood_wait_secs > MAX_TELEGRAM_TIMEOUT_SECS {
            bail!("TG2S3_GRAMMERS_MAX_FLOOD_WAIT_SECS is too large");
        }
        if self.gc_interval == 0
            || self.gc_interval > MAX_GC_INTERVAL_SECS
            || self.gc_limit == 0
            || self.gc_limit > MAX_GC_LIMIT
        {
            bail!(
                "GC interval must be between 1 and {MAX_GC_INTERVAL_SECS} seconds and GC limit between 1 and {MAX_GC_LIMIT}"
            );
        }
        let bot_token = self
            .bot_token
            .map(|value| value.trim().to_string())
            .unwrap_or_default();
        let chat_id = self.chat_id.unwrap_or_default();
        if require_telegram && bot_token.is_empty() {
            bail!("TG2S3_BOT_TOKEN is required for this command");
        }
        if require_telegram && chat_id == 0 {
            bail!("TG2S3_CHAT_ID is required for this command");
        }
        let grammers_api_hash = self
            .grammers_api_hash
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let grammers_chat_username = self
            .grammers_chat_username
            .map(|value| value.trim().trim_start_matches('@').to_string())
            .filter(|value| !value.is_empty());
        if require_telegram && telegram_backend == TelegramBackend::Grammers {
            if self.grammers_api_id.is_none_or(|value| value <= 0) {
                bail!("TG2S3_TELEGRAM_API_ID is required and must be positive for grammers");
            }
            if grammers_api_hash.is_none() {
                bail!("TG2S3_TELEGRAM_API_HASH is required for grammers");
            }
            if grammers_chat_username.is_some() == self.grammers_chat_access_hash.is_some() {
                bail!(
                    "configure exactly one of TG2S3_GRAMMERS_CHAT_USERNAME and TG2S3_GRAMMERS_CHAT_ACCESS_HASH"
                );
            }
        }
        if self.max_object_size == 0 || self.max_object_size > MAX_OBJECT_SIZE {
            bail!("TG2S3_MAX_OBJECT_SIZE must be between 1 and {MAX_OBJECT_SIZE} bytes");
        }
        if self.max_active_transfers == 0 || self.max_active_transfers > MAX_ACTIVE_TRANSFERS {
            bail!("TG2S3_MAX_ACTIVE_TRANSFERS must be between 1 and {MAX_ACTIVE_TRANSFERS}");
        }
        if self.limit_wait_secs > MAX_LIMIT_WAIT_SECS {
            bail!("TG2S3_LIMIT_WAIT_SECS must be at most {MAX_LIMIT_WAIT_SECS}");
        }
        for (name, value) in [
            ("TG2S3_UPLOAD_RATE_BPS", self.upload_rate_bps),
            ("TG2S3_DOWNLOAD_RATE_BPS", self.download_rate_bps),
        ] {
            if value > MAX_RATE_BPS {
                bail!("{name} must be at most {MAX_RATE_BPS} bytes per second");
            }
        }
        match (&self.access_key, &self.secret_key) {
            (Some(access_key), Some(secret_key))
                if !access_key.is_empty() && !secret_key.is_empty() => {}
            (None, None) => {}
            _ => bail!("TG2S3_ACCESS_KEY and TG2S3_SECRET_KEY must be configured together"),
        }
        if require_auth
            && !self.allow_anonymous
            && (self.access_key.is_none() || self.secret_key.is_none())
        {
            bail!("configure SigV4 credentials or explicitly set TG2S3_ALLOW_ANONYMOUS=true");
        }
        let init_buckets = split_list(&self.init_buckets);
        for bucket in &init_buckets {
            validate_bucket_name(bucket)?;
        }
        let cors = CorsConfiguration {
            allowed_origins: split_list(&self.cors_allowed_origins),
            allowed_methods: split_list(&self.cors_allowed_methods)
                .into_iter()
                .map(|value| value.to_ascii_uppercase())
                .collect(),
            allowed_headers: split_list(&self.cors_allowed_headers),
            expose_headers: split_list(&self.cors_expose_headers),
            max_age_seconds: self.cors_max_age,
        };
        validate_cors(&cors)?;
        std::fs::create_dir_all(&self.data_dir)
            .with_context(|| format!("create data directory {}", self.data_dir.display()))?;
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create database directory {}", parent.display()))?;
        }
        let telegram_api_url = normalize_telegram_api_url(&self.telegram_api_url)?;
        let public_host = normalize_public_host(self.public_host)?;
        let grammers_session_path = self
            .grammers_session_path
            .unwrap_or_else(|| self.data_dir.join("grammers.session.sqlite3"));
        if let Some(parent) = grammers_session_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("create grammers session directory {}", parent.display())
            })?;
        }
        Ok(Config {
            data_dir: self.data_dir,
            db_path,
            listen: self.listen,
            bot_token,
            chat_id,
            telegram_backend,
            telegram_api_url,
            local_bot_api: self.local_bot_api,
            chunk_size,
            upload_concurrency: self.upload_concurrency,
            download_concurrency: self.download_concurrency,
            telegram_timeout_secs: self.telegram_timeout_secs,
            grammers_api_id: self.grammers_api_id,
            grammers_api_hash,
            grammers_session_path,
            grammers_chat_username,
            grammers_chat_access_hash: self.grammers_chat_access_hash,
            grammers_max_flood_wait_secs: self.grammers_max_flood_wait_secs,
            access_key: self.access_key,
            secret_key: self.secret_key,
            allow_anonymous: self.allow_anonymous,
            region,
            public_host,
            init_buckets,
            cors,
            gc_interval: self.gc_interval,
            gc_limit: self.gc_limit,
            max_object_size: i64::try_from(self.max_object_size)
                .context("TG2S3_MAX_OBJECT_SIZE does not fit SQLite integer range")?,
            max_active_transfers: self.max_active_transfers,
            limit_wait_secs: self.limit_wait_secs,
            upload_rate_bps: self.upload_rate_bps,
            download_rate_bps: self.download_rate_bps,
        })
    }
}

fn normalize_public_host(value: Option<String>) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim().trim_end_matches('/');
    let url_value = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };
    let url = Url::parse(&url_value).context("parse TG2S3_PUBLIC_HOST")?;
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        bail!("TG2S3_PUBLIC_HOST must be a host or URL without a path");
    }
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("TG2S3_PUBLIC_HOST has no host"))?;
    Ok(Some(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }))
}

fn normalize_telegram_api_url(value: &str) -> Result<String> {
    let trimmed = value.trim().trim_end_matches('/');
    let url = Url::parse(trimmed).context("parse TG2S3_TELEGRAM_API_URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("TG2S3_TELEGRAM_API_URL must be an HTTP(S) URL without a query or fragment");
    }
    Ok(trimmed.to_string())
}

pub fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub fn validate_bucket_name(bucket: &str) -> Result<()> {
    if bucket.is_empty()
        || bucket.len() > 63
        || !bucket
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.')
        || !bucket.as_bytes()[0].is_ascii_alphanumeric()
        || !bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric()
    {
        bail!("invalid bucket name: {bucket}");
    }
    Ok(())
}

fn validate_cors(cors: &CorsConfiguration) -> Result<()> {
    if cors.allowed_origins.is_empty()
        || cors.allowed_methods.is_empty()
        || cors.allowed_headers.is_empty()
    {
        bail!("CORS origins, methods and headers must not be empty");
    }
    if cors
        .allowed_origins
        .iter()
        .any(|origin| origin == "*" && cors.allowed_origins.len() > 1)
    {
        bail!("CORS wildcard origin cannot be combined with explicit origins");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_init_buckets_and_open_cors() -> Result<()> {
        let args = ConfigArgs {
            data_dir: PathBuf::from("./data"),
            db_path: None,
            listen: "127.0.0.1:9000".parse().unwrap(),
            bot_token: None,
            chat_id: None,
            telegram_backend: "bot_api".to_string(),
            telegram_api_url: "https://api.telegram.org".to_string(),
            local_bot_api: false,
            chunk_size: Some(16 * 1024 * 1024),
            upload_concurrency: 1,
            download_concurrency: 1,
            telegram_timeout_secs: 300,
            grammers_api_id: None,
            grammers_api_hash: None,
            grammers_session_path: None,
            grammers_chat_username: None,
            grammers_chat_access_hash: None,
            grammers_max_flood_wait_secs: 30,
            access_key: None,
            secret_key: None,
            allow_anonymous: true,
            region: " us-east-1 ".to_string(),
            public_host: None,
            init_buckets: "A, cloudreve".to_string(),
            cors_allowed_origins: "*".to_string(),
            cors_allowed_methods: "GET,PUT".to_string(),
            cors_allowed_headers: "*".to_string(),
            cors_expose_headers: "ETag".to_string(),
            cors_max_age: 3600,
            gc_interval: 300,
            gc_limit: 100,
            max_object_size: DEFAULT_MAX_OBJECT_SIZE as u64,
            max_active_transfers: 16,
            limit_wait_secs: 5,
            upload_rate_bps: 0,
            download_rate_bps: 0,
        };
        let config = args.clone().into_config(false, false)?;
        assert_eq!(config.init_buckets, ["A", "cloudreve"]);
        assert_eq!(config.cors.allowed_origins, ["*"]);
        assert_eq!(config.region, "us-east-1");
        let mut grammers = args.clone();
        grammers.telegram_backend = "grammers".to_string();
        grammers.chunk_size = None;
        let grammers_config = grammers.into_config(false, false)?;
        assert_eq!(grammers_config.chunk_size, DEFAULT_GRAMMERS_CHUNK_SIZE);
        let mut invalid_region = args.clone();
        invalid_region.region = " ".to_string();
        assert!(invalid_region.into_config(false, false).is_err());
        let mut invalid = args;
        invalid.telegram_timeout_secs = 0;
        assert!(invalid.into_config(false, false).is_err());
        Ok(())
    }

    #[test]
    fn validates_telegram_api_base_url() -> Result<()> {
        assert_eq!(
            normalize_telegram_api_url("http://telegram-bot-api:8081/")?,
            "http://telegram-bot-api:8081"
        );
        assert!(normalize_telegram_api_url("telegram-bot-api:8081").is_err());
        assert!(normalize_telegram_api_url("https://api.telegram.org?x=1").is_err());
        Ok(())
    }
}
