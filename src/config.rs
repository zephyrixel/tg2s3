use crate::model::CorsConfiguration;
use anyhow::{Context, Result, bail};
use clap::Args;
use std::{net::SocketAddr, path::PathBuf};
use url::Url;

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

    #[arg(
        long,
        env = "TG2S3_TELEGRAM_API_URL",
        default_value = "https://api.telegram.org"
    )]
    pub telegram_api_url: String,

    #[arg(long, env = "TG2S3_LOCAL_BOT_API", default_value_t = false)]
    pub local_bot_api: bool,

    #[arg(long, env = "TG2S3_CHUNK_SIZE", default_value_t = 16 * 1024 * 1024)]
    pub chunk_size: usize,

    #[arg(long, env = "TG2S3_UPLOAD_CONCURRENCY", default_value_t = 4)]
    pub upload_concurrency: usize,

    #[arg(long, env = "TG2S3_DOWNLOAD_CONCURRENCY", default_value_t = 4)]
    pub download_concurrency: usize,

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
}

#[derive(Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub listen: SocketAddr,
    pub bot_token: String,
    pub chat_id: i64,
    pub telegram_api_url: String,
    pub local_bot_api: bool,
    pub chunk_size: usize,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub allow_anonymous: bool,
    pub region: String,
    pub public_host: Option<String>,
    pub init_buckets: Vec<String>,
    pub cors: CorsConfiguration,
    pub gc_interval: u64,
    pub gc_limit: usize,
}

impl ConfigArgs {
    pub fn into_config(self, require_telegram: bool, require_auth: bool) -> Result<Config> {
        let db_path = self
            .db_path
            .clone()
            .unwrap_or_else(|| self.data_dir.join("tg2s3.sqlite3"));
        if self.chunk_size == 0 || self.chunk_size > 2_000_000_000 {
            bail!("chunk size must be between 1 and 2,000,000,000 bytes");
        }
        if !self.local_bot_api && self.chunk_size > 20 * 1024 * 1024 {
            bail!(
                "public Bot API mode requires chunk size <= 20 MiB; enable TG2S3_LOCAL_BOT_API for larger chunks"
            );
        }
        if self.upload_concurrency == 0 || self.download_concurrency == 0 {
            bail!("concurrency must be greater than zero");
        }
        if self.gc_interval == 0 || self.gc_limit == 0 {
            bail!("GC interval and limit must be greater than zero");
        }
        let bot_token = self.bot_token.unwrap_or_default();
        let chat_id = self.chat_id.unwrap_or_default();
        if require_telegram && bot_token.is_empty() {
            bail!("TG2S3_BOT_TOKEN is required for this command");
        }
        if require_telegram && chat_id == 0 {
            bail!("TG2S3_CHAT_ID is required for this command");
        }
        match (&self.access_key, &self.secret_key) {
            (Some(_), Some(_)) | (None, None) => {}
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
        let public_host = normalize_public_host(self.public_host)?;
        Ok(Config {
            data_dir: self.data_dir,
            db_path,
            listen: self.listen,
            bot_token,
            chat_id,
            telegram_api_url: self.telegram_api_url.trim_end_matches('/').to_string(),
            local_bot_api: self.local_bot_api,
            chunk_size: self.chunk_size,
            upload_concurrency: self.upload_concurrency,
            download_concurrency: self.download_concurrency,
            access_key: self.access_key,
            secret_key: self.secret_key,
            allow_anonymous: self.allow_anonymous,
            region: self.region,
            public_host,
            init_buckets,
            cors,
            gc_interval: self.gc_interval,
            gc_limit: self.gc_limit,
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
            telegram_api_url: "https://api.telegram.org".to_string(),
            local_bot_api: false,
            chunk_size: 16 * 1024 * 1024,
            upload_concurrency: 1,
            download_concurrency: 1,
            access_key: None,
            secret_key: None,
            allow_anonymous: true,
            region: "us-east-1".to_string(),
            public_host: None,
            init_buckets: "A, cloudreve".to_string(),
            cors_allowed_origins: "*".to_string(),
            cors_allowed_methods: "GET,PUT".to_string(),
            cors_allowed_headers: "*".to_string(),
            cors_expose_headers: "ETag".to_string(),
            cors_max_age: 3600,
            gc_interval: 300,
            gc_limit: 100,
        };
        let config = args.into_config(false, false)?;
        assert_eq!(config.init_buckets, ["A", "cloudreve"]);
        assert_eq!(config.cors.allowed_origins, ["*"]);
        Ok(())
    }
}
