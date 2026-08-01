use anyhow::{Context, Result, bail};
use clap::Args;
use std::{net::SocketAddr, path::PathBuf};

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
}

#[derive(Clone, Debug)]
pub struct Config {
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
}

impl ConfigArgs {
    pub fn into_config(self, require_telegram: bool, require_auth: bool) -> Result<Config> {
        let db_path = self
            .db_path
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
        std::fs::create_dir_all(&self.data_dir)
            .with_context(|| format!("create data directory {}", self.data_dir.display()))?;
        Ok(Config {
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
            public_host: self.public_host,
        })
    }
}
