use crate::config::Config;
use crate::db::Db;
use crate::engine::Engine;
use crate::telegram::TelegramClient;
use anyhow::{Result, bail};

pub async fn prepare(config: Config, verify_telegram: bool) -> Result<Engine> {
    tracing::info!(
        data_dir = %config.data_dir.display(),
        database = %config.db_path.display(),
        buckets = ?config.init_buckets,
        "configuration validated"
    );
    let db = Db::open(&config.db_path).await?;
    let telegram = TelegramClient::new(&config)?;
    let engine = Engine::new(config, db, telegram);

    for bucket in &engine.config.init_buckets {
        engine.db.create_bucket(bucket).await?;
    }
    let integrity = engine.db.integrity_check().await?;
    if integrity != "ok" {
        bail!("SQLite integrity check failed: {integrity}");
    }
    if verify_telegram {
        let check = engine.telegram.verify().await?;
        tracing::info!(chat = %check.title, kind = %check.kind, "Telegram storage chat verified");
    }
    Ok(engine)
}
