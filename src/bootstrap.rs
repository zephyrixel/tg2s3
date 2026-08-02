use crate::config::Config;
use crate::db::Db;
use crate::engine::Engine;
use crate::model::TelegramBackend;
use crate::telegram::TelegramClient;
use anyhow::{Result, bail};

pub async fn prepare(config: Config, verify_telegram: bool) -> Result<Engine> {
    tracing::info!(
        data_dir = %config.data_dir.display(),
        database = %config.db_path.display(),
        buckets = ?config.init_buckets,
        local_bot_api = config.local_bot_api,
        telegram_backend = %config.telegram_backend,
        chunk_size = config.chunk_size,
        upload_concurrency = config.upload_concurrency,
        download_concurrency = config.download_concurrency,
        max_object_size = config.max_object_size,
        max_active_transfers = config.max_active_transfers,
        "configuration validated"
    );
    let db = Db::open(&config.db_path).await?;
    let enable_grammers = verify_telegram
        && (config.telegram_backend == TelegramBackend::Grammers
            || db.has_backend(TelegramBackend::Grammers).await?);
    let telegram = if verify_telegram {
        TelegramClient::connect(&config, enable_grammers).await?
    } else {
        TelegramClient::new(&config)?
    };
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
