mod auth;
mod config;
mod db;
mod engine;
mod model;
mod s3;
mod telegram;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::{Config, ConfigArgs};
use db::Db;
use engine::Engine;
use telegram::TelegramClient;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "tg2s3",
    about = "S3-compatible object storage backed by Telegram"
)]
struct Cli {
    #[command(flatten)]
    config: ConfigArgs,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve,
    Doctor,
    Inspect,
    Gc {
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    let requires_telegram = !matches!(cli.command, Command::Inspect);
    let requires_auth = matches!(cli.command, Command::Serve);
    let config = cli.config.into_config(requires_telegram, requires_auth)?;
    match cli.command {
        Command::Serve => serve(config).await,
        Command::Doctor => doctor(config).await,
        Command::Inspect => inspect(config),
        Command::Gc { limit } => gc(config, limit).await,
    }
}

fn open_db(config: &Config) -> Result<Db> {
    Db::open(&config.db_path)
}

async fn build_engine(config: Config) -> Result<Engine> {
    let db = open_db(&config)?;
    let telegram = TelegramClient::new(&config)?;
    Ok(Engine::new(config, db, telegram))
}

async fn serve(config: Config) -> Result<()> {
    let listen = config.listen;
    let engine = build_engine(config.clone()).await?;
    let check = engine.telegram.verify().await?;
    tracing::info!(chat = %check.title, kind = %check.kind, "Telegram storage chat verified");
    let auth = auth::SigV4 {
        access_key: config.access_key,
        secret_key: config.secret_key,
        region: config.region,
        allow_anonymous: config.allow_anonymous,
    };
    let app = s3::router(s3::AppState {
        engine: engine.clone(),
        auth,
        public_host: config.public_host,
    });
    let gc_engine = engine.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            interval.tick().await;
            if let Err(error) = gc_engine.run_gc(100).await {
                tracing::error!(%error, "background Telegram garbage collection failed");
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "S3 endpoint listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn doctor(config: Config) -> Result<()> {
    let engine = build_engine(config).await?;
    let check = engine.telegram.verify().await?;
    println!("Telegram chat verified: {} ({})", check.title, check.kind);
    println!("SQLite integrity: {}", engine.db.integrity_check()?);
    Ok(())
}

fn inspect(config: Config) -> Result<()> {
    let db = open_db(&config)?;
    println!("database: {}", config.db_path.display());
    println!("SQLite integrity: {}", db.integrity_check()?);
    println!("buckets: {}", db.list_buckets()?.len());
    Ok(())
}

async fn gc(config: Config, limit: usize) -> Result<()> {
    let engine = build_engine(config).await?;
    println!("processed {} garbage records", engine.run_gc(limit).await?);
    Ok(())
}
