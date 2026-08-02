use anyhow::Result;
use clap::{Parser, Subcommand};
use std::time::Duration;
use tg2s3::auth::SigV4;
use tg2s3::bootstrap;
use tg2s3::config::{Config, ConfigArgs};
use tg2s3::s3;
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
    Init,
    Serve,
    #[command(alias = "check")]
    Doctor,
    Inspect,
    Gc {
        #[arg(long)]
        limit: Option<usize>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();
    let cli = Cli::parse();
    let requires_telegram = !matches!(cli.command, Command::Inspect);
    let requires_auth = matches!(cli.command, Command::Serve);
    let config = cli.config.into_config(requires_telegram, requires_auth)?;
    match cli.command {
        Command::Init => init(config).await,
        Command::Serve => serve(config).await,
        Command::Doctor => doctor(config).await,
        Command::Inspect => inspect(config).await,
        Command::Gc { limit } => gc(config, limit).await,
    }
}

async fn init(config: Config) -> Result<()> {
    let engine = bootstrap::prepare(config.clone(), true).await?;
    println!("initialized database: {}", config.db_path.display());
    println!("buckets: {}", engine.db.list_buckets().await?.len());
    println!("SQLite integrity: {}", engine.db.integrity_check().await?);
    Ok(())
}

async fn serve(config: Config) -> Result<()> {
    let listen = config.listen;
    let gc_interval = config.gc_interval;
    let gc_limit = config.gc_limit;
    let engine = bootstrap::prepare(config.clone(), true).await?;
    let auth = SigV4 {
        access_key: config.access_key,
        secret_key: config.secret_key,
        region: config.region,
        allow_anonymous: config.allow_anonymous,
    };
    let app = s3::router(s3::AppState {
        engine: engine.clone(),
        auth,
        public_host: config.public_host,
        cors: config.cors,
    });
    let gc_engine = engine.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(gc_interval));
        loop {
            interval.tick().await;
            match gc_engine.run_gc(gc_limit).await {
                Ok(processed) if processed > 0 => {
                    tracing::info!(
                        processed,
                        "background Telegram garbage collection completed"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(%error, "background Telegram garbage collection failed");
                }
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "S3 endpoint listening");
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn doctor(config: Config) -> Result<()> {
    let engine = bootstrap::prepare(config, true).await?;
    println!("SQLite integrity: {}", engine.db.integrity_check().await?);
    println!("buckets: {}", engine.db.list_buckets().await?.len());
    Ok(())
}

async fn inspect(config: Config) -> Result<()> {
    let engine = bootstrap::prepare(config, false).await?;
    println!("database ready");
    println!("SQLite integrity: {}", engine.db.integrity_check().await?);
    println!("buckets: {}", engine.db.list_buckets().await?.len());
    Ok(())
}

async fn gc(config: Config, limit: Option<usize>) -> Result<()> {
    let engine = bootstrap::prepare(config, true).await?;
    let processed = engine
        .run_gc(limit.unwrap_or(engine.config.gc_limit))
        .await?;
    println!("processed {processed} garbage records");
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler")
                .recv()
                .await;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
