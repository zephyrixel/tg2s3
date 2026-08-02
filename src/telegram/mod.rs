mod bot_api;
mod grammers;
mod session;

use crate::config::Config;
use crate::model::{BlockRef, TelegramBackend};
use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use std::sync::Arc;

pub use bot_api::{BotApiClient, ChatCheck};

#[derive(Clone, Debug)]
pub struct StoredDocument {
    pub backend: TelegramBackend,
    pub message_id: i64,
    pub document_id: Option<i64>,
    pub file_id: String,
    pub file_unique_id: String,
    pub file_size: i64,
    pub message_date: i64,
}

#[derive(Clone)]
pub struct TelegramClient {
    active_backend: TelegramBackend,
    bot_api: BotApiClient,
    grammers: Option<Arc<grammers::GrammersClient>>,
}

impl TelegramClient {
    pub fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            active_backend: config.telegram_backend,
            bot_api: BotApiClient::new(config)?,
            grammers: None,
        })
    }

    pub async fn connect(config: &Config, enable_grammers: bool) -> Result<Self> {
        let grammers = if enable_grammers {
            Some(Arc::new(grammers::GrammersClient::connect(config).await?))
        } else {
            None
        };
        if config.telegram_backend == TelegramBackend::Grammers && grammers.is_none() {
            bail!("grammers backend was selected but could not be initialized");
        }
        Ok(Self {
            active_backend: config.telegram_backend,
            bot_api: BotApiClient::new(config)?,
            grammers,
        })
    }

    pub fn active_backend(&self) -> TelegramBackend {
        self.active_backend
    }

    pub async fn verify(&self) -> Result<ChatCheck> {
        match self.active_backend {
            TelegramBackend::BotApi => self.bot_api.verify().await,
            TelegramBackend::Grammers => {
                self.grammers
                    .as_ref()
                    .ok_or_else(|| anyhow!("grammers backend is not initialized"))?
                    .verify()
                    .await
            }
        }
    }

    pub async fn upload_chunk(&self, data: Bytes, filename: &str) -> Result<StoredDocument> {
        match self.active_backend {
            TelegramBackend::BotApi => Ok(self.bot_api.upload_chunk(data, filename).await?.into()),
            TelegramBackend::Grammers => {
                self.grammers
                    .as_ref()
                    .ok_or_else(|| anyhow!("grammers backend is not initialized"))?
                    .upload_chunk(data, filename)
                    .await
            }
        }
    }

    pub async fn download_chunk(&self, block: &BlockRef, start: i64, end: i64) -> Result<Bytes> {
        match block.backend {
            TelegramBackend::BotApi => {
                self.bot_api
                    .download_chunk(&block.file_id, start, end)
                    .await
            }
            TelegramBackend::Grammers => {
                self.grammers
                    .as_ref()
                    .ok_or_else(|| anyhow!("grammers backend is not initialized"))?
                    .download_chunk(block.message_id, start, end)
                    .await
            }
        }
    }

    pub async fn delete_message(&self, block: &BlockRef) -> Result<()> {
        match block.backend {
            TelegramBackend::BotApi => self.bot_api.delete_message(block.message_id).await,
            TelegramBackend::Grammers => {
                self.grammers
                    .as_ref()
                    .ok_or_else(|| anyhow!("grammers backend is not initialized"))?
                    .delete_message(block.message_id)
                    .await
            }
        }
    }

    pub async fn delete_message_by_id(
        &self,
        backend: TelegramBackend,
        message_id: i64,
    ) -> Result<()> {
        match backend {
            TelegramBackend::BotApi => self.bot_api.delete_message(message_id).await,
            TelegramBackend::Grammers => {
                self.grammers
                    .as_ref()
                    .ok_or_else(|| anyhow!("grammers backend is not initialized"))?
                    .delete_message(message_id)
                    .await
            }
        }
    }
}

pub fn is_missing_message(error: &anyhow::Error) -> bool {
    bot_api::is_missing_message(error)
        || error
            .to_string()
            .to_ascii_lowercase()
            .contains("message not found")
}

impl From<bot_api::UploadedDocument> for StoredDocument {
    fn from(document: bot_api::UploadedDocument) -> Self {
        Self {
            backend: TelegramBackend::BotApi,
            message_id: document.message_id,
            document_id: None,
            file_id: document.file_id,
            file_unique_id: document.file_unique_id,
            file_size: document.file_size,
            message_date: document.message_date,
        }
    }
}
