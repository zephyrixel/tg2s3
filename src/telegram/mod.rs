mod bot_api;
mod download;
mod grammers;
mod session;

use crate::config::Config;
use crate::model::{BlockRef, TelegramBackend};
use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;

pub use bot_api::{BotApiClient, ChatCheck};

pub type UploadReader = Pin<Box<dyn AsyncRead + Send + Unpin + 'static>>;
pub type DownloadStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;

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

    pub async fn upload_stream(
        &self,
        reader: UploadReader,
        size: u64,
        filename: &str,
    ) -> Result<StoredDocument> {
        match self.active_backend {
            TelegramBackend::BotApi => Ok(self
                .bot_api
                .upload_stream(reader, size, filename)
                .await?
                .into()),
            TelegramBackend::Grammers => {
                self.grammers
                    .as_ref()
                    .ok_or_else(|| anyhow!("grammers backend is not initialized"))?
                    .upload_stream(reader, size, filename)
                    .await
            }
        }
    }

    pub async fn download_stream(
        &self,
        block: &BlockRef,
        start: i64,
        end: i64,
    ) -> Result<DownloadStream> {
        match block.backend {
            TelegramBackend::BotApi => {
                self.bot_api
                    .download_stream(&block.file_id, start, end)
                    .await
            }
            TelegramBackend::Grammers => {
                self.grammers
                    .as_ref()
                    .ok_or_else(|| anyhow!("grammers backend is not initialized"))?
                    .download_stream(block.message_id, start, end)
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
    let text = error.to_string().to_ascii_lowercase();
    bot_api::is_missing_message(error)
        || text.contains("message not found")
        || text.contains("message_id_invalid")
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

#[cfg(test)]
mod tests {
    use super::is_missing_message;

    #[test]
    fn recognizes_missing_grammers_message_ids() {
        let error =
            anyhow::anyhow!("grammers Telegram request failed: RPC error MESSAGE_ID_INVALID");
        assert!(is_missing_message(&error));
        assert!(!is_missing_message(&anyhow::anyhow!(
            "grammers Telegram request failed: RPC error MESSAGE_DELETE_FORBIDDEN"
        )));
    }
}
