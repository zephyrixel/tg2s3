use super::download::exact_range_stream;
use super::{ChatCheck, StoredDocument};
use super::{DownloadStream, UploadReader};
use crate::config::Config;
use crate::model::TelegramBackend;
use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use futures_util::stream;
use grammers_client::client::AutoSleep;
use grammers_client::client::ClientConfiguration;
use grammers_client::media::Media;
use grammers_client::message::InputMessage;
use grammers_client::peer::Peer;
use grammers_client::{Client, SenderPool};
use grammers_session::types::{ChannelKind, PeerId, PeerRef};
use std::sync::Arc;
use std::time::Duration;

use super::session::SqlxSession;

const DOWNLOAD_CHUNK_SIZE: i32 = 512 * 1024;

pub struct GrammersClient {
    client: Client,
    peer: PeerRef,
    chat_id: i64,
}

impl GrammersClient {
    pub async fn connect(config: &Config) -> Result<Self> {
        let expected_bot_id = bot_id_from_token(&config.bot_token)?;
        let api_id = config
            .grammers_api_id
            .ok_or_else(|| anyhow!("TG2S3_TELEGRAM_API_ID is required for grammers"))?;
        let api_hash = config
            .grammers_api_hash
            .as_deref()
            .ok_or_else(|| anyhow!("TG2S3_TELEGRAM_API_HASH is required for grammers"))?;
        let session = Arc::new(
            SqlxSession::open(&config.grammers_session_path)
                .await
                .with_context(|| {
                    format!(
                        "open grammers session {}",
                        config.grammers_session_path.display()
                    )
                })?,
        );
        let pool = SenderPool::new(Arc::clone(&session), api_id);
        let client_configuration = ClientConfiguration {
            retry_policy: Box::new(AutoSleep {
                threshold: Duration::from_secs(config.grammers_max_flood_wait_secs),
                io_errors_as_flood_of: None,
            }),
            auto_cache_peers: true,
        };
        let client = Client::with_configuration(pool.handle, client_configuration);
        tokio::spawn(async move {
            let _updates = pool.updates;
            pool.runner.run().await;
            tracing::error!("grammers sender pool stopped");
        });
        if !client.is_authorized().await.map_err(invocation_error)? {
            client
                .bot_sign_in(&config.bot_token, api_hash)
                .await
                .map_err(invocation_error)?;
        }
        let me = client.get_me().await.map_err(invocation_error)?;
        if me.id().bare_id() != Some(expected_bot_id) {
            bail!(
                "grammers session belongs to bot {}, expected bot {}",
                me.id().bare_id().unwrap_or_default(),
                expected_bot_id
            );
        }
        let peer = resolve_storage_peer(&client, config).await?;
        Ok(Self {
            client,
            peer,
            chat_id: config.chat_id,
        })
    }

    pub async fn verify(&self) -> Result<ChatCheck> {
        let peer = self
            .client
            .resolve_peer(self.peer)
            .await
            .map_err(|error| anyhow!("grammers upload failed: {error}"))?;
        let Peer::Channel(channel) = peer else {
            bail!("grammers storage peer must be a channel or supergroup");
        };
        if channel.id().bot_api_dialog_id() != Some(self.chat_id) {
            bail!("grammers storage peer does not match TG2S3_CHAT_ID");
        }
        let me = self.client.get_me().await.map_err(invocation_error)?;
        let permissions = self
            .client
            .get_permissions(self.peer, PeerRef::from(&me.raw))
            .await
            .map_err(invocation_error)?;
        if !permissions.is_admin() {
            bail!("grammers account must be an administrator of the storage chat");
        }
        if let Some(rights) = channel.admin_rights() {
            if !rights.delete_messages {
                bail!("grammers account needs permission to delete storage messages");
            }
            if channel.kind() == Some(ChannelKind::Broadcast) && !rights.post_messages {
                bail!("grammers account needs permission to post in the storage channel");
            }
        }
        Ok(ChatCheck {
            title: channel.title().to_string(),
            kind: "channel".to_string(),
        })
    }

    pub async fn upload_stream(
        &self,
        mut reader: UploadReader,
        size: u64,
        filename: &str,
    ) -> Result<StoredDocument> {
        let size = usize::try_from(size).context("grammers upload size does not fit usize")?;
        let uploaded = self
            .client
            .upload_stream(&mut reader, size, filename.to_string())
            .await
            .map_err(|error| anyhow!("grammers upload failed: {error}"))?;
        let message = self
            .client
            .send_message(self.peer, InputMessage::new().silent(true).file(uploaded))
            .await
            .map_err(invocation_error)?;
        let document = match message.media() {
            Some(Media::Document(document)) => document,
            Some(_) => bail!("grammers returned a non-document media"),
            None => bail!("grammers returned a message without media"),
        };
        let file_size = document
            .size()
            .and_then(|size| i64::try_from(size).ok())
            .ok_or_else(|| anyhow!("grammers document size is unavailable"))?;
        let expected_size = i64::try_from(size).context("grammers upload size is too large")?;
        if file_size != expected_size {
            bail!("grammers stored chunk with size {file_size}, expected {expected_size}");
        }
        Ok(StoredDocument {
            backend: TelegramBackend::Grammers,
            message_id: i64::from(message.id()),
            document_id: Some(document.id()),
            file_id: String::new(),
            file_unique_id: document.id().to_string(),
            file_size,
            message_date: message.date().timestamp(),
        })
    }

    pub async fn download_stream(
        &self,
        message_id: i64,
        start: i64,
        end: i64,
    ) -> Result<DownloadStream> {
        if start < 0 || end < start {
            bail!("invalid Telegram range");
        }
        let message_id = i32::try_from(message_id).context("Telegram message ID is too large")?;
        let message = self
            .client
            .get_messages_by_id(self.peer, &[message_id])
            .await
            .map_err(invocation_error)?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| anyhow!("Telegram message not found"))?;
        let document = match message.media() {
            Some(Media::Document(document)) => document,
            _ => bail!("Telegram message has no document"),
        };
        if let Some(size) = document.size()
            && end >= i64::try_from(size).unwrap_or(i64::MAX)
        {
            bail!("Telegram range exceeds document size");
        }
        let chunk_size = i64::from(DOWNLOAD_CHUNK_SIZE);
        let skip = start / chunk_size;
        let iterator_offset = skip * chunk_size;
        let skip = i32::try_from(skip).context("Telegram download offset is too large")?;
        let iterator = self
            .client
            .iter_download(&document)
            .chunk_size(DOWNLOAD_CHUNK_SIZE)
            .skip_chunks(skip);
        let source = stream::unfold((iterator, true), |(mut iterator, active)| async move {
            if !active {
                return None;
            }
            match iterator.next().await {
                Ok(Some(chunk)) => Some((Ok(Bytes::from(chunk)), (iterator, true))),
                Ok(None) => None,
                Err(error) => Some((Err(invocation_error(error)), (iterator, false))),
            }
        });
        let expected = u64::try_from(
            end.checked_sub(start)
                .and_then(|length| length.checked_add(1))
                .ok_or_else(|| anyhow!("Telegram range is too large"))?,
        )
        .map_err(|_| anyhow!("Telegram range is too large"))?;
        let skip_within_chunk = u64::try_from(start - iterator_offset)
            .context("Telegram download offset is invalid")?;
        Ok(exact_range_stream(source, skip_within_chunk, expected))
    }

    pub async fn delete_message(&self, message_id: i64) -> Result<()> {
        let message_id = i32::try_from(message_id).context("Telegram message ID is too large")?;
        let deleted = self
            .client
            .delete_messages(self.peer, &[message_id])
            .await
            .map_err(invocation_error)?;
        if deleted == 0 {
            bail!("Telegram message not found");
        }
        Ok(())
    }
}

async fn resolve_storage_peer(client: &Client, config: &Config) -> Result<PeerRef> {
    let peer = if let Some(username) = &config.grammers_chat_username {
        client
            .resolve_username(username)
            .await
            .map_err(invocation_error)?
            .ok_or_else(|| anyhow!("grammers storage username was not found"))?
    } else if let Some(access_hash) = config.grammers_chat_access_hash {
        let peer_id = PeerId::from_bot_api_dialog_id(config.chat_id)
            .ok_or_else(|| anyhow!("invalid Telegram Bot API chat ID"))?;
        let channel_id = peer_id
            .bare_id()
            .ok_or_else(|| anyhow!("invalid Telegram channel ID"))?;
        let input = grammers_client::tl::types::InputPeerChannel {
            channel_id,
            access_hash,
        };
        client
            .resolve_peer(PeerRef::from(&input))
            .await
            .map_err(invocation_error)?
    } else {
        bail!("grammers storage username or access hash is required")
    };
    let Peer::Channel(channel) = peer else {
        bail!("grammers storage peer must be a channel or supergroup");
    };
    if channel.id().bot_api_dialog_id() != Some(config.chat_id) {
        bail!("grammers storage peer does not match TG2S3_CHAT_ID");
    }
    channel
        .to_ref()
        .await
        .map_err(|error| anyhow!("cache grammers storage peer: {error}"))?
        .ok_or_else(|| anyhow!("grammers storage peer has no usable reference"))
}

fn invocation_error(error: grammers_client::InvocationError) -> anyhow::Error {
    anyhow!("grammers Telegram request failed: {error}")
}

fn bot_id_from_token(token: &str) -> Result<i64> {
    token
        .split_once(':')
        .and_then(|(id, _)| id.parse().ok())
        .filter(|id: &i64| *id > 0)
        .ok_or_else(|| anyhow!("TG2S3_BOT_TOKEN must start with a numeric bot id"))
}
