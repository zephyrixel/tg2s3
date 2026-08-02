use crate::config::Config;
use crate::model::{BlockRef, TelegramBackend};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::time::sleep;

#[derive(Clone)]
pub struct BotApiClient {
    token: String,
    chat_id: i64,
    api_url: String,
    local_bot_api: bool,
    client: Client,
}

#[derive(Clone, Debug)]
pub struct UploadedDocument {
    pub message_id: i64,
    pub file_id: String,
    pub file_unique_id: String,
    pub file_size: i64,
    pub message_date: i64,
}

#[derive(Clone, Debug)]
pub struct ChatCheck {
    pub title: String,
    pub kind: String,
}

#[derive(Debug, Deserialize)]
struct ApiEnvelope<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
    parameters: Option<ApiParameters>,
}

#[derive(Debug, Deserialize)]
struct ApiParameters {
    retry_after: Option<u64>,
}

#[derive(Debug)]
struct TelegramFailure {
    status: StatusCode,
    description: String,
    retry_after: Option<Duration>,
}

fn retry_policy(error: &anyhow::Error, idempotent: bool) -> (bool, Option<Duration>) {
    if let Some(failure) = error.downcast_ref::<TelegramFailure>() {
        return (
            failure.status == StatusCode::TOO_MANY_REQUESTS
                || (idempotent
                    && (failure.status == StatusCode::REQUEST_TIMEOUT
                        || failure.status.is_server_error())),
            failure.retry_after,
        );
    }
    (
        idempotent && error.downcast_ref::<reqwest::Error>().is_some(),
        None,
    )
}

pub fn is_missing_message(error: &anyhow::Error) -> bool {
    let Some(failure) = error.downcast_ref::<TelegramFailure>() else {
        return false;
    };
    let description = failure.description.to_ascii_lowercase();
    failure.status == StatusCode::BAD_REQUEST
        && description.contains("message")
        && description.contains("not found")
}

impl std::fmt::Display for TelegramFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Telegram {}: {}", self.status, self.description)
    }
}

impl std::error::Error for TelegramFailure {}

#[derive(Debug, Deserialize)]
struct Chat {
    #[allow(dead_code)]
    id: i64,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    title: String,
    message_auto_delete_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ChatMember {
    status: String,
    can_post_messages: Option<bool>,
    can_delete_messages: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct Message {
    message_id: i64,
    date: i64,
    document: Option<Document>,
}

#[derive(Debug, Deserialize)]
struct Document {
    file_id: String,
    file_unique_id: String,
    file_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct FileInfo {
    file_path: Option<String>,
}

impl BotApiClient {
    pub fn new(config: &Config) -> Result<Self> {
        let client = Client::builder()
            .pool_max_idle_per_host(
                (config.upload_concurrency + config.download_concurrency).max(8),
            )
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(config.telegram_timeout_secs))
            .build()?;
        Ok(Self {
            token: config.bot_token.clone(),
            chat_id: config.chat_id,
            api_url: config.telegram_api_url.clone(),
            local_bot_api: config.local_bot_api,
            client,
        })
    }

    pub async fn verify(&self) -> Result<ChatCheck> {
        let chat: Chat = self
            .call_get("getChat", &[("chat_id", self.chat_id.to_string())])
            .await?;
        if chat.kind != "supergroup" && chat.kind != "channel" {
            return Err(anyhow!(
                "storage chat must be a supergroup or channel, got {}",
                chat.kind
            ));
        }
        let member: ChatMember = self
            .call_get(
                "getChatMember",
                &[
                    ("chat_id", self.chat_id.to_string()),
                    ("user_id", self.get_me_id().await?.to_string()),
                ],
            )
            .await?;
        if member.status != "administrator" && member.status != "creator" {
            return Err(anyhow!("bot must be an administrator of the storage chat"));
        }
        if chat.kind == "channel" && !has_admin_permission(&member, member.can_post_messages) {
            return Err(anyhow!(
                "bot needs can_post_messages in the storage channel"
            ));
        }
        if chat.kind == "supergroup" && !has_admin_permission(&member, member.can_delete_messages) {
            return Err(anyhow!(
                "bot needs can_delete_messages in the storage supergroup"
            ));
        }
        if !has_admin_permission(&member, member.can_delete_messages) {
            return Err(anyhow!(
                "bot needs can_delete_messages for garbage collection"
            ));
        }
        if chat.message_auto_delete_time.unwrap_or(0) > 0 {
            return Err(anyhow!(
                "storage chat has automatic message deletion enabled"
            ));
        }
        Ok(ChatCheck {
            title: chat.title,
            kind: chat.kind,
        })
    }

    async fn get_me_id(&self) -> Result<i64> {
        #[derive(Deserialize)]
        struct User {
            id: i64,
        }
        Ok(self.call_get::<User>("getMe", &[]).await?.id)
    }

    pub async fn upload_chunk(&self, data: Bytes, filename: &str) -> Result<UploadedDocument> {
        let api_url = self.method_url("sendDocument");
        self.retry("sendDocument", false, || async {
            let document = reqwest::multipart::Part::stream_with_length(
                reqwest::Body::from(data.clone()),
                data.len() as u64,
            )
            .file_name(filename.to_string());
            let form = reqwest::multipart::Form::new()
                .text("chat_id", self.chat_id.to_string())
                .text("disable_notification", "true")
                .part("document", document);
            let response = self.client.post(&api_url).multipart(form).send().await?;
            let message: Message = self.decode(response, "sendDocument").await?;
            let document = message
                .document
                .ok_or_else(|| anyhow!("Telegram sendDocument returned no document"))?;
            Ok(UploadedDocument {
                message_id: message.message_id,
                file_id: document.file_id,
                file_unique_id: document.file_unique_id,
                file_size: document.file_size.unwrap_or(data.len() as i64),
                message_date: message.date,
            })
        })
        .await
    }

    pub async fn download_chunk(&self, file_id: &str, start: i64, end: i64) -> Result<Bytes> {
        if start < 0 || end < start {
            return Err(anyhow!("invalid Telegram range"));
        }
        let info: FileInfo = self
            .call_get("getFile", &[("file_id", file_id.to_string())])
            .await?;
        let path = info
            .file_path
            .ok_or_else(|| anyhow!("Telegram getFile returned no file_path"))?;
        let expected = usize::try_from(
            end.checked_sub(start)
                .and_then(|length| length.checked_add(1))
                .ok_or_else(|| anyhow!("Telegram range is too large"))?,
        )
        .map_err(|_| anyhow!("Telegram range does not fit in memory"))?;
        if self.local_bot_api && Path::new(&path).is_absolute() {
            let mut file = tokio::fs::File::open(&path).await?;
            file.seek(std::io::SeekFrom::Start(start as u64)).await?;
            let mut data = vec![0_u8; expected];
            file.read_exact(&mut data).await?;
            return Ok(Bytes::from(data));
        }
        let url = format!(
            "{}/file/bot{}/{}",
            self.api_url,
            self.token,
            path.trim_start_matches('/')
        );
        self.retry("file download", true, || async {
            let response = self
                .client
                .get(&url)
                .header(reqwest::header::RANGE, format!("bytes={start}-{end}"))
                .send()
                .await?;
            let status = response.status();
            if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
                return Err(anyhow!(TelegramFailure {
                    status,
                    description: status.to_string(),
                    retry_after: None
                }));
            }
            let data = response.bytes().await?;
            if status == StatusCode::PARTIAL_CONTENT {
                if data.len() != expected {
                    return Err(anyhow!(
                        "Telegram returned {} bytes for {} byte range",
                        data.len(),
                        expected
                    ));
                }
                return Ok(data);
            }
            let end_index = start as usize + expected;
            if data.len() < end_index {
                return Err(anyhow!("Telegram returned a short file"));
            }
            Ok(data.slice(start as usize..end_index))
        })
        .await
    }

    pub async fn delete_message(&self, message_id: i64) -> Result<()> {
        let _: bool = self
            .call_post(
                "deleteMessage",
                &[
                    ("chat_id", self.chat_id.to_string()),
                    ("message_id", message_id.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    async fn call_get<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &[(&str, String)],
    ) -> Result<T> {
        let url = self.method_url(method);
        self.retry(method, true, || async {
            let mut request = self.client.get(&url);
            let query: Vec<(&str, &str)> = params
                .iter()
                .map(|(key, value)| (*key, value.as_str()))
                .collect();
            request = request.query(&query);
            let response = request.send().await?;
            self.decode(response, method).await
        })
        .await
    }

    async fn call_post<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &[(&str, String)],
    ) -> Result<T> {
        let url = self.method_url(method);
        self.retry(method, true, || async {
            let mut form: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
            for (key, value) in params {
                form.insert(*key, value.as_str());
            }
            let response = self.client.post(&url).form(&form).send().await?;
            self.decode(response, method).await
        })
        .await
    }

    async fn decode<T: DeserializeOwned>(
        &self,
        response: reqwest::Response,
        method: &str,
    ) -> Result<T> {
        let status = response.status();
        let body = response.bytes().await?;
        let envelope: ApiEnvelope<T> = match serde_json::from_slice(&body) {
            Ok(envelope) => envelope,
            Err(error) if !status.is_success() => {
                return Err(anyhow!(TelegramFailure {
                    status,
                    description: format!("invalid Telegram error response: {error}"),
                    retry_after: None,
                }));
            }
            Err(error) => return Err(anyhow!("Telegram {method} invalid response: {error}")),
        };
        if !status.is_success() || !envelope.ok {
            let failure = TelegramFailure {
                status,
                description: envelope
                    .description
                    .unwrap_or_else(|| "unknown Telegram error".to_string()),
                retry_after: envelope
                    .parameters
                    .and_then(|p| p.retry_after)
                    .map(Duration::from_secs),
            };
            return Err(anyhow!(failure));
        }
        envelope
            .result
            .ok_or_else(|| anyhow!("Telegram {method} returned no result"))
    }

    async fn retry<T, F, Fut>(&self, method: &str, idempotent: bool, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut delay = Duration::from_millis(500);
        let mut last_error = None;
        for attempt in 0..5 {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let (retryable, retry_after) = retry_policy(&error, idempotent);
                    let error = if error.downcast_ref::<reqwest::Error>().is_some() {
                        anyhow!("Telegram {method} request transport error")
                    } else {
                        error
                    };
                    if !retryable {
                        return Err(error);
                    }
                    if attempt == 4 {
                        last_error = Some(error);
                        break;
                    }
                    let wait = retry_after.unwrap_or(delay);
                    tracing::warn!(method, attempt = attempt + 1, ?wait, error = %error, "retrying Telegram request");
                    sleep(wait).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("Telegram {method} failed")))
    }

    fn method_url(&self, method: &str) -> String {
        format!("{}/bot{}/{}", self.api_url, self.token, method)
    }
}

fn has_admin_permission(member: &ChatMember, permission: Option<bool>) -> bool {
    member.status == "creator" || permission == Some(true)
}

impl From<UploadedDocument> for BlockRef {
    fn from(document: UploadedDocument) -> Self {
        Self {
            id: 0,
            ordinal: 0,
            offset: 0,
            size: document.file_size,
            chat_id: 0,
            message_id: document.message_id,
            backend: TelegramBackend::BotApi,
            document_id: None,
            file_id: document.file_id,
            file_unique_id: document.file_unique_id,
            message_date: document.message_date,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(status: StatusCode) -> anyhow::Error {
        anyhow::Error::new(TelegramFailure {
            status,
            description: "test".to_string(),
            retry_after: Some(Duration::from_secs(3)),
        })
    }

    #[test]
    fn retries_only_transient_telegram_failures() {
        assert_eq!(
            retry_policy(&failure(StatusCode::BAD_REQUEST), true),
            (false, Some(Duration::from_secs(3)))
        );
        assert_eq!(
            retry_policy(&failure(StatusCode::TOO_MANY_REQUESTS), false),
            (true, Some(Duration::from_secs(3)))
        );
        assert_eq!(
            retry_policy(&failure(StatusCode::INTERNAL_SERVER_ERROR), true),
            (true, Some(Duration::from_secs(3)))
        );
        assert_eq!(
            retry_policy(&failure(StatusCode::INTERNAL_SERVER_ERROR), false),
            (false, Some(Duration::from_secs(3)))
        );
    }

    #[test]
    fn creator_has_implicit_storage_permissions() {
        let creator = ChatMember {
            status: "creator".to_string(),
            can_post_messages: None,
            can_delete_messages: None,
        };
        assert!(has_admin_permission(&creator, None));

        let administrator = ChatMember {
            status: "administrator".to_string(),
            can_post_messages: None,
            can_delete_messages: None,
        };
        assert!(!has_admin_permission(&administrator, None));
        assert!(has_admin_permission(&administrator, Some(true)));
    }

    #[test]
    fn recognizes_already_deleted_messages() {
        let error = anyhow::Error::new(TelegramFailure {
            status: StatusCode::BAD_REQUEST,
            description: "Bad Request: message to delete not found".to_string(),
            retry_after: None,
        });
        assert!(is_missing_message(&error));
        assert!(!is_missing_message(&failure(StatusCode::BAD_REQUEST)));
    }

    #[tokio::test]
    async fn classifies_non_json_server_errors_as_retryable() -> Result<()> {
        use axum::response::IntoResponse;

        async fn gateway_error() -> impl IntoResponse {
            (StatusCode::BAD_GATEWAY, "upstream gateway error")
        }

        let app = axum::Router::new().route("/", axum::routing::get(gateway_error));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = BotApiClient {
            token: String::new(),
            chat_id: 0,
            api_url: String::new(),
            local_bot_api: false,
            client: reqwest::Client::new(),
        };
        let response = client
            .client
            .get(format!("http://{address}/"))
            .send()
            .await?;
        let error = client
            .decode::<serde_json::Value>(response, "test")
            .await
            .expect_err("gateway error should fail");
        assert!(retry_policy(&error, true).0);
        assert!(!retry_policy(&error, false).0);
        Ok(())
    }
}
