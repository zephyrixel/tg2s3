use crate::config::Config;
use crate::db::Db;
use crate::limits::{AdmissionPermit, TransferLimits, check_size};
use crate::model::{
    ObjectCondition, ObjectMetadata, ObjectRecord, PartRecord, UploadRecord, normalize_etag,
};
use crate::telegram::{TelegramClient, is_missing_message};
use anyhow::{Result, anyhow, bail};
use axum::body::Body;
use bytes::Bytes;
use futures_util::Stream;
use md5::{Digest, Md5};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use uuid::Uuid;

pub const MIN_MULTIPART_PART: i64 = 5 * 1024 * 1024;
pub const MAX_MULTIPART_PARTS: usize = 10_000;

#[derive(Clone)]
pub struct Engine {
    pub db: Db,
    pub telegram: TelegramClient,
    pub config: Arc<Config>,
    pub limits: Arc<TransferLimits>,
    upload_slots: Arc<Semaphore>,
    download_slots: Arc<Semaphore>,
}

impl Engine {
    pub fn new(config: Config, db: Db, telegram: TelegramClient) -> Self {
        let limits = Arc::new(TransferLimits::new(&config));
        let slots = Arc::new(Semaphore::new(config.upload_concurrency));
        let download_slots = Arc::new(Semaphore::new(config.download_concurrency));
        Self {
            db,
            telegram,
            config: Arc::new(config),
            limits,
            upload_slots: slots,
            download_slots,
        }
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Body,
        metadata: ObjectMetadata,
        expected_length: Option<i64>,
        condition: ObjectCondition,
    ) -> Result<ObjectRecord> {
        self.require_bucket(bucket).await?;
        check_size(expected_length, self.config.max_object_size)?;
        let upload_id = format!("put-{}", Uuid::new_v4());
        self.db
            .create_upload(&UploadRecord {
                upload_id: upload_id.clone(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                metadata,
                kind: "put".to_string(),
                created_at: now(),
            })
            .await?;
        let result = async {
            let (part, actual_length) = self
                .upload_stream(&upload_id, 0, body, expected_length)
                .await?;
            if let Some(expected) = expected_length
                && expected != actual_length
            {
                bail!(
                    "request Content-Length {expected} does not match body length {actual_length}"
                );
            }
            self.db
                .replace_part(&upload_id, 0, actual_length, &part.etag, &part.blocks)
                .await?;
            let etag = part.etag.clone();
            let committed = self
                .db
                .commit_upload(&upload_id, actual_length, &etag, &[part], &condition)
                .await?
                .ok_or_else(|| anyhow!("upload disappeared"))?;
            Ok(committed.0)
        }
        .await;
        if result.is_err() {
            let _ = self.db.abort_upload(&upload_id).await;
        }
        result
    }

    pub async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        metadata: ObjectMetadata,
    ) -> Result<String> {
        self.require_bucket(bucket).await?;
        let upload_id = Uuid::new_v4().to_string();
        self.db
            .create_upload(&UploadRecord {
                upload_id: upload_id.clone(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                metadata,
                kind: "multipart".to_string(),
                created_at: now(),
            })
            .await?;
        Ok(upload_id)
    }

    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        body: Body,
        expected_length: Option<i64>,
    ) -> Result<PartRecord> {
        if !(1..=10_000).contains(&part_number) {
            bail!("part number must be between 1 and 10,000");
        }
        let upload = self
            .db
            .get_upload(upload_id)
            .await?
            .ok_or_else(|| anyhow!("NoSuchUpload"))?;
        if upload.kind != "multipart" {
            bail!("NoSuchUpload");
        }
        check_size(expected_length, self.config.max_object_size)?;
        let (part, size) = self
            .upload_stream(upload_id, part_number, body, expected_length)
            .await?;
        self.db
            .replace_part(upload_id, part_number, size, &part.etag, &part.blocks)
            .await?;
        Ok(PartRecord {
            upload_id: upload_id.to_string(),
            part_number,
            size,
            etag: part.etag,
            blocks: part.blocks,
        })
    }

    pub async fn complete_multipart(
        &self,
        upload_id: &str,
        requested: &[(i32, String)],
    ) -> Result<ObjectRecord> {
        let upload = self
            .db
            .get_upload(upload_id)
            .await?
            .ok_or_else(|| anyhow!("NoSuchUpload"))?;
        if upload.kind != "multipart" {
            bail!("NoSuchUpload");
        }
        if requested.is_empty() || requested.len() > MAX_MULTIPART_PARTS {
            bail!("InvalidPart");
        }
        for window in requested.windows(2) {
            if window[0].0 >= window[1].0 {
                bail!("InvalidPartOrder");
            }
        }
        let stored = self.db.get_parts(upload_id, false).await?;
        if stored.len() != requested.len() {
            bail!("InvalidPart");
        }
        let mut ordered = Vec::with_capacity(requested.len());
        let mut total_size = 0_i64;
        let mut composite = Md5::new();
        for (index, (part_number, requested_etag)) in requested.iter().enumerate() {
            let part = stored.get(index).ok_or_else(|| anyhow!("InvalidPart"))?;
            if part.part_number != *part_number
                || normalize_etag(&part.etag) != normalize_etag(requested_etag)
            {
                bail!("InvalidPart");
            }
            if index + 1 != requested.len() && part.size < MIN_MULTIPART_PART {
                bail!("EntityTooSmall");
            }
            let digest =
                hex::decode(normalize_etag(&part.etag)).map_err(|_| anyhow!("InvalidPart"))?;
            if digest.len() != 16 {
                bail!("InvalidPart");
            }
            composite.update(digest);
            total_size = total_size
                .checked_add(part.size)
                .ok_or_else(|| anyhow!("multipart object is too large"))?;
            if total_size > self.config.max_object_size {
                bail!("EntityTooLarge");
            }
            ordered.push(part.clone());
        }
        let etag = format!("{:x}-{}", composite.finalize(), requested.len());
        let (object, _) = self
            .db
            .commit_upload(
                upload_id,
                total_size,
                &etag,
                &ordered,
                &ObjectCondition::default(),
            )
            .await?
            .ok_or_else(|| anyhow!("NoSuchUpload"))?;
        Ok(object)
    }

    pub async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartRecord>> {
        if self.db.get_upload(upload_id).await?.is_none() {
            bail!("NoSuchUpload");
        }
        self.db.get_parts(upload_id, false).await
    }

    pub async fn list_uploads(&self, bucket: &str, key: Option<&str>) -> Result<Vec<UploadRecord>> {
        self.require_bucket(bucket).await?;
        self.db.list_uploads(bucket, key).await
    }

    pub async fn abort_multipart(&self, upload_id: &str) -> Result<bool> {
        self.db.abort_upload(upload_id).await
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectRecord> {
        self.require_bucket(bucket).await?;
        let object = self
            .db
            .get_object(bucket, key)
            .await?
            .ok_or_else(|| anyhow!("NoSuchKey"))?;
        validate_object_layout(&object)?;
        Ok(object)
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        condition: &ObjectCondition,
    ) -> Result<bool> {
        self.require_bucket(bucket).await?;
        Ok(self
            .db
            .delete_object(bucket, key, condition)
            .await?
            .is_some())
    }

    pub async fn copy_object(
        &self,
        source_bucket: &str,
        source_key: &str,
        bucket: &str,
        key: &str,
        metadata: &ObjectMetadata,
        condition: &ObjectCondition,
    ) -> Result<ObjectRecord> {
        self.require_bucket(bucket).await?;
        let source = self.get_object(source_bucket, source_key).await?;
        Ok(self
            .db
            .copy_object(&source, bucket, key, metadata, condition)
            .await?
            .0)
    }

    pub fn range_stream(
        &self,
        object: &ObjectRecord,
        start: i64,
        end: i64,
        admission_permit: Option<AdmissionPermit>,
    ) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
        crate::transfer::range_stream(
            self.telegram.clone(),
            self.download_slots.clone(),
            object.blocks.clone(),
            start,
            end,
            admission_permit,
            self.limits.clone(),
        )
    }

    pub async fn run_gc(&self, limit: usize) -> Result<usize> {
        if limit == 0 || limit > crate::config::MAX_GC_LIMIT {
            bail!(
                "GC limit must be between 1 and {}",
                crate::config::MAX_GC_LIMIT
            );
        }
        let timestamp = now();
        let _ = self.db.expire_uploads(timestamp - 7 * 24 * 3600).await?;
        for stale in self.db.stale_blocks(timestamp - 3600).await? {
            self.db.delete_stale_block(stale.block_id).await?;
        }
        let candidates = self.db.gc_candidates(timestamp, limit).await?;
        let mut processed = 0;
        for candidate in candidates {
            if candidate.message_date > 0 && candidate.message_date < timestamp - 48 * 3600 {
                self.db
                    .gc_orphan(candidate.block_id, "Telegram deleteMessage 48 hour limit")
                    .await?;
                processed += 1;
                continue;
            }
            let block = candidate.as_block_ref();
            match self.telegram.delete_message(&block).await {
                Ok(()) => self.db.gc_success(candidate.block_id).await?,
                Err(error) if is_missing_message(&error) => {
                    self.db.gc_success(candidate.block_id).await?;
                }
                Err(error) => {
                    self.db
                        .gc_failure(candidate.block_id, &error.to_string(), timestamp + 300)
                        .await?
                }
            }
            processed += 1;
        }
        Ok(processed)
    }

    async fn upload_stream(
        &self,
        upload_id: &str,
        part_number: i32,
        body: Body,
        expected_length: Option<i64>,
    ) -> Result<(PartRecord, i64)> {
        let context = crate::transfer::UploadContext::new(
            &self.db,
            &self.telegram,
            self.config.clone(),
            self.limits.clone(),
            self.upload_slots.clone(),
        );
        crate::transfer::upload(context, upload_id, part_number, body, expected_length).await
    }

    async fn require_bucket(&self, bucket: &str) -> Result<()> {
        if !self.db.bucket_exists(bucket).await? {
            bail!("NoSuchBucket");
        }
        Ok(())
    }
}

fn validate_object_layout(object: &ObjectRecord) -> Result<()> {
    if object.size < 0 {
        bail!("object storage layout is invalid");
    }
    let mut offset = 0_i64;
    let mut block_ids = HashSet::new();
    for block in &object.blocks {
        if block.size <= 0 || block.offset != offset || !block_ids.insert(block.id) {
            bail!("object storage layout is invalid");
        }
        offset = offset
            .checked_add(block.size)
            .ok_or_else(|| anyhow!("object storage layout is invalid"))?;
    }
    if offset != object.size {
        bail!("object storage layout is invalid");
    }
    Ok(())
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::db::Db;
    use crate::model::{BlockRef, TelegramBackend};
    use anyhow::Context;
    use axum::extract::State;
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use futures_util::StreamExt;
    use futures_util::stream;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct MockState {
        files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        next_id: Arc<std::sync::atomic::AtomicI64>,
        upload_started: Arc<Notify>,
    }

    async fn mock_telegram(State(state): State<MockState>, request: Request<Body>) -> Response {
        let path = request.uri().path().to_string();
        if path.ends_with("/getMe") {
            return json_response(r#"{"ok":true,"result":{"id":1}}"#);
        }
        if path.ends_with("/getChat") {
            return json_response(
                r#"{"ok":true,"result":{"id":-100,"type":"supergroup","title":"test","message_auto_delete_time":0}}"#,
            );
        }
        if path.ends_with("/getChatMember") {
            return json_response(
                r#"{"ok":true,"result":{"status":"administrator","can_delete_messages":true,"can_post_messages":true}}"#,
            );
        }
        if path.ends_with("/sendDocument") {
            state.upload_started.notify_one();
            let content_type = request
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let bytes = axum::body::to_bytes(request.into_body(), 32 * 1024 * 1024)
                .await
                .unwrap();
            let data = multipart_document(&bytes, &content_type);
            let id = state
                .next_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let file_id = format!("file-{id}");
            state
                .files
                .lock()
                .unwrap()
                .insert(file_id.clone(), data.clone());
            return json_response(&format!(
                r#"{{"ok":true,"result":{{"message_id":{id},"date":{},"document":{{"file_id":"{}","file_unique_id":"unique-{}","file_size":{}}}}}}}"#,
                now(),
                file_id,
                id,
                data.len()
            ));
        }
        if path.ends_with("/getFile") {
            let query: HashMap<String, String> =
                url::form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes())
                    .into_owned()
                    .collect();
            let file_id = query.get("file_id").cloned().unwrap_or_default();
            return json_response(&format!(
                r#"{{"ok":true,"result":{{"file_path":"file/{}"}}}}"#,
                file_id
            ));
        }
        if path.ends_with("/deleteMessage") {
            return json_response(r#"{"ok":true,"result":true}"#);
        }
        if path.contains("/file/bot") {
            let file_id = path.rsplit('/').next().unwrap_or_default();
            let Some(data) = state.files.lock().unwrap().get(file_id).cloned() else {
                return status_response(StatusCode::NOT_FOUND);
            };
            let (start, end) = request
                .headers()
                .get("range")
                .and_then(|value| value.to_str().ok())
                .and_then(parse_test_range)
                .unwrap_or((0, data.len().saturating_sub(1)));
            let end = end.min(data.len().saturating_sub(1));
            let bytes = if data.is_empty() {
                Vec::new()
            } else {
                data[start.min(data.len())..=end].to_vec()
            };
            let mut response = Response::new(Body::from(bytes));
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            return response;
        }
        status_response(StatusCode::NOT_FOUND)
    }

    fn json_response(body: &str) -> Response {
        Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }
    fn status_response(status: StatusCode) -> Response {
        Response::builder()
            .status(status)
            .body(Body::empty())
            .unwrap()
    }
    fn parse_test_range(value: &str) -> Option<(usize, usize)> {
        let value = value.strip_prefix("bytes=")?;
        let (start, end) = value.split_once('-')?;
        Some((start.parse().ok()?, end.parse().ok()?))
    }
    fn multipart_document(body: &[u8], content_type: &str) -> Vec<u8> {
        let boundary = content_type.split("boundary=").nth(1).unwrap_or_default();
        let marker = br#"name="document""#;
        let header_start = body
            .windows(marker.len())
            .position(|window| window == marker)
            .unwrap();
        let data_start = body[header_start..]
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + header_start
            + 4;
        let end_marker = format!("\r\n--{boundary}");
        let data_end = body[data_start..]
            .windows(end_marker.len())
            .position(|window| window == end_marker.as_bytes())
            .unwrap()
            + data_start;
        body[data_start..data_end].to_vec()
    }

    #[test]
    fn validates_object_block_layout() {
        let object = ObjectRecord {
            id: 1,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            size: 3,
            etag: "etag".to_string(),
            metadata: ObjectMetadata::default(),
            created_at: 0,
            modified_at: 0,
            blocks: vec![BlockRef {
                id: 1,
                ordinal: 0,
                offset: 0,
                size: 3,
                chat_id: -100,
                message_id: 1,
                backend: TelegramBackend::BotApi,
                document_id: None,
                file_id: "file".to_string(),
                file_unique_id: "unique".to_string(),
                message_date: 1,
            }],
        };
        assert!(validate_object_layout(&object).is_ok());
        let mut duplicate = object.clone();
        duplicate.size = 6;
        duplicate.blocks.push(BlockRef {
            offset: 3,
            ..duplicate.blocks[0].clone()
        });
        assert!(validate_object_layout(&duplicate).is_err());

        let mut invalid = object;
        invalid.blocks[0].offset = 1;
        assert!(validate_object_layout(&invalid).is_err());
    }

    #[tokio::test]
    async fn puts_and_reads_ranges_through_telegram() -> Result<()> {
        let state = MockState {
            files: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
            upload_started: Arc::new(Notify::new()),
        };
        let upload_started = state.upload_started.clone();
        let app = axum::Router::new()
            .fallback(mock_telegram)
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind Telegram mock listener")?;
        let address: SocketAddr = listener
            .local_addr()
            .context("read Telegram mock listener address")?;
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let directory = tempdir()?;
        let config = Config {
            data_dir: directory.path().to_path_buf(),
            db_path: directory.path().join("db.sqlite3"),
            listen: "127.0.0.1:0".parse().unwrap(),
            bot_token: "token".to_string(),
            chat_id: -100,
            telegram_backend: TelegramBackend::BotApi,
            telegram_api_url: format!("http://{address}"),
            local_bot_api: false,
            chunk_size: 8,
            upload_concurrency: 2,
            download_concurrency: 2,
            telegram_timeout_secs: 300,
            grammers_api_id: None,
            grammers_api_hash: None,
            grammers_session_path: directory.path().join("grammers.session.sqlite3"),
            grammers_chat_username: None,
            grammers_chat_access_hash: None,
            grammers_max_flood_wait_secs: 30,
            access_key: None,
            secret_key: None,
            allow_anonymous: true,
            region: "us-east-1".to_string(),
            public_host: None,
            init_buckets: Vec::new(),
            cors: crate::model::CorsConfiguration::default(),
            gc_interval: 300,
            gc_limit: 100,
            max_object_size: crate::config::DEFAULT_MAX_OBJECT_SIZE,
            max_active_transfers: 16,
            limit_wait_secs: 5,
            upload_rate_bps: 0,
            download_rate_bps: 0,
        };
        let db = Db::open(&config.db_path).await?;
        db.create_bucket("bucket").await?;
        let telegram = TelegramClient::new(&config)?;
        let engine = Engine::new(config, db, telegram);

        let source_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release_source = Arc::new(Notify::new());
        let source_done_for_stream = source_done.clone();
        let release_for_stream = release_source.clone();
        let body_stream = stream::unfold(0_u8, move |state| {
            let source_done = source_done_for_stream.clone();
            let release_source = release_for_stream.clone();
            async move {
                match state {
                    0 => Some((
                        Ok::<_, std::convert::Infallible>(Bytes::from_static(b"0123")),
                        1,
                    )),
                    1 => {
                        release_source.notified().await;
                        source_done.store(true, std::sync::atomic::Ordering::SeqCst);
                        Some((Ok(Bytes::from_static(b"4567")), 2))
                    }
                    _ => None,
                }
            }
        });
        let streaming_upload = {
            let engine = engine.clone();
            tokio::spawn(async move {
                engine
                    .put_object(
                        "bucket",
                        "streamed",
                        Body::from_stream(body_stream),
                        ObjectMetadata::default(),
                        Some(8),
                        ObjectCondition::default(),
                    )
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), upload_started.notified())
            .await
            .context("Telegram upload did not start before request EOF")?;
        assert!(!source_done.load(std::sync::atomic::Ordering::SeqCst));
        release_source.notify_one();
        streaming_upload.await??;

        let spooled = engine
            .put_object(
                "bucket",
                "spooled",
                Body::from_stream(stream::iter(vec![Ok::<_, std::convert::Infallible>(
                    Bytes::from_static(b"unknown length body"),
                )])),
                ObjectMetadata::default(),
                None,
                ObjectCondition::default(),
            )
            .await?;
        assert_eq!(spooled.size, 19);
        let mut spools = tokio::fs::read_dir(engine.config.data_dir.join("upload-spool")).await?;
        assert!(spools.next_entry().await?.is_none());

        let data = b"hello telegram storage";
        let object = engine
            .put_object(
                "bucket",
                "file",
                Body::from(data.to_vec()),
                ObjectMetadata::default(),
                Some(data.len() as i64),
                ObjectCondition::default(),
            )
            .await?;
        assert_eq!(object.size, data.len() as i64);
        let mut stream = Box::pin(engine.range_stream(&object, 6, 13, None));
        let mut result = Vec::new();
        while let Some(chunk) = stream.next().await {
            result.extend_from_slice(&chunk?);
        }
        assert_eq!(result, b"telegram");

        let s3_app = crate::s3::router(crate::s3::AppState {
            engine: engine.clone(),
            auth: crate::auth::SigV4 {
                access_key: None,
                secret_key: None,
                region: "us-east-1".to_string(),
                allow_anonymous: true,
            },
            public_host: None,
            cors: crate::model::CorsConfiguration::default(),
        });
        let s3_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind S3 test listener")?;
        let s3_address = s3_listener
            .local_addr()
            .context("read S3 test listener address")?;
        tokio::spawn(async move {
            axum::serve(s3_listener, s3_app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let endpoint = format!("http://{s3_address}/bucket/http-key");
        let preflight = client
            .request(reqwest::Method::OPTIONS, &endpoint)
            .header("origin", "https://cloudreve.example")
            .header("access-control-request-method", "PUT")
            .header("access-control-request-headers", "content-type")
            .send()
            .await?;
        assert_eq!(preflight.status(), reqwest::StatusCode::NO_CONTENT);
        assert_eq!(
            preflight
                .headers()
                .get("access-control-allow-origin")
                .and_then(|value| value.to_str().ok()),
            Some("*")
        );
        assert!(
            preflight
                .headers()
                .get("access-control-allow-methods")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .contains("PUT")
        );
        let response = client.put(&endpoint).body("abcdef").send().await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let response = client
            .put(&endpoint)
            .header("if-match", "\"stale-etag\"")
            .body("replacement")
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::PRECONDITION_FAILED);
        let response = client.get(&endpoint).send().await?;
        assert_eq!(response.bytes().await?, Bytes::from_static(b"abcdef"));
        let response = client
            .delete(&endpoint)
            .header("if-match", "\"stale-etag\"")
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::PRECONDITION_FAILED);
        let response = client
            .get(&endpoint)
            .header("range", "bytes=1-3")
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.bytes().await?, Bytes::from_static(b"bcd"));
        let response = client
            .put(format!("http://{s3_address}/bucket/copied"))
            .header("x-amz-copy-source", "/bucket/http-key")
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        for key in ["aaa/a", "aaa/b"] {
            let response = client
                .put(format!("http://{s3_address}/bucket/{key}"))
                .body("x")
                .send()
                .await?;
            assert_eq!(response.status(), reqwest::StatusCode::OK);
        }
        let first_page = client
            .get(format!(
                "http://{s3_address}/bucket?list-type=2&delimiter=%2F&max-keys=1"
            ))
            .send()
            .await?
            .text()
            .await?;
        assert!(first_page.contains("<Prefix>aaa/</Prefix>"));
        assert!(first_page.contains("<IsTruncated>true</IsTruncated>"));
        let continuation = first_page
            .split("<NextContinuationToken>")
            .nth(1)
            .and_then(|value| value.split("</NextContinuationToken>").next())
            .ok_or_else(|| anyhow!("missing continuation token"))?;
        let second_page = client
            .get(format!(
                "http://{s3_address}/bucket?list-type=2&delimiter=%2F&max-keys=1&continuation-token={continuation}"
            ))
            .send()
            .await?
            .text()
            .await?;
        assert!(!second_page.contains("<Prefix>aaa/</Prefix>"));
        assert!(second_page.contains("<Key>copied</Key>"));

        let quiet_delete = client
            .post(format!("http://{s3_address}/bucket?delete"))
            .body("<Delete><Quiet>true</Quiet><Object><Key>aaa/a</Key></Object></Delete>")
            .send()
            .await?;
        assert_eq!(quiet_delete.status(), reqwest::StatusCode::OK);
        assert!(!quiet_delete.text().await?.contains("<Deleted>"));
        let missing_bucket_delete = client
            .post(format!("http://{s3_address}/missing?delete"))
            .body("<Delete><Object><Key>key</Key></Object></Delete>")
            .send()
            .await?;
        assert_eq!(
            missing_bucket_delete.status(),
            reqwest::StatusCode::NOT_FOUND
        );

        let cors_xml = r#"
            <CORSConfiguration>
              <CORSRule>
                <AllowedOrigin>https://cloudreve.example</AllowedOrigin>
                <AllowedMethod>GET</AllowedMethod>
                <AllowedHeader>*</AllowedHeader>
                <ExposeHeader>ETag</ExposeHeader>
                <MaxAgeSeconds>600</MaxAgeSeconds>
              </CORSRule>
            </CORSConfiguration>
        "#;
        let response = client
            .put(format!("http://{s3_address}/bucket?cors"))
            .body(cors_xml)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let response = client
            .get(format!("http://{s3_address}/bucket?cors"))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert!(response.text().await?.contains("cloudreve.example"));
        let response = client
            .delete(format!("http://{s3_address}/bucket?cors"))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
        let listing = client
            .get(format!("http://{s3_address}/bucket?list-type=2"))
            .send()
            .await?
            .text()
            .await?;
        assert!(listing.contains("http-key"));
        assert!(listing.contains("copied"));
        assert_eq!(
            client.delete(&endpoint).send().await?.status(),
            reqwest::StatusCode::NO_CONTENT
        );

        let multipart_base = format!("http://{s3_address}/bucket/multipart");
        let initiate = client
            .post(format!("{multipart_base}?uploads"))
            .send()
            .await?;
        assert_eq!(initiate.status(), reqwest::StatusCode::OK);
        let initiate_xml = initiate.text().await?;
        let upload_id = initiate_xml
            .split("<UploadId>")
            .nth(1)
            .and_then(|value| value.split("</UploadId>").next())
            .ok_or_else(|| anyhow!("missing multipart upload id"))?;
        let part_response = client
            .put(format!(
                "{multipart_base}?partNumber=1&uploadId={upload_id}"
            ))
            .body("part-data")
            .send()
            .await?;
        assert_eq!(part_response.status(), reqwest::StatusCode::OK);
        let part_etag = part_response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| anyhow!("missing part etag"))?;
        let complete_xml = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>"
        );
        let complete = client
            .post(format!("{multipart_base}?uploadId={upload_id}"))
            .header("content-type", "application/xml")
            .body(complete_xml)
            .send()
            .await?;
        assert_eq!(complete.status(), reqwest::StatusCode::OK);
        assert_eq!(
            client.get(&multipart_base).send().await?.bytes().await?,
            Bytes::from_static(b"part-data")
        );
        Ok(())
    }
}
