use crate::config::Config;
use crate::db::Db;
use crate::model::{BlockRef, ObjectMetadata, ObjectRecord, PartRecord, UploadRecord};
use crate::telegram::TelegramClient;
use anyhow::{Result, anyhow, bail};
use axum::body::Body;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use md5::{Digest, Md5};
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
    upload_slots: Arc<Semaphore>,
}

#[derive(Clone, Debug)]
struct UploadedBlock {
    ordinal: i64,
    offset: i64,
    data_size: i64,
    message_id: i64,
    file_id: String,
    file_unique_id: String,
    message_date: i64,
}

impl Engine {
    pub fn new(config: Config, db: Db, telegram: TelegramClient) -> Self {
        let slots = Arc::new(Semaphore::new(config.upload_concurrency));
        Self {
            db,
            telegram,
            config: Arc::new(config),
            upload_slots: slots,
        }
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Body,
        metadata: ObjectMetadata,
        expected_length: Option<i64>,
    ) -> Result<ObjectRecord> {
        self.require_bucket(bucket).await?;
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
            let (part, actual_length) = self.upload_stream(&upload_id, 0, body).await?;
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
                .commit_upload(&upload_id, actual_length, &etag, &[part])
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
        let (part, size) = self.upload_stream(upload_id, part_number, body).await?;
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
            total_size += part.size;
            ordered.push(part.clone());
        }
        let etag = format!("{:x}-{}", composite.finalize(), requested.len());
        let (object, _) = self
            .db
            .commit_upload(upload_id, total_size, &etag, &ordered)
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
        self.db
            .get_object(bucket, key)
            .await?
            .ok_or_else(|| anyhow!("NoSuchKey"))
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool> {
        Ok(self.db.delete_object(bucket, key).await?.is_some())
    }

    pub async fn copy_object(
        &self,
        source_bucket: &str,
        source_key: &str,
        bucket: &str,
        key: &str,
        metadata: &ObjectMetadata,
    ) -> Result<ObjectRecord> {
        self.require_bucket(bucket).await?;
        let source = self.get_object(source_bucket, source_key).await?;
        Ok(self.db.copy_object(&source, bucket, key, metadata).await?.0)
    }

    pub fn range_stream(
        &self,
        object: &ObjectRecord,
        start: i64,
        end: i64,
    ) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
        let telegram = self.telegram.clone();
        let blocks = object.blocks.clone();
        let state = RangeState {
            telegram,
            blocks,
            index: 0,
            start,
            end,
        };
        stream::unfold(state, |mut state| async move {
            while state.index < state.blocks.len() {
                let block = state.blocks[state.index].clone();
                state.index += 1;
                let block_end = block.offset + block.size - 1;
                if block_end < state.start || block.offset > state.end {
                    continue;
                }
                let read_start = state.start.max(block.offset) - block.offset;
                let read_end = state.end.min(block_end) - block.offset;
                match state
                    .telegram
                    .download_chunk(&block.file_id, read_start, read_end)
                    .await
                {
                    Ok(bytes) => return Some((Ok(bytes), state)),
                    Err(error) => return Some((Err(std::io::Error::other(error)), state)),
                }
            }
            None
        })
    }

    pub async fn run_gc(&self, limit: usize) -> Result<usize> {
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
                continue;
            }
            match self.telegram.delete_message(candidate.message_id).await {
                Ok(()) => self.db.gc_success(candidate.block_id).await?,
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
    ) -> Result<(PartRecord, i64)> {
        let mut stream = body.into_data_stream();
        let mut tasks = futures_util::stream::FuturesUnordered::new();
        let mut buffer = Vec::with_capacity(self.config.chunk_size);
        let mut ordinal = 0_i64;
        let mut offset = 0_i64;
        let mut digest = Md5::new();
        let mut total = 0_i64;
        let mut read_error = None;
        let mut uploaded = Vec::new();

        loop {
            match stream.next().await {
                Some(Ok(data)) => {
                    digest.update(&data);
                    total += data.len() as i64;
                    buffer.extend_from_slice(&data);
                    while buffer.len() >= self.config.chunk_size {
                        let chunk =
                            Bytes::from(buffer.drain(..self.config.chunk_size).collect::<Vec<_>>());
                        let task = self.spawn_chunk(
                            upload_id.to_string(),
                            part_number,
                            ordinal,
                            offset,
                            chunk,
                        );
                        tasks.push(task);
                        ordinal += 1;
                        offset += self.config.chunk_size as i64;
                        if tasks.len() >= self.config.upload_concurrency
                            && let Some(result) = tasks.next().await
                        {
                            match result {
                                Ok(Ok(block)) => uploaded.push(block),
                                Ok(Err(error)) => {
                                    if read_error.is_none() {
                                        read_error = Some(error)
                                    }
                                }
                                Err(error) => {
                                    if read_error.is_none() {
                                        read_error = Some(anyhow!(error))
                                    }
                                }
                            }
                        }
                    }
                }
                Some(Err(error)) => {
                    read_error = Some(anyhow!(error));
                    break;
                }
                None => break,
            }
        }
        if !buffer.is_empty() {
            let size = buffer.len() as i64;
            let chunk = Bytes::from(buffer);
            tasks.push(self.spawn_chunk(
                upload_id.to_string(),
                part_number,
                ordinal,
                offset,
                chunk,
            ));
            offset += size;
        }
        let mut task_error = read_error;
        while let Some(result) = tasks.next().await {
            match result {
                Ok(Ok(block)) => uploaded.push(block),
                Ok(Err(error)) => {
                    if task_error.is_none() {
                        task_error = Some(error)
                    }
                }
                Err(error) => {
                    if task_error.is_none() {
                        task_error = Some(anyhow!(error))
                    }
                }
            }
        }
        if let Some(error) = task_error {
            return Err(error);
        }
        uploaded.sort_by_key(|block| block.ordinal);
        let mut refs = Vec::with_capacity(uploaded.len());
        for block in uploaded {
            let reference = BlockRef {
                id: 0,
                ordinal: block.ordinal,
                offset: block.offset,
                size: block.data_size,
                chat_id: self.config.chat_id,
                message_id: block.message_id,
                file_id: block.file_id,
                file_unique_id: block.file_unique_id,
                message_date: block.message_date,
            };
            let id = self.db.add_staged_block(&reference).await?;
            refs.push(BlockRef { id, ..reference });
        }
        let etag = format!("{:x}", digest.finalize());
        Ok((
            PartRecord {
                upload_id: upload_id.to_string(),
                part_number,
                size: offset,
                etag,
                blocks: refs,
            },
            total,
        ))
    }

    fn spawn_chunk(
        &self,
        upload_id: String,
        part_number: i32,
        ordinal: i64,
        offset: i64,
        data: Bytes,
    ) -> tokio::task::JoinHandle<Result<UploadedBlock>> {
        let telegram = self.telegram.clone();
        let slots = self.upload_slots.clone();
        let filename = format!("tg2s3-{}-{}-{}.part", upload_id, part_number, ordinal);
        tokio::spawn(async move {
            let _permit = slots
                .acquire_owned()
                .await
                .map_err(|_| anyhow!("upload semaphore closed"))?;
            let size = data.len() as i64;
            let document = telegram.upload_chunk(data, &filename).await?;
            if document.file_size != 0 && document.file_size != size {
                bail!(
                    "Telegram stored chunk with size {}, expected {}",
                    document.file_size,
                    size
                );
            }
            Ok(UploadedBlock {
                ordinal,
                offset,
                data_size: size,
                message_id: document.message_id,
                file_id: document.file_id,
                file_unique_id: document.file_unique_id,
                message_date: document.message_date,
            })
        })
    }

    async fn require_bucket(&self, bucket: &str) -> Result<()> {
        if !self.db.bucket_exists(bucket).await? {
            bail!("NoSuchBucket");
        }
        Ok(())
    }
}

struct RangeState {
    telegram: TelegramClient,
    blocks: Vec<BlockRef>,
    index: usize,
    start: i64,
    end: i64,
}

fn normalize_etag(value: &str) -> String {
    value.trim().trim_matches('"').to_string()
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
    use anyhow::Context;
    use axum::extract::State;
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[derive(Clone)]
    struct MockState {
        files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        next_id: Arc<std::sync::atomic::AtomicI64>,
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

    #[tokio::test]
    async fn puts_and_reads_ranges_through_telegram() -> Result<()> {
        let state = MockState {
            files: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        };
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
            telegram_api_url: format!("http://{address}"),
            local_bot_api: false,
            chunk_size: 4,
            upload_concurrency: 2,
            download_concurrency: 2,
            access_key: None,
            secret_key: None,
            allow_anonymous: true,
            region: "us-east-1".to_string(),
            public_host: None,
            init_buckets: Vec::new(),
            cors: crate::model::CorsConfiguration::default(),
            gc_interval: 300,
            gc_limit: 100,
        };
        let db = Db::open(&config.db_path).await?;
        db.create_bucket("bucket").await?;
        let telegram = TelegramClient::new(&config)?;
        let engine = Engine::new(config, db, telegram);
        let data = b"hello telegram storage";
        let object = engine
            .put_object(
                "bucket",
                "file",
                Body::from(data.to_vec()),
                ObjectMetadata::default(),
                Some(data.len() as i64),
            )
            .await?;
        assert_eq!(object.size, data.len() as i64);
        let mut stream = Box::pin(engine.range_stream(&object, 6, 13));
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
