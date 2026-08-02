use crate::config::Config;
use crate::db::Db;
use crate::limits::{TransferDirection, TransferLimits};
use crate::model::{BlockRef, PartRecord, TelegramBackend};
use crate::telegram::{StoredDocument, TelegramClient, UploadReader};
use anyhow::{Result, anyhow, bail};
use axum::body::Body;
use futures_util::{StreamExt, stream::FuturesUnordered};
use md5::{Digest, Md5};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::STREAM_BUFFER_SIZE;
use super::cleanup::{cleanup_uploaded, references_for_uploaded};

#[derive(Clone, Debug)]
pub(super) struct UploadedBlock {
    pub(super) id: i64,
    pub(super) ordinal: i64,
    pub(super) offset: i64,
    pub(super) data_size: i64,
    pub(super) message_id: i64,
    pub(super) backend: TelegramBackend,
    pub(super) document_id: Option<i64>,
    pub(super) file_id: String,
    pub(super) file_unique_id: String,
    pub(super) message_date: i64,
}

type UploadTask = JoinHandle<Result<UploadedBlock>>;

struct UploadTasks {
    inner: FuturesUnordered<UploadTask>,
}

impl UploadTasks {
    fn new() -> Self {
        Self {
            inner: FuturesUnordered::new(),
        }
    }

    fn push(&mut self, task: UploadTask) {
        self.inner.push(task);
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    async fn next(
        &mut self,
    ) -> Option<std::result::Result<Result<UploadedBlock>, tokio::task::JoinError>> {
        self.inner.next().await
    }
}

impl Drop for UploadTasks {
    fn drop(&mut self) {
        for task in self.inner.iter_mut() {
            task.abort();
        }
    }
}

#[derive(Clone)]
pub(crate) struct UploadContext {
    db: Db,
    telegram: TelegramClient,
    config: Arc<Config>,
    limits: Arc<TransferLimits>,
    upload_slots: Arc<Semaphore>,
}

impl UploadContext {
    pub(crate) fn new(
        db: &Db,
        telegram: &TelegramClient,
        config: Arc<Config>,
        limits: Arc<TransferLimits>,
        upload_slots: Arc<Semaphore>,
    ) -> Self {
        Self {
            db: db.clone(),
            telegram: telegram.clone(),
            config,
            limits,
            upload_slots,
        }
    }
}

#[derive(Clone)]
struct BlockSpec {
    upload_id: String,
    part_number: i32,
    ordinal: i64,
    offset: i64,
    size: u64,
}

pub(crate) async fn cleanup_stale_spools(data_dir: &std::path::Path) -> Result<usize> {
    let directory = data_dir.join("upload-spool");
    let mut entries = match fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(24 * 60 * 60))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    while let Some(entry) = entries.next_entry().await? {
        let metadata = entry.metadata().await?;
        if !metadata.is_file() || metadata.modified().unwrap_or(SystemTime::now()) > cutoff {
            continue;
        }
        match fs::remove_file(entry.path()).await {
            Ok(()) => removed += 1,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(path = %entry.path().display(), error = %error, "failed to remove stale upload spool");
            }
        }
    }
    Ok(removed)
}

pub(crate) async fn upload(
    context: UploadContext,
    upload_id: &str,
    part_number: i32,
    body: Body,
    expected_length: Option<i64>,
) -> Result<(PartRecord, i64)> {
    let result = match expected_length {
        Some(expected_length) => {
            upload_known_length(&context, upload_id, part_number, body, expected_length).await
        }
        None => upload_unknown_length(&context, upload_id, part_number, body).await,
    }?;

    let (mut uploaded, total, digest) = result;
    uploaded.sort_by_key(|block| block.ordinal);
    let refs = references_for_uploaded(context.config.chat_id, &uploaded);
    let etag = format!("{:x}", digest.finalize());
    Ok((
        PartRecord {
            upload_id: upload_id.to_string(),
            part_number,
            size: total,
            etag,
            blocks: refs,
        },
        total,
    ))
}

async fn upload_known_length(
    context: &UploadContext,
    upload_id: &str,
    part_number: i32,
    body: Body,
    expected_length: i64,
) -> Result<(Vec<UploadedBlock>, i64, Md5)> {
    if expected_length < 0 {
        bail!("InvalidRequest");
    }
    let mut source = body.into_data_stream();
    let mut tasks = UploadTasks::new();
    let mut uploaded = Vec::new();
    let mut current = None;
    let mut digest = Md5::new();
    let mut total = 0_i64;
    let mut ordinal = 0_i64;
    let mut offset = 0_i64;
    let mut failure = None;

    'source: while let Some(frame) = source.next().await {
        let data = match frame {
            Ok(data) => data,
            Err(error) => {
                failure = Some(anyhow!(error));
                break;
            }
        };
        let next_total = match total.checked_add(data.len() as i64) {
            Some(total) => total,
            None => {
                failure = Some(anyhow!("EntityTooLarge"));
                break;
            }
        };
        if next_total > context.config.max_object_size {
            failure = Some(anyhow!("EntityTooLarge"));
            break;
        }
        if next_total > expected_length {
            failure = Some(anyhow!(
                "request Content-Length {expected_length} does not match body length {next_total}"
            ));
            break;
        }
        if let Err(error) = context
            .limits
            .bandwidth
            .throttle(TransferDirection::Upload, data.len(), true)
            .await
        {
            failure = Some(error);
            break;
        }
        digest.update(&data);
        total = next_total;

        let mut position = 0_usize;
        while position < data.len() {
            if current.is_none() {
                let block_size =
                    match block_size(context.config.chunk_size, offset, expected_length) {
                        Ok(size) => size,
                        Err(error) => {
                            failure = Some(error);
                            break 'source;
                        }
                    };
                current = Some(start_pipe_task(
                    context,
                    BlockSpec {
                        upload_id: upload_id.to_string(),
                        part_number,
                        ordinal,
                        offset,
                        size: block_size,
                    },
                ));
                ordinal += 1;
            }
            let (amount, block_size, complete) = {
                let block = current.as_mut().expect("current upload block exists");
                let remaining = block.size - block.written;
                let amount = remaining.min((data.len() - position) as u64) as usize;
                if let Err(error) = block
                    .writer
                    .write_all(&data[position..position + amount])
                    .await
                {
                    failure = Some(error.into());
                    break 'source;
                }
                block.written += amount as u64;
                (amount, block.size, block.written == block.size)
            };
            position += amount;
            if complete {
                let finished = current.take().expect("current upload block exists");
                if let Err(error) = finish_pipe_task(finished, &mut tasks).await {
                    failure = Some(error);
                    break 'source;
                }
                if tasks.len() >= context.config.upload_concurrency {
                    if let Err(error) = reap_one(&mut tasks, &mut uploaded).await {
                        failure = Some(error);
                        break 'source;
                    }
                }
                offset = match offset.checked_add(block_size as i64) {
                    Some(offset) => offset,
                    None => {
                        failure = Some(anyhow!("EntityTooLarge"));
                        break 'source;
                    }
                };
            }
        }
    }

    if let Some(block) = current.take() {
        if let Err(error) = finish_pipe_task(block, &mut tasks).await {
            failure.get_or_insert(error);
        }
    }
    drain_tasks(&mut tasks, &mut uploaded, &mut failure).await;
    if let Some(error) = failure {
        cleanup_uploaded(
            &context.db,
            &context.telegram,
            context.config.chat_id,
            uploaded,
        )
        .await;
        return Err(error);
    }
    if total != expected_length {
        cleanup_uploaded(
            &context.db,
            &context.telegram,
            context.config.chat_id,
            uploaded,
        )
        .await;
        bail!("request Content-Length {expected_length} does not match body length {total}");
    }
    Ok((uploaded, total, digest))
}

async fn upload_unknown_length(
    context: &UploadContext,
    upload_id: &str,
    part_number: i32,
    body: Body,
) -> Result<(Vec<UploadedBlock>, i64, Md5)> {
    let spool = spool_body(
        &context.config,
        &context.limits,
        upload_id,
        part_number,
        body,
    )
    .await?;
    tracing::info!(
        upload_id,
        part_number,
        bytes = spool.size,
        path = %spool.path.display(),
        "using disk spool for upload without Content-Length"
    );
    let result = upload_spooled(context, upload_id, part_number, &spool).await;
    if let Err(error) = fs::remove_file(&spool.path).await {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %spool.path.display(), error = %error, "failed to remove upload spool");
        }
    }
    result
}

struct Spool {
    path: PathBuf,
    size: i64,
    digest: Md5,
}

async fn spool_body(
    config: &Config,
    limits: &TransferLimits,
    upload_id: &str,
    part_number: i32,
    body: Body,
) -> Result<Spool> {
    let directory = config.data_dir.join("upload-spool");
    fs::create_dir_all(&directory).await?;
    let path = directory.join(format!(
        "{}-{}-{}.part",
        upload_id,
        part_number,
        Uuid::new_v4()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await?;
    let mut source = body.into_data_stream();
    let mut digest = Md5::new();
    let mut size = 0_i64;
    let result = async {
        while let Some(frame) = source.next().await {
            let data = frame.map_err(anyhow::Error::from)?;
            let next_size = size
                .checked_add(data.len() as i64)
                .ok_or_else(|| anyhow!("EntityTooLarge"))?;
            if next_size > config.max_object_size {
                bail!("EntityTooLarge");
            }
            limits
                .bandwidth
                .throttle(TransferDirection::Upload, data.len(), true)
                .await?;
            file.write_all(&data).await?;
            digest.update(&data);
            size = next_size;
        }
        file.flush().await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    drop(file);
    if let Err(error) = result {
        let _ = fs::remove_file(&path).await;
        return Err(error);
    }
    Ok(Spool { path, size, digest })
}

async fn upload_spooled(
    context: &UploadContext,
    upload_id: &str,
    part_number: i32,
    spool: &Spool,
) -> Result<(Vec<UploadedBlock>, i64, Md5)> {
    let mut tasks = UploadTasks::new();
    let mut uploaded = Vec::new();
    let mut failure = None;
    let mut ordinal = 0_i64;
    let mut offset = 0_i64;
    while offset < spool.size {
        let size = block_size(context.config.chunk_size, offset, spool.size)?;
        let file_path = spool.path.clone();
        let task = start_file_task(
            context,
            BlockSpec {
                upload_id: upload_id.to_string(),
                part_number,
                ordinal,
                offset,
                size,
            },
            file_path,
        );
        tasks.push(task);
        ordinal += 1;
        offset = match offset.checked_add(size as i64) {
            Some(offset) => offset,
            None => {
                failure = Some(anyhow!("EntityTooLarge"));
                break;
            }
        };
        if tasks.len() >= context.config.upload_concurrency {
            if let Err(error) = reap_one(&mut tasks, &mut uploaded).await {
                failure = Some(error);
                break;
            }
        }
    }
    drain_tasks(&mut tasks, &mut uploaded, &mut failure).await;
    if let Some(error) = failure {
        cleanup_uploaded(
            &context.db,
            &context.telegram,
            context.config.chat_id,
            uploaded,
        )
        .await;
        return Err(error);
    }
    Ok((uploaded, spool.size, spool.digest.clone()))
}

struct PipeTask {
    writer: tokio::io::DuplexStream,
    size: u64,
    written: u64,
    task: UploadTask,
}

fn start_pipe_task(context: &UploadContext, spec: BlockSpec) -> PipeTask {
    let (writer, reader) = tokio::io::duplex(STREAM_BUFFER_SIZE);
    let reader: UploadReader = Box::pin(reader);
    let size = spec.size;
    let task = spawn_upload_task(context.clone(), spec, reader);
    PipeTask {
        writer,
        size,
        written: 0,
        task,
    }
}

async fn finish_pipe_task(mut block: PipeTask, tasks: &mut UploadTasks) -> Result<()> {
    if let Err(error) = block.writer.shutdown().await {
        block.task.abort();
        return Err(error.into());
    }
    tasks.push(block.task);
    Ok(())
}

fn start_file_task(context: &UploadContext, spec: BlockSpec, path: PathBuf) -> UploadTask {
    let context = context.clone();
    let offset = spec.offset;
    let size = spec.size;
    let task = async move {
        let mut file = File::open(path).await?;
        file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
        let reader: UploadReader = Box::pin(file.take(size));
        upload_one(context, spec, reader).await
    };
    tokio::spawn(task)
}

fn spawn_upload_task(context: UploadContext, spec: BlockSpec, reader: UploadReader) -> UploadTask {
    tokio::spawn(upload_one(context, spec, reader))
}

async fn upload_one(
    context: UploadContext,
    spec: BlockSpec,
    reader: UploadReader,
) -> Result<UploadedBlock> {
    let _permit: OwnedSemaphorePermit = context
        .upload_slots
        .acquire_owned()
        .await
        .map_err(|_| anyhow!("upload semaphore closed"))?;
    let filename = format!(
        "tg2s3-{}-{}-{}.part",
        spec.upload_id, spec.part_number, spec.ordinal
    );
    let document = context
        .telegram
        .upload_stream(reader, spec.size, &filename)
        .await?;
    if document.file_size != 0 && document.file_size != spec.size as i64 {
        if let Err(error) = context
            .telegram
            .delete_message_by_id(document.backend, document.message_id)
            .await
        {
            tracing::warn!(
                message_id = document.message_id,
                error = %error,
                "failed to delete Telegram upload with invalid size"
            );
        }
        bail!(
            "Telegram stored chunk with size {}, expected {}",
            document.file_size,
            spec.size
        );
    }
    let mut block = uploaded_block(spec.ordinal, spec.offset, spec.size as i64, document);
    let reference = BlockRef {
        id: 0,
        ordinal: block.ordinal,
        offset: block.offset,
        size: block.data_size,
        chat_id: context.config.chat_id,
        message_id: block.message_id,
        backend: block.backend,
        document_id: block.document_id,
        file_id: block.file_id.clone(),
        file_unique_id: block.file_unique_id.clone(),
        message_date: block.message_date,
    };
    block.id = match context.db.add_staged_block(&reference).await {
        Ok(id) => id,
        Err(error) => {
            if let Err(cleanup_error) = context
                .telegram
                .delete_message_by_id(block.backend, block.message_id)
                .await
            {
                tracing::warn!(
                    message_id = block.message_id,
                    error = %cleanup_error,
                    "failed to delete Telegram upload after SQLite staging failed"
                );
            }
            return Err(error);
        }
    };
    Ok(block)
}

fn uploaded_block(
    ordinal: i64,
    offset: i64,
    data_size: i64,
    document: StoredDocument,
) -> UploadedBlock {
    UploadedBlock {
        id: 0,
        ordinal,
        offset,
        data_size,
        message_id: document.message_id,
        backend: document.backend,
        document_id: document.document_id,
        file_id: document.file_id,
        file_unique_id: document.file_unique_id,
        message_date: document.message_date,
    }
}

async fn reap_one(tasks: &mut UploadTasks, uploaded: &mut Vec<UploadedBlock>) -> Result<()> {
    let result = tasks
        .next()
        .await
        .ok_or_else(|| anyhow!("upload task disappeared"))?;
    uploaded.push(join_task(result)?);
    Ok(())
}

async fn drain_tasks(
    tasks: &mut UploadTasks,
    uploaded: &mut Vec<UploadedBlock>,
    failure: &mut Option<anyhow::Error>,
) {
    while let Some(result) = tasks.next().await {
        match join_task(result) {
            Ok(block) => uploaded.push(block),
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
}

fn join_task(
    result: std::result::Result<Result<UploadedBlock>, tokio::task::JoinError>,
) -> Result<UploadedBlock> {
    result.map_err(|error| anyhow!("Telegram upload task failed: {error}"))?
}

fn block_size(chunk_size: usize, offset: i64, total: i64) -> Result<u64> {
    if chunk_size == 0 {
        bail!("chunk size must be greater than zero");
    }
    let remaining = total
        .checked_sub(offset)
        .ok_or_else(|| anyhow!("object storage layout is invalid"))?;
    if remaining <= 0 {
        bail!("object storage layout is invalid");
    }
    Ok(remaining.min(chunk_size as i64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_non_empty_block_sizes() -> Result<()> {
        assert_eq!(block_size(8, 0, 20)?, 8);
        assert_eq!(block_size(8, 8, 20)?, 8);
        assert_eq!(block_size(8, 16, 20)?, 4);
        assert!(block_size(0, 0, 20).is_err());
        assert!(block_size(8, 20, 20).is_err());
        Ok(())
    }
}
