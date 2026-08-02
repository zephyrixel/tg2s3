use super::{BlockSpec, UploadContext, UploadTask, UploadedBlock, uploaded_block};
use crate::model::TelegramBackend;
use crate::telegram::{StoredDocument, UploadReader};
use anyhow::{Result, anyhow, bail};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::OwnedSemaphorePermit;

pub(super) struct UploadTasks {
    inner: FuturesUnordered<UploadTask>,
}

impl UploadTasks {
    pub(super) fn new() -> Self {
        Self {
            inner: FuturesUnordered::new(),
        }
    }

    pub(super) fn push(&mut self, task: UploadTask) {
        self.inner.push(task);
    }

    pub(super) fn len(&self) -> usize {
        self.inner.len()
    }

    pub(super) async fn next(
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

pub(super) struct PipeTask {
    pub(super) writer: tokio::io::DuplexStream,
    pub(super) size: u64,
    pub(super) written: u64,
    task: UploadTask,
}

pub(super) fn start_pipe_task(context: &UploadContext, spec: BlockSpec) -> PipeTask {
    let (writer, reader) = tokio::io::duplex(super::super::STREAM_BUFFER_SIZE);
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

pub(super) async fn finish_pipe_task(mut block: PipeTask, tasks: &mut UploadTasks) -> Result<()> {
    if let Err(error) = block.writer.shutdown().await {
        block.task.abort();
        return Err(error.into());
    }
    tasks.push(block.task);
    Ok(())
}

pub(super) fn start_file_task(
    context: &UploadContext,
    spec: BlockSpec,
    path: PathBuf,
) -> UploadTask {
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

struct MessageCleanupGuard {
    telegram: crate::telegram::TelegramClient,
    backend: TelegramBackend,
    message_id: i64,
    armed: bool,
}

impl MessageCleanupGuard {
    fn new(telegram: &crate::telegram::TelegramClient, document: &StoredDocument) -> Self {
        Self {
            telegram: telegram.clone(),
            backend: document.backend,
            message_id: document.message_id,
            armed: true,
        }
    }

    async fn cleanup(mut self) {
        if let Err(error) = self
            .telegram
            .delete_message_by_id(self.backend, self.message_id)
            .await
        {
            tracing::warn!(
                message_id = self.message_id,
                error = %error,
                "failed to delete Telegram upload message"
            );
        }
        self.armed = false;
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for MessageCleanupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let telegram = self.telegram.clone();
        let backend = self.backend;
        let message_id = self.message_id;
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::error!(
                message_id,
                "cannot schedule cleanup for Telegram upload message without a Tokio runtime"
            );
            return;
        };
        handle.spawn(async move {
            if let Err(error) = telegram.delete_message_by_id(backend, message_id).await {
                tracing::warn!(message_id, error = %error, "failed to delete Telegram upload message");
            }
        });
    }
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
    let message_cleanup = MessageCleanupGuard::new(&context.telegram, &document);
    if document.file_size != 0 && document.file_size != spec.size as i64 {
        message_cleanup.cleanup().await;
        bail!(
            "Telegram stored chunk with size {}, expected {}",
            document.file_size,
            spec.size
        );
    }
    let mut block = uploaded_block(spec.ordinal, spec.offset, spec.size as i64, document);
    let reference = block.to_block_ref(context.config.chat_id);
    block.id = match context.db.add_staged_block(&reference).await {
        Ok(id) => id,
        Err(error) => {
            message_cleanup.cleanup().await;
            return Err(error);
        }
    };
    context
        .cleanup
        .register(block.to_block_ref(context.config.chat_id));
    message_cleanup.disarm();
    Ok(block)
}
