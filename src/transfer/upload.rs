#[path = "upload/known.rs"]
mod known;
#[path = "upload/spool.rs"]
mod spool;
#[path = "upload/task.rs"]
mod task;
#[cfg(test)]
#[path = "upload/tests.rs"]
mod tests;

use crate::config::Config;
use crate::db::Db;
use crate::limits::TransferLimits;
use crate::model::{BlockRef, PartRecord, TelegramBackend};
use crate::telegram::{StoredDocument, TelegramClient};
use anyhow::{Result, anyhow};
use axum::body::Body;
use md5::Digest;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use super::cleanup::{UploadCleanup, UploadCleanupGuard, references_for_uploaded};

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

impl UploadedBlock {
    pub(super) fn to_block_ref(&self, chat_id: i64) -> BlockRef {
        BlockRef {
            id: self.id,
            ordinal: self.ordinal,
            offset: self.offset,
            size: self.data_size,
            chat_id,
            message_id: self.message_id,
            backend: self.backend,
            document_id: self.document_id,
            file_id: self.file_id.clone(),
            file_unique_id: self.file_unique_id.clone(),
            message_date: self.message_date,
        }
    }
}

pub(super) type UploadTask = JoinHandle<Result<UploadedBlock>>;

#[derive(Clone)]
pub(crate) struct UploadContext {
    db: Db,
    telegram: TelegramClient,
    config: Arc<Config>,
    limits: Arc<TransferLimits>,
    upload_slots: Arc<Semaphore>,
    cleanup: UploadCleanup,
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
            cleanup: UploadCleanup::new(db, telegram),
        }
    }
}

#[derive(Clone)]
pub(super) struct BlockSpec {
    pub(super) upload_id: String,
    pub(super) part_number: i32,
    pub(super) ordinal: i64,
    pub(super) offset: i64,
    pub(super) size: u64,
}

pub(crate) async fn cleanup_stale_spools(data_dir: &Path) -> Result<usize> {
    spool::cleanup_stale_spools(data_dir).await
}

pub(crate) async fn upload(
    mut context: UploadContext,
    upload_id: &str,
    part_number: i32,
    body: Body,
    expected_length: Option<i64>,
) -> Result<(PartRecord, i64)> {
    let mut cleanup = UploadCleanupGuard::new(&context.db, &context.telegram);
    context.cleanup = cleanup.registry();
    let result = match expected_length {
        Some(expected_length) => {
            known::upload_known_length(&context, upload_id, part_number, body, expected_length)
                .await
        }
        None => spool::upload_unknown_length(&context, upload_id, part_number, body).await,
    };
    let (mut uploaded, total, digest) = match result {
        Ok(result) => result,
        Err(error) => {
            cleanup.cleanup().await;
            return Err(error);
        }
    };

    uploaded.sort_by_key(|block| block.ordinal);
    let refs = references_for_uploaded(context.config.chat_id, &uploaded);
    let etag = format!("{:x}", digest.finalize());
    cleanup.disarm();
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

async fn reap_one(tasks: &mut task::UploadTasks, uploaded: &mut Vec<UploadedBlock>) -> Result<()> {
    let result = tasks
        .next()
        .await
        .ok_or_else(|| anyhow!("upload task disappeared"))?;
    uploaded.push(join_task(result)?);
    Ok(())
}

async fn drain_tasks(
    tasks: &mut task::UploadTasks,
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
        anyhow::bail!("chunk size must be greater than zero");
    }
    let remaining = total
        .checked_sub(offset)
        .ok_or_else(|| anyhow!("object storage layout is invalid"))?;
    if remaining <= 0 {
        anyhow::bail!("object storage layout is invalid");
    }
    Ok(remaining.min(chunk_size as i64) as u64)
}
