use super::upload::UploadedBlock;
use crate::db::Db;
use crate::model::BlockRef;
use crate::telegram::{TelegramClient, is_missing_message};
use std::sync::{Arc, Mutex};

pub(crate) fn references_for_uploaded(chat_id: i64, uploaded: &[UploadedBlock]) -> Vec<BlockRef> {
    uploaded
        .iter()
        .map(|block| to_reference(chat_id, block))
        .collect()
}

// Task clones register staged blocks here; the owning guard drains them on cancellation.
#[derive(Clone)]
pub(crate) struct UploadCleanup {
    state: Arc<Mutex<CleanupState>>,
    db: Db,
    telegram: TelegramClient,
}

struct CleanupState {
    closed: bool,
    references: Vec<BlockRef>,
}

impl UploadCleanup {
    pub(crate) fn new(db: &Db, telegram: &TelegramClient) -> Self {
        Self {
            state: Arc::new(Mutex::new(CleanupState {
                closed: false,
                references: Vec::new(),
            })),
            db: db.clone(),
            telegram: telegram.clone(),
        }
    }

    pub(crate) fn register(&self, reference: BlockRef) {
        let cleanup_now = {
            let mut state = self.state.lock().expect("upload cleanup state poisoned");
            if state.closed {
                true
            } else {
                state.references.push(reference.clone());
                false
            }
        };
        if cleanup_now {
            self.spawn_cleanup(vec![reference]);
        }
    }

    fn close_and_take(&self) -> Vec<BlockRef> {
        let mut state = self.state.lock().expect("upload cleanup state poisoned");
        state.closed = true;
        std::mem::take(&mut state.references)
    }

    fn close(&self) {
        self.state
            .lock()
            .expect("upload cleanup state poisoned")
            .closed = true;
    }

    fn snapshot(&self) -> Vec<BlockRef> {
        self.state
            .lock()
            .expect("upload cleanup state poisoned")
            .references
            .clone()
    }

    fn clear(&self) {
        self.state
            .lock()
            .expect("upload cleanup state poisoned")
            .references
            .clear();
    }

    fn spawn_cleanup(&self, references: Vec<BlockRef>) {
        if references.is_empty() {
            return;
        }
        let db = self.db.clone();
        let telegram = self.telegram.clone();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::error!(
                count = references.len(),
                "cannot schedule cleanup for Telegram upload without a Tokio runtime"
            );
            return;
        };
        handle.spawn(async move {
            cleanup_block_refs(&db, &telegram, references).await;
        });
    }
}

pub(crate) struct UploadCleanupGuard {
    cleanup: UploadCleanup,
    armed: bool,
}

impl UploadCleanupGuard {
    pub(crate) fn new(db: &Db, telegram: &TelegramClient) -> Self {
        Self {
            cleanup: UploadCleanup::new(db, telegram),
            armed: true,
        }
    }

    pub(crate) fn registry(&self) -> UploadCleanup {
        self.cleanup.clone()
    }

    pub(crate) async fn cleanup(&mut self) {
        self.cleanup.close();
        let references = self.cleanup.snapshot();
        cleanup_block_refs(&self.cleanup.db, &self.cleanup.telegram, references).await;
        self.cleanup.clear();
        self.armed = false;
    }

    pub(crate) fn disarm(mut self) {
        self.armed = false;
        self.cleanup.close_and_take();
    }
}

impl Drop for UploadCleanupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let references = self.cleanup.close_and_take();
        self.cleanup.spawn_cleanup(references);
    }
}

pub(crate) async fn cleanup_block_refs(
    db: &Db,
    telegram: &TelegramClient,
    references: Vec<BlockRef>,
) {
    for reference in references {
        cleanup_untracked_block(db, telegram, reference).await;
    }
}

fn to_reference(chat_id: i64, block: &UploadedBlock) -> BlockRef {
    BlockRef {
        id: block.id,
        ordinal: block.ordinal,
        offset: block.offset,
        size: block.data_size,
        chat_id,
        message_id: block.message_id,
        backend: block.backend,
        document_id: block.document_id,
        file_id: block.file_id.clone(),
        file_unique_id: block.file_unique_id.clone(),
        message_date: block.message_date,
    }
}

async fn cleanup_untracked_block(db: &Db, telegram: &TelegramClient, reference: BlockRef) {
    let queued = if reference.id > 0 {
        match db.queue_stale_block(reference.id).await {
            Ok(true) => true,
            Ok(false) => {
                tracing::debug!(
                    block_id = reference.id,
                    "skipping Telegram cleanup because block is no longer staged"
                );
                return;
            }
            Err(queue_error) => {
                tracing::warn!(
                    block_id = reference.id,
                    error = %queue_error,
                    "failed to queue Telegram orphan for GC"
                );
                false
            }
        }
    } else {
        false
    };
    match telegram
        .delete_message_by_id(reference.backend, reference.message_id)
        .await
    {
        Ok(()) => {
            if queued {
                if let Err(gc_error) = db.gc_success(reference.id).await {
                    tracing::warn!(block_id = reference.id, error = %gc_error, "failed to remove deleted Telegram orphan record");
                }
            }
        }
        Err(error) if reference.id > 0 && is_missing_message(&error) => {
            if queued {
                if let Err(gc_error) = db.gc_success(reference.id).await {
                    tracing::warn!(block_id = reference.id, error = %gc_error, "failed to remove deleted Telegram orphan record");
                }
            }
        }
        Err(error) => {
            tracing::warn!(
                message_id = reference.message_id,
                error = %error,
                "failed to delete Telegram upload; queueing it for GC"
            );
            if reference.id == 0 {
                match db.add_staged_block(&reference).await {
                    Ok(id) => {
                        if let Err(queue_error) = db.queue_stale_block(id).await {
                            tracing::warn!(
                                block_id = id,
                                error = %queue_error,
                                "failed to queue Telegram orphan for GC"
                            );
                        }
                    }
                    Err(db_error) => {
                        tracing::error!(
                            message_id = reference.message_id,
                            error = %db_error,
                            "failed to record Telegram orphan"
                        );
                    }
                }
            }
        }
    }
}
