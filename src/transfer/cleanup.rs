use super::upload::UploadedBlock;
use crate::db::Db;
use crate::model::BlockRef;
use crate::telegram::{TelegramClient, is_missing_message};

pub(crate) fn references_for_uploaded(chat_id: i64, uploaded: &[UploadedBlock]) -> Vec<BlockRef> {
    uploaded
        .iter()
        .map(|block| to_reference(chat_id, block))
        .collect()
}

pub(crate) async fn cleanup_uploaded(
    db: &Db,
    telegram: &TelegramClient,
    chat_id: i64,
    uploaded: Vec<UploadedBlock>,
) {
    let references = uploaded
        .iter()
        .map(|block| to_reference(chat_id, block))
        .collect();
    cleanup_block_refs(db, telegram, references).await;
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
        match db.delete_stale_block(reference.id).await {
            Ok(()) => true,
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
                        if let Err(queue_error) = db.delete_stale_block(id).await {
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
