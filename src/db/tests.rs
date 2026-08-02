use super::Db;
use super::support::{now, validate_parts_layout};
use crate::model::{
    BlockRef, ObjectCondition, ObjectMetadata, PartRecord, TelegramBackend, UploadRecord,
};
use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::tempdir;

fn block(message_id: i64, size: i64) -> BlockRef {
    BlockRef {
        id: 0,
        ordinal: 0,
        offset: 0,
        size,
        chat_id: -100,
        message_id,
        backend: TelegramBackend::BotApi,
        document_id: None,
        file_id: format!("file-{message_id}"),
        file_unique_id: format!("unique-{message_id}"),
        message_date: now(),
    }
}

#[test]
fn rejects_inconsistent_part_layouts() {
    let part = PartRecord {
        upload_id: "upload".to_string(),
        part_number: 1,
        size: 4,
        etag: "etag".to_string(),
        blocks: vec![block(1, 3)],
    };
    assert!(validate_parts_layout(&[part]).is_err());
}

#[tokio::test]
async fn loads_public_and_internal_part_blocks() -> Result<()> {
    let dir = tempdir()?;
    let db = Db::open(&dir.path().join("test.sqlite3")).await?;
    db.create_bucket("bucket").await?;
    let upload = UploadRecord {
        upload_id: "multipart-1".to_string(),
        bucket: "bucket".to_string(),
        key: "object".to_string(),
        metadata: ObjectMetadata::default(),
        kind: "multipart".to_string(),
        created_at: now(),
    };
    db.create_upload(&upload).await?;

    let internal_source = block(0, 3);
    let internal_id = db.add_staged_block(&internal_source).await?;
    let first = block(1, 3);
    let first_id = db.add_staged_block(&first).await?;
    let second = BlockRef {
        offset: 3,
        ..block(2, 3)
    };
    let second_id = db.add_staged_block(&second).await?;
    let third = block(3, 2);
    let third_id = db.add_staged_block(&third).await?;
    let internal = BlockRef {
        id: internal_id,
        ..internal_source
    };
    db.replace_part(&upload.upload_id, 0, 3, "internal", &[internal])
        .await?;
    db.replace_part(
        &upload.upload_id,
        1,
        6,
        "one",
        &[
            BlockRef {
                id: first_id,
                offset: 0,
                ..block(1, 3)
            },
            BlockRef {
                id: second_id,
                offset: 3,
                ..second
            },
        ],
    )
    .await?;
    db.replace_part(
        &upload.upload_id,
        2,
        2,
        "two",
        &[BlockRef {
            id: third_id,
            ..third
        }],
    )
    .await?;

    let public_parts = db.get_parts(&upload.upload_id, false).await?;
    assert_eq!(public_parts.len(), 2);
    assert_eq!(public_parts[0].blocks.len(), 2);
    assert_eq!(public_parts[1].blocks.len(), 1);
    assert!(!db.queue_stale_block(first_id).await?);
    let all_parts = db.get_parts(&upload.upload_id, true).await?;
    assert_eq!(all_parts.len(), 3);
    assert_eq!(all_parts[0].part_number, 0);
    Ok(())
}

#[tokio::test]
async fn bounds_stale_scan_and_rejects_live_blocks() -> Result<()> {
    let dir = tempdir()?;
    let db = Db::open(&dir.path().join("test.sqlite3")).await?;
    let staged_id = db.add_staged_block(&block(1, 3)).await?;
    let another_staged_id = db.add_staged_block(&block(2, 3)).await?;
    assert_eq!(db.stale_blocks(now() + 1, 1).await?.len(), 1);
    assert_eq!(db.stale_blocks(now() + 1, 10).await?.len(), 2);
    assert!(db.queue_stale_block(staged_id).await?);
    assert!(db.queue_stale_block(staged_id).await?);
    assert_eq!(db.gc_candidates(now(), 10).await?.len(), 1);

    let committed_id = db.add_staged_block(&block(3, 3)).await?;
    sqlx::query("UPDATE telegram_blocks SET state = 'committed' WHERE id = ?1")
        .bind(committed_id)
        .execute(db.pool())
        .await?;
    assert!(!db.queue_stale_block(committed_id).await?);
    assert!(
        db.gc_candidates(now(), 10)
            .await?
            .iter()
            .all(|candidate| candidate.block_id != committed_id)
    );
    assert!(db.queue_stale_block(another_staged_id).await?);
    assert_eq!(db.gc_candidates(now(), 10).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn object_commit_copy_delete_and_gc_preserve_references() -> Result<()> {
    let dir = tempdir()?;
    let db = Db::open(&dir.path().join("test.sqlite3")).await?;
    assert!(db.create_bucket("bucket").await?);

    let upload = UploadRecord {
        upload_id: "upload-1".to_string(),
        bucket: "bucket".to_string(),
        key: "one".to_string(),
        metadata: ObjectMetadata::default(),
        kind: "put".to_string(),
        created_at: now(),
    };
    db.create_upload(&upload).await?;
    let staged = BlockRef {
        backend: TelegramBackend::Grammers,
        document_id: Some(42),
        ..block(1, 3)
    };
    let id = db.add_staged_block(&staged).await?;
    let staged = BlockRef { id, ..staged };
    let part = PartRecord {
        upload_id: upload.upload_id.clone(),
        part_number: 0,
        size: 3,
        etag: "900150983cd24fb0d6963f7d28e17f72".to_string(),
        blocks: vec![staged.clone()],
    };
    db.replace_part(&upload.upload_id, 0, 3, &part.etag, &part.blocks)
        .await?;
    let etag = part.etag.clone();
    db.commit_upload(
        &upload.upload_id,
        3,
        &etag,
        &[part],
        &ObjectCondition::default(),
    )
    .await?
    .expect("commit");

    let loaded = db
        .get_object("bucket", "one")
        .await?
        .expect("loaded object");
    assert_eq!(loaded.blocks[0].backend, TelegramBackend::Grammers);
    assert_eq!(loaded.blocks[0].document_id, Some(42));

    let source = db
        .get_object("bucket", "one")
        .await?
        .expect("source object");
    assert_eq!(source.blocks.len(), 1);

    let conditional_upload = UploadRecord {
        upload_id: "conditional-upload".to_string(),
        bucket: "bucket".to_string(),
        key: "one".to_string(),
        metadata: ObjectMetadata::default(),
        kind: "put".to_string(),
        created_at: now(),
    };
    db.create_upload(&conditional_upload).await?;
    let replacement = block(2, 3);
    let replacement_id = db.add_staged_block(&replacement).await?;
    let replacement = BlockRef {
        id: replacement_id,
        ..replacement
    };
    let replacement_part = PartRecord {
        upload_id: conditional_upload.upload_id.clone(),
        part_number: 0,
        size: 3,
        etag: "f561aaf6ef0bf14d4208bb46a22e7d9f".to_string(),
        blocks: vec![replacement],
    };
    db.replace_part(
        &conditional_upload.upload_id,
        0,
        3,
        &replacement_part.etag,
        &replacement_part.blocks,
    )
    .await?;
    let stale_condition = ObjectCondition {
        if_match: Some("\"stale-etag\"".to_string()),
        if_none_match: None,
    };
    assert!(
        db.commit_upload(
            &conditional_upload.upload_id,
            3,
            "replacement-etag",
            &[replacement_part],
            &stale_condition,
        )
        .await
        .is_err()
    );
    assert!(db.abort_upload(&conditional_upload.upload_id).await?);
    let aborted = db.gc_candidates(now(), 10).await?;
    assert_eq!(aborted.len(), 1);
    db.gc_success(aborted[0].block_id).await?;
    assert_eq!(
        db.get_object("bucket", "one").await?.unwrap().etag,
        source.etag
    );

    assert!(
        db.copy_object(
            &source,
            "bucket",
            "two",
            &ObjectMetadata::default(),
            &stale_condition,
        )
        .await
        .is_err()
    );
    assert!(db.get_object("bucket", "two").await?.is_none());
    assert!(
        db.delete_object("bucket", "one", &stale_condition)
            .await
            .is_err()
    );
    assert!(db.get_object("bucket", "one").await?.is_some());

    db.copy_object(
        &source,
        "bucket",
        "two",
        &ObjectMetadata::default(),
        &ObjectCondition::default(),
    )
    .await?;
    db.delete_object("bucket", "one", &ObjectCondition::default())
        .await?
        .expect("delete source");
    assert!(db.get_object("bucket", "two").await?.is_some());
    assert!(
        db.copy_object(
            &source,
            "bucket",
            "stale-copy",
            &ObjectMetadata::default(),
            &ObjectCondition::default(),
        )
        .await
        .is_err()
    );
    assert!(db.gc_candidates(now(), 10).await?.is_empty());

    db.delete_object("bucket", "two", &ObjectCondition::default())
        .await?
        .expect("delete copy");
    let garbage = db.gc_candidates(now(), 10).await?;
    assert_eq!(garbage.len(), 1);
    db.gc_success(garbage[0].block_id).await?;
    assert!(db.gc_candidates(now(), 10).await?.is_empty());
    assert_eq!(db.integrity_check().await?, "ok");
    Ok(())
}

#[tokio::test]
async fn baseline_adopts_existing_database_without_losing_metadata() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("legacy.sqlite3");
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let legacy = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    sqlx::query("CREATE TABLE buckets(name TEXT PRIMARY KEY, created_at INTEGER NOT NULL)")
        .execute(&legacy)
        .await?;
    sqlx::query(
        "CREATE TABLE objects(
            id INTEGER PRIMARY KEY,
            bucket TEXT NOT NULL,
            object_key TEXT NOT NULL,
            size INTEGER NOT NULL,
            etag TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            modified_at INTEGER NOT NULL,
            UNIQUE(bucket, object_key)
        )",
    )
    .execute(&legacy)
    .await?;
    sqlx::query("INSERT INTO buckets(name, created_at) VALUES('legacy', 1)")
        .execute(&legacy)
        .await?;
    sqlx::query(
        "INSERT INTO objects
         (bucket, object_key, size, etag, metadata_json, created_at, modified_at)
         VALUES('legacy', 'file.txt', 3, 'etag', '{}', 1, 1)",
    )
    .execute(&legacy)
    .await?;
    legacy.close().await;

    let db = Db::open(&path).await?;
    assert!(db.bucket_exists("legacy").await?);
    let object = db.get_object("legacy", "file.txt").await?.expect("object");
    assert_eq!(object.size, 3);
    assert_eq!(object.etag, "etag");
    assert!(db.get_bucket_cors("legacy").await?.is_none());
    Ok(())
}

#[tokio::test]
async fn bucket_delete_rejects_active_uploads() -> Result<()> {
    let dir = tempdir()?;
    let db = Db::open(&dir.path().join("test.sqlite3")).await?;
    db.create_bucket("bucket").await?;
    db.create_upload(&UploadRecord {
        upload_id: "multipart-1".to_string(),
        bucket: "bucket".to_string(),
        key: "pending".to_string(),
        metadata: ObjectMetadata::default(),
        kind: "multipart".to_string(),
        created_at: now(),
    })
    .await?;

    assert_eq!(db.delete_bucket("bucket").await?, Some(false));
    assert!(db.abort_upload("multipart-1").await?);
    assert_eq!(db.delete_bucket("bucket").await?, Some(true));
    Ok(())
}

#[tokio::test]
async fn upload_creation_requires_an_existing_bucket() -> Result<()> {
    let dir = tempdir()?;
    let db = Db::open(&dir.path().join("test.sqlite3")).await?;
    let upload = UploadRecord {
        upload_id: "missing-bucket-upload".to_string(),
        bucket: "missing".to_string(),
        key: "object".to_string(),
        metadata: ObjectMetadata::default(),
        kind: "put".to_string(),
        created_at: now(),
    };
    assert!(db.create_upload(&upload).await.is_err());
    assert!(db.get_upload(&upload.upload_id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn queued_stale_block_cannot_be_reused_for_commit() -> Result<()> {
    let dir = tempdir()?;
    let db = Db::open(&dir.path().join("test.sqlite3")).await?;
    db.create_bucket("bucket").await?;
    let upload = UploadRecord {
        upload_id: "upload-stale".to_string(),
        bucket: "bucket".to_string(),
        key: "object".to_string(),
        metadata: ObjectMetadata::default(),
        kind: "put".to_string(),
        created_at: now(),
    };
    db.create_upload(&upload).await?;
    let staged = block(1, 3);
    let id = db.add_staged_block(&staged).await?;
    let staged = BlockRef { id, ..staged };
    db.queue_stale_block(id).await?;
    let part = PartRecord {
        upload_id: upload.upload_id.clone(),
        part_number: 0,
        size: 3,
        etag: "900150983cd24fb0d6963f7d28e17f72".to_string(),
        blocks: vec![staged],
    };
    assert!(
        db.replace_part(&upload.upload_id, 0, 3, &part.etag, &part.blocks)
            .await
            .is_err()
    );
    assert!(db.abort_upload(&upload.upload_id).await?);
    assert_eq!(db.gc_candidates(now(), 10).await?.len(), 1);
    Ok(())
}
