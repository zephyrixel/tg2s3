use crate::model::{
    BlockRef, ObjectMetadata, PartRecord, TelegramBackend, UploadRecord, normalize_etag,
};
use anyhow::{Context, Result, anyhow, bail};
use sqlx::sqlite::SqliteRow;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(super) fn parse_metadata(value: &str) -> Result<ObjectMetadata> {
    serde_json::from_str(value).context("decode stored metadata")
}

pub(super) fn escape_glob(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '*' => escaped.push_str("[*]"),
            '?' => escaped.push_str("[?]"),
            '[' => escaped.push_str("[[]"),
            ']' => escaped.push_str("[]]"),
            character => escaped.push(character),
        }
    }
    escaped
}

pub(super) fn upload_from_row(row: SqliteRow) -> Result<UploadRecord> {
    Ok(UploadRecord {
        upload_id: row.get("upload_id"),
        bucket: row.get("bucket"),
        key: row.get("object_key"),
        metadata: parse_metadata(&row.get::<String, _>("metadata_json"))?,
        kind: row.get("kind"),
        created_at: row.get("created_at"),
    })
}

pub(super) fn validate_parts_layout(parts: &[PartRecord]) -> Result<i64> {
    let mut total = 0_i64;
    let mut block_ids = HashSet::new();
    for part in parts {
        let mut offset = 0_i64;
        for block in &part.blocks {
            if block.size <= 0 || block.offset != offset || !block_ids.insert(block.id) {
                bail!("object storage layout is invalid");
            }
            offset = offset
                .checked_add(block.size)
                .ok_or_else(|| anyhow!("object storage layout is invalid"))?;
        }
        if offset != part.size {
            bail!("object storage layout is invalid");
        }
        total = total
            .checked_add(offset)
            .ok_or_else(|| anyhow!("object storage layout is invalid"))?;
    }
    Ok(total)
}

pub(super) async fn load_object_blocks(pool: &SqlitePool, object_id: i64) -> Result<Vec<BlockRef>> {
    let rows = sqlx::query(
        "SELECT b.id, ob.ordinal, ob.byte_offset, ob.size, b.chat_id, b.message_id,
                b.backend, b.document_id, b.file_id, b.file_unique_id, b.message_date
         FROM object_blocks ob JOIN telegram_blocks b ON b.id = ob.block_id
         WHERE ob.object_id = ?1 ORDER BY ob.ordinal",
    )
    .bind(object_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(block_from_row).collect()
}

pub(super) async fn load_object_blocks_tx(
    tx: &mut Transaction<'_, Sqlite>,
    object_id: i64,
) -> Result<Vec<BlockRef>> {
    let rows = sqlx::query(
        "SELECT b.id, ob.ordinal, ob.byte_offset, ob.size, b.chat_id, b.message_id,
                b.backend, b.document_id, b.file_id, b.file_unique_id, b.message_date
         FROM object_blocks ob JOIN telegram_blocks b ON b.id = ob.block_id
         WHERE ob.object_id = ?1 ORDER BY ob.ordinal",
    )
    .bind(object_id)
    .fetch_all(&mut **tx)
    .await?;
    rows.into_iter().map(block_from_row).collect()
}

pub(super) fn block_from_row(row: SqliteRow) -> Result<BlockRef> {
    let backend = parse_backend(row.get(6))?;
    Ok(BlockRef {
        id: row.get(0),
        ordinal: row.get(1),
        offset: row.get(2),
        size: row.get(3),
        chat_id: row.get(4),
        message_id: row.get(5),
        backend,
        document_id: row.get(7),
        file_id: row.get(8),
        file_unique_id: row.get(9),
        message_date: row.get(10),
    })
}

pub(super) fn parse_backend(value: String) -> Result<TelegramBackend> {
    value
        .parse()
        .map_err(|error| anyhow!("invalid Telegram block backend: {error}"))
}

pub(super) async fn decrement_block(tx: &mut Transaction<'_, Sqlite>, block_id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE telegram_blocks SET ref_count = CASE
         WHEN ref_count > 0 THEN ref_count - 1 ELSE 0 END WHERE id = ?1",
    )
    .bind(block_id)
    .execute(&mut **tx)
    .await?;
    let zero = sqlx::query(
        "SELECT chat_id, message_id, message_date FROM telegram_blocks
         WHERE id = ?1 AND ref_count = 0",
    )
    .bind(block_id)
    .fetch_optional(&mut **tx)
    .await?;
    if zero.is_some() {
        sqlx::query(
            "INSERT OR IGNORE INTO gc_queue(block_id, next_attempt, state)
             VALUES(?1, ?2, 'pending')",
        )
        .bind(block_id)
        .bind(now())
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

// A writer must not resurrect a block after GC has claimed it.
pub(super) async fn ensure_blocks_not_queued(
    tx: &mut Transaction<'_, Sqlite>,
    block_ids: &[i64],
) -> Result<()> {
    if block_ids.is_empty() {
        return Ok(());
    }
    let mut query =
        QueryBuilder::<Sqlite>::new("SELECT block_id FROM gc_queue WHERE block_id IN (");
    let mut separated = query.separated(", ");
    for block_id in block_ids {
        separated.push_bind(block_id);
    }
    separated.push_unseparated(")");
    if query.build().fetch_optional(&mut **tx).await?.is_some() {
        bail!("SlowDown: upload block is pending garbage collection");
    }
    Ok(())
}

pub(super) async fn verify_parts_tx(
    tx: &mut Transaction<'_, Sqlite>,
    upload_id: &str,
    parts: &[PartRecord],
) -> Result<()> {
    let rows = sqlx::query(
        "SELECT part_number, size, etag FROM multipart_parts
         WHERE upload_id = ?1 ORDER BY part_number",
    )
    .bind(upload_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut expected = HashMap::with_capacity(parts.len());
    for part in parts {
        if expected.insert(part.part_number, part).is_some() {
            bail!("InvalidPart");
        }
    }
    if rows.len() != expected.len() {
        bail!("InvalidPart");
    }

    let block_rows = sqlx::query(
        "SELECT part_number, ordinal, block_id, byte_offset, size
         FROM multipart_part_blocks WHERE upload_id = ?1
         ORDER BY part_number, ordinal",
    )
    .bind(upload_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut persisted_blocks: HashMap<i32, Vec<(i64, i64, i64)>> = HashMap::new();
    for row in block_rows {
        persisted_blocks
            .entry(row.get("part_number"))
            .or_default()
            .push((row.get("block_id"), row.get("byte_offset"), row.get("size")));
    }

    for row in rows {
        let part_number: i32 = row.get("part_number");
        let Some(part) = expected.remove(&part_number) else {
            bail!("InvalidPart");
        };
        let size: i64 = row.get("size");
        let etag: String = row.get("etag");
        if size != part.size || normalize_etag(&etag) != normalize_etag(&part.etag) {
            bail!("InvalidPart");
        }
        let actual = persisted_blocks.remove(&part_number).unwrap_or_default();
        if actual.len() != part.blocks.len()
            || actual
                .iter()
                .zip(&part.blocks)
                .any(|((id, offset, size), block)| {
                    *id != block.id || *offset != block.offset || *size != block.size
                })
        {
            bail!("InvalidPart");
        }
    }
    if !expected.is_empty() || !persisted_blocks.is_empty() {
        bail!("InvalidPart");
    }
    Ok(())
}
