use crate::model::{
    BlockRef, BucketRecord, GarbageRecord, ListingRecord, ObjectMetadata, ObjectRecord, PartRecord,
    UploadRecord,
};
use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA: &str = r#"
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS buckets (
    name TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
    id INTEGER PRIMARY KEY,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    UNIQUE(bucket, object_key)
);

CREATE TABLE IF NOT EXISTS telegram_blocks (
    id INTEGER PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    file_id TEXT NOT NULL,
    file_unique_id TEXT NOT NULL DEFAULT '',
    size INTEGER NOT NULL,
    message_date INTEGER NOT NULL,
    ref_count INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'staged',
    created_at INTEGER NOT NULL,
    UNIQUE(chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS object_blocks (
    object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    block_id INTEGER NOT NULL REFERENCES telegram_blocks(id),
    byte_offset INTEGER NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY(object_id, ordinal)
);

CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id TEXT PRIMARY KEY,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'multipart',
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id TEXT NOT NULL REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY(upload_id, part_number)
);

CREATE TABLE IF NOT EXISTS multipart_part_blocks (
    upload_id TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    block_id INTEGER NOT NULL REFERENCES telegram_blocks(id),
    byte_offset INTEGER NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY(upload_id, part_number, ordinal),
    FOREIGN KEY(upload_id, part_number) REFERENCES multipart_parts(upload_id, part_number) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS gc_queue (
    block_id INTEGER PRIMARY KEY REFERENCES telegram_blocks(id) ON DELETE CASCADE,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt INTEGER NOT NULL,
    last_error TEXT,
    state TEXT NOT NULL DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_objects_listing ON objects(bucket, object_key);
CREATE INDEX IF NOT EXISTS idx_object_blocks ON object_blocks(object_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_staged_blocks ON telegram_blocks(state, created_at);
CREATE INDEX IF NOT EXISTS idx_gc_queue ON gc_queue(state, next_attempt);
CREATE INDEX IF NOT EXISTS idx_multipart_bucket ON multipart_uploads(bucket, object_key);
"#;

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn =
            Connection::open(path).with_context(|| format!("open SQLite {}", path.display()))?;
        conn.execute_batch(SCHEMA)
            .context("initialize SQLite schema")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| anyhow!("SQLite mutex poisoned"))
    }

    pub fn integrity_check(&self) -> Result<String> {
        let conn = self.lock()?;
        Ok(conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?)
    }

    pub fn create_bucket(&self, name: &str) -> Result<bool> {
        let conn = self.lock()?;
        let changed = conn.execute(
            "INSERT OR IGNORE INTO buckets(name, created_at) VALUES(?1, ?2)",
            params![name, now()],
        )?;
        Ok(changed == 1)
    }

    pub fn bucket_exists(&self, name: &str) -> Result<bool> {
        let conn = self.lock()?;
        Ok(conn
            .query_row("SELECT 1 FROM buckets WHERE name=?1", params![name], |_| {
                Ok(())
            })
            .optional()?
            .is_some())
    }

    pub fn list_buckets(&self) -> Result<Vec<BucketRecord>> {
        let conn = self.lock()?;
        let mut stmt = conn.prepare("SELECT name, created_at FROM buckets ORDER BY name")?;
        let rows = stmt.query_map([], |row| {
            Ok(BucketRecord {
                name: row.get(0)?,
                created_at: row.get(1)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn delete_bucket(&self, name: &str) -> Result<Option<bool>> {
        let conn = self.lock()?;
        let exists: Option<i64> = conn
            .query_row("SELECT 1 FROM buckets WHERE name=?1", params![name], |r| {
                r.get(0)
            })
            .optional()?;
        if exists.is_none() {
            return Ok(None);
        }
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM objects WHERE bucket=?1",
            params![name],
            |r| r.get(0),
        )?;
        if count != 0 {
            return Ok(Some(false));
        }
        conn.execute("DELETE FROM buckets WHERE name=?1", params![name])?;
        Ok(Some(true))
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectRecord>> {
        let conn = self.lock()?;
        let row: Option<(i64, i64, String, String, i64, i64)> = conn
            .query_row(
                "SELECT id, size, etag, metadata_json, created_at, modified_at FROM objects WHERE bucket=?1 AND object_key=?2",
                params![bucket, key],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
            )
            .optional()?;
        let Some((id, size, etag, metadata_json, created_at, modified_at)) = row else {
            return Ok(None);
        };
        let metadata: ObjectMetadata = serde_json::from_str(&metadata_json).unwrap_or_default();
        let blocks = load_object_blocks(&conn, id)?;
        Ok(Some(ObjectRecord {
            id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            size,
            etag,
            metadata,
            created_at,
            modified_at,
            blocks,
        }))
    }

    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<ListingRecord>> {
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT object_key, size, etag, modified_at FROM objects WHERE bucket=?1 AND object_key>?2 ORDER BY object_key",
        )?;
        let rows = stmt.query_map(params![bucket, after], |r| {
            Ok(ListingRecord {
                key: r.get(0)?,
                size: r.get(1)?,
                etag: r.get(2)?,
                modified_at: r.get(3)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            let row = row?;
            if row.key.starts_with(prefix) {
                result.push(row);
                if result.len() >= limit {
                    break;
                }
            }
        }
        Ok(result)
    }

    pub fn create_upload(&self, upload: &UploadRecord) -> Result<()> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO multipart_uploads(upload_id,bucket,object_key,metadata_json,kind,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
            params![upload.upload_id, upload.bucket, upload.key, serde_json::to_string(&upload.metadata)?, upload.kind, upload.created_at],
        )?;
        Ok(())
    }

    pub fn get_upload(&self, upload_id: &str) -> Result<Option<UploadRecord>> {
        let conn = self.lock()?;
        let row: Option<(String, String, String, String, String, i64)> = conn
            .query_row(
                "SELECT upload_id,bucket,object_key,metadata_json,kind,created_at FROM multipart_uploads WHERE upload_id=?1",
                params![upload_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
            )
            .optional()?;
        row.map(
            |(upload_id, bucket, key, metadata_json, kind, created_at)| {
                Ok(UploadRecord {
                    upload_id,
                    bucket,
                    key,
                    metadata: serde_json::from_str(&metadata_json).unwrap_or_default(),
                    kind,
                    created_at,
                })
            },
        )
        .transpose()
    }

    pub fn list_uploads(&self, bucket: &str, key: Option<&str>) -> Result<Vec<UploadRecord>> {
        let conn = self.lock()?;
        let mut result = Vec::new();
        if let Some(key) = key {
            let mut stmt = conn.prepare("SELECT upload_id,bucket,object_key,metadata_json,kind,created_at FROM multipart_uploads WHERE bucket=?1 AND object_key=?2 ORDER BY created_at")?;
            let rows = stmt.query_map(params![bucket, key], upload_from_row)?;
            for row in rows {
                result.push(row?);
            }
        } else {
            let mut stmt = conn.prepare("SELECT upload_id,bucket,object_key,metadata_json,kind,created_at FROM multipart_uploads WHERE bucket=?1 ORDER BY created_at")?;
            let rows = stmt.query_map(params![bucket], upload_from_row)?;
            for row in rows {
                result.push(row?);
            }
        }
        Ok(result)
    }

    pub fn add_staged_block(&self, block: &BlockRef) -> Result<i64> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO telegram_blocks(chat_id,message_id,file_id,file_unique_id,size,message_date,ref_count,state,created_at) VALUES(?1,?2,?3,?4,?5,?6,1,'staged',?7)",
            params![block.chat_id, block.message_id, block.file_id, block.file_unique_id, block.size, block.message_date, now()],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn replace_part(
        &self,
        upload_id: &str,
        part_number: i32,
        size: i64,
        etag: &str,
        block_ids: &[BlockRef],
    ) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let old: Vec<i64> = {
            let mut stmt = tx.prepare(
                "SELECT block_id FROM multipart_part_blocks WHERE upload_id=?1 AND part_number=?2",
            )?;
            stmt.query_map(params![upload_id, part_number], |r| r.get(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        tx.execute(
            "DELETE FROM multipart_parts WHERE upload_id=?1 AND part_number=?2",
            params![upload_id, part_number],
        )?;
        for block_id in old {
            decrement_block(&tx, block_id)?;
        }
        tx.execute(
            "INSERT INTO multipart_parts(upload_id,part_number,size,etag) VALUES(?1,?2,?3,?4)",
            params![upload_id, part_number, size, etag],
        )?;
        for (ordinal, block) in block_ids.iter().enumerate() {
            tx.execute(
                "INSERT INTO multipart_part_blocks(upload_id,part_number,ordinal,block_id,byte_offset,size) VALUES(?1,?2,?3,?4,?5,?6)",
                params![upload_id, part_number, ordinal as i64, block.id, block.offset, block.size],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_parts(&self, upload_id: &str, include_internal: bool) -> Result<Vec<PartRecord>> {
        let conn = self.lock()?;
        let mut stmt = if include_internal {
            conn.prepare("SELECT upload_id,part_number,size,etag FROM multipart_parts WHERE upload_id=?1 ORDER BY part_number")?
        } else {
            conn.prepare("SELECT upload_id,part_number,size,etag FROM multipart_parts WHERE upload_id=?1 AND part_number>0 ORDER BY part_number")?
        };
        let rows = stmt.query_map(params![upload_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i32>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (upload_id, part_number, size, etag) = row?;
            let blocks = load_part_blocks(&conn, &upload_id, part_number)?;
            result.push(PartRecord {
                upload_id,
                part_number,
                size,
                etag,
                blocks,
            });
        }
        Ok(result)
    }

    pub fn commit_upload(
        &self,
        upload_id: &str,
        size: i64,
        etag: &str,
        parts: &[PartRecord],
    ) -> Result<Option<(ObjectRecord, Vec<BlockRef>)>> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let upload: Option<(String, String, String, String, String, i64)> = tx
            .query_row("SELECT upload_id,bucket,object_key,metadata_json,kind,created_at FROM multipart_uploads WHERE upload_id=?1", params![upload_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))
            .optional()?;
        let Some((upload_id, bucket, key, metadata_json, _kind, created_at)) = upload else {
            return Ok(None);
        };
        let mut old_blocks = Vec::new();
        if let Some(object_id) = tx
            .query_row(
                "SELECT id FROM objects WHERE bucket=?1 AND object_key=?2",
                params![bucket, key],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
        {
            old_blocks = load_object_blocks_tx(&tx, object_id)?;
            tx.execute("DELETE FROM objects WHERE id=?1", params![object_id])?;
            for block in &old_blocks {
                decrement_block(&tx, block.id)?;
            }
        }
        let modified_at = now();
        tx.execute(
            "INSERT INTO objects(bucket,object_key,size,etag,metadata_json,created_at,modified_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![bucket, key, size, etag, metadata_json, created_at, modified_at],
        )?;
        let object_id = tx.last_insert_rowid();
        let mut ordinal = 0_i64;
        let mut offset = 0_i64;
        for part in parts {
            for block in &part.blocks {
                tx.execute("INSERT INTO object_blocks(object_id,ordinal,block_id,byte_offset,size) VALUES(?1,?2,?3,?4,?5)", params![object_id, ordinal, block.id, offset, block.size])?;
                tx.execute(
                    "UPDATE telegram_blocks SET state='committed' WHERE id=?1",
                    params![block.id],
                )?;
                ordinal += 1;
                offset += block.size;
            }
        }
        tx.execute(
            "DELETE FROM multipart_uploads WHERE upload_id=?1",
            params![upload_id],
        )?;
        tx.commit()?;
        drop(conn);
        let object = self
            .get_object(&bucket, &key)?
            .ok_or_else(|| anyhow!("object disappeared after commit"))?;
        Ok(Some((object, old_blocks)))
    }

    pub fn abort_upload(&self, upload_id: &str) -> Result<bool> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let exists: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM multipart_uploads WHERE upload_id=?1",
                params![upload_id],
                |r| r.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Ok(false);
        }
        let ids: Vec<i64> = {
            let mut stmt =
                tx.prepare("SELECT block_id FROM multipart_part_blocks WHERE upload_id=?1")?;
            stmt.query_map(params![upload_id], |r| r.get(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        tx.execute(
            "DELETE FROM multipart_uploads WHERE upload_id=?1",
            params![upload_id],
        )?;
        for id in ids {
            decrement_block(&tx, id)?;
        }
        tx.commit()?;
        Ok(true)
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectRecord>> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let object_id: Option<i64> = tx
            .query_row(
                "SELECT id FROM objects WHERE bucket=?1 AND object_key=?2",
                params![bucket, key],
                |r| r.get(0),
            )
            .optional()?;
        let Some(object_id) = object_id else {
            return Ok(None);
        };
        let old_blocks = load_object_blocks_tx(&tx, object_id)?;
        tx.execute("DELETE FROM objects WHERE id=?1", params![object_id])?;
        for block in &old_blocks {
            decrement_block(&tx, block.id)?;
        }
        tx.commit()?;
        Ok(Some(ObjectRecord {
            id: object_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: 0,
            etag: String::new(),
            metadata: ObjectMetadata::default(),
            created_at: 0,
            modified_at: 0,
            blocks: old_blocks,
        }))
    }

    pub fn copy_object(
        &self,
        source: &ObjectRecord,
        bucket: &str,
        key: &str,
        metadata: &ObjectMetadata,
    ) -> Result<(ObjectRecord, Vec<BlockRef>)> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut old_blocks = Vec::new();
        if let Some(old_id) = tx
            .query_row(
                "SELECT id FROM objects WHERE bucket=?1 AND object_key=?2",
                params![bucket, key],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
        {
            old_blocks = load_object_blocks_tx(&tx, old_id)?;
            tx.execute("DELETE FROM objects WHERE id=?1", params![old_id])?;
            for block in &old_blocks {
                decrement_block(&tx, block.id)?;
            }
        }
        let timestamp = now();
        let metadata_json = serde_json::to_string(metadata)?;
        tx.execute("INSERT INTO objects(bucket,object_key,size,etag,metadata_json,created_at,modified_at) VALUES(?1,?2,?3,?4,?5,?6,?7)", params![bucket,key,source.size,source.etag,metadata_json,timestamp,timestamp])?;
        let object_id = tx.last_insert_rowid();
        for block in &source.blocks {
            tx.execute(
                "UPDATE telegram_blocks SET ref_count=ref_count+1, state='committed' WHERE id=?1",
                params![block.id],
            )?;
            tx.execute("DELETE FROM gc_queue WHERE block_id=?1", params![block.id])?;
            tx.execute("INSERT INTO object_blocks(object_id,ordinal,block_id,byte_offset,size) VALUES(?1,?2,?3,?4,?5)", params![object_id,block.ordinal,block.id,block.offset,block.size])?;
        }
        tx.commit()?;
        drop(conn);
        let object = self
            .get_object(bucket, key)?
            .ok_or_else(|| anyhow!("copy result disappeared"))?;
        Ok((object, old_blocks))
    }

    pub fn gc_candidates(&self, timestamp: i64, limit: usize) -> Result<Vec<GarbageRecord>> {
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT q.block_id,b.chat_id,b.message_id,b.message_date,q.attempts,q.next_attempt,q.last_error FROM gc_queue q JOIN telegram_blocks b ON b.id=q.block_id WHERE q.state='pending' AND b.ref_count=0 AND q.next_attempt<=?1 ORDER BY q.next_attempt LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![timestamp, limit as i64], |r| {
            Ok(GarbageRecord {
                block_id: r.get(0)?,
                chat_id: r.get(1)?,
                message_id: r.get(2)?,
                message_date: r.get(3)?,
                attempts: r.get(4)?,
                next_attempt: r.get(5)?,
                last_error: r.get(6)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn gc_success(&self, block_id: i64) -> Result<()> {
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM telegram_blocks WHERE id=?1 AND ref_count=0",
            params![block_id],
        )?;
        Ok(())
    }

    pub fn gc_failure(&self, block_id: i64, error: &str, next_attempt: i64) -> Result<()> {
        let conn = self.lock()?;
        conn.execute("UPDATE gc_queue SET attempts=attempts+1,last_error=?2,next_attempt=?3 WHERE block_id=?1", params![block_id,error,next_attempt])?;
        Ok(())
    }

    pub fn gc_orphan(&self, block_id: i64, error: &str) -> Result<()> {
        let conn = self.lock()?;
        conn.execute(
            "UPDATE gc_queue SET state='orphan',last_error=?2 WHERE block_id=?1",
            params![block_id, error],
        )?;
        Ok(())
    }

    pub fn expire_uploads(&self, before: i64) -> Result<usize> {
        let uploads = {
            let conn = self.lock()?;
            let mut stmt =
                conn.prepare("SELECT upload_id FROM multipart_uploads WHERE created_at<?1")?;
            stmt.query_map(params![before], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut count = 0;
        for id in uploads {
            if self.abort_upload(&id)? {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn stale_blocks(&self, before: i64) -> Result<Vec<GarbageRecord>> {
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT b.id,b.chat_id,b.message_id,b.message_date,0,0,NULL FROM telegram_blocks b LEFT JOIN object_blocks ob ON ob.block_id=b.id LEFT JOIN multipart_part_blocks pb ON pb.block_id=b.id WHERE b.state='staged' AND b.created_at<?1 AND ob.block_id IS NULL AND pb.block_id IS NULL",
        )?;
        let rows = stmt.query_map(params![before], |r| {
            Ok(GarbageRecord {
                block_id: r.get(0)?,
                chat_id: r.get(1)?,
                message_id: r.get(2)?,
                message_date: r.get(3)?,
                attempts: r.get(4)?,
                next_attempt: r.get(5)?,
                last_error: r.get(6)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn delete_stale_block(&self, block_id: i64) -> Result<()> {
        let conn = self.lock()?;
        conn.execute(
            "UPDATE telegram_blocks SET ref_count=0 WHERE id=?1 AND state='staged'",
            params![block_id],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO gc_queue(block_id,next_attempt) VALUES(?1,?2)",
            params![block_id, now()],
        )?;
        Ok(())
    }
}

fn upload_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<UploadRecord> {
    let metadata_json: String = row.get(3)?;
    Ok(UploadRecord {
        upload_id: row.get(0)?,
        bucket: row.get(1)?,
        key: row.get(2)?,
        metadata: serde_json::from_str(&metadata_json).unwrap_or_default(),
        kind: row.get(4)?,
        created_at: row.get(5)?,
    })
}

fn load_object_blocks(conn: &Connection, object_id: i64) -> Result<Vec<BlockRef>> {
    let mut stmt = conn.prepare("SELECT b.id,ob.ordinal,ob.byte_offset,ob.size,b.chat_id,b.message_id,b.file_id,b.file_unique_id,b.message_date FROM object_blocks ob JOIN telegram_blocks b ON b.id=ob.block_id WHERE ob.object_id=?1 ORDER BY ob.ordinal")?;
    let rows = stmt.query_map(params![object_id], block_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn load_object_blocks_tx(tx: &Transaction<'_>, object_id: i64) -> Result<Vec<BlockRef>> {
    let mut stmt = tx.prepare("SELECT b.id,ob.ordinal,ob.byte_offset,ob.size,b.chat_id,b.message_id,b.file_id,b.file_unique_id,b.message_date FROM object_blocks ob JOIN telegram_blocks b ON b.id=ob.block_id WHERE ob.object_id=?1 ORDER BY ob.ordinal")?;
    let rows = stmt.query_map(params![object_id], block_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn load_part_blocks(conn: &Connection, upload_id: &str, part_number: i32) -> Result<Vec<BlockRef>> {
    let mut stmt = conn.prepare("SELECT b.id,pb.ordinal,pb.byte_offset,pb.size,b.chat_id,b.message_id,b.file_id,b.file_unique_id,b.message_date FROM multipart_part_blocks pb JOIN telegram_blocks b ON b.id=pb.block_id WHERE pb.upload_id=?1 AND pb.part_number=?2 ORDER BY pb.ordinal")?;
    let rows = stmt.query_map(params![upload_id, part_number], block_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn block_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<BlockRef> {
    Ok(BlockRef {
        id: row.get(0)?,
        ordinal: row.get(1)?,
        offset: row.get(2)?,
        size: row.get(3)?,
        chat_id: row.get(4)?,
        message_id: row.get(5)?,
        file_id: row.get(6)?,
        file_unique_id: row.get(7)?,
        message_date: row.get(8)?,
    })
}

fn decrement_block(tx: &Transaction<'_>, block_id: i64) -> Result<()> {
    tx.execute("UPDATE telegram_blocks SET ref_count=CASE WHEN ref_count>0 THEN ref_count-1 ELSE 0 END WHERE id=?1", params![block_id])?;
    let zero: Option<(i64, i64, i64)> = tx.query_row("SELECT chat_id,message_id,message_date FROM telegram_blocks WHERE id=?1 AND ref_count=0", params![block_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    if zero.is_some() {
        tx.execute(
            "INSERT OR IGNORE INTO gc_queue(block_id,next_attempt,state) VALUES(?1,?2,'pending')",
            params![block_id, now()],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn block(message_id: i64, size: i64) -> BlockRef {
        BlockRef {
            id: 0,
            ordinal: 0,
            offset: 0,
            size,
            chat_id: -100,
            message_id,
            file_id: format!("file-{message_id}"),
            file_unique_id: format!("unique-{message_id}"),
            message_date: now(),
        }
    }

    #[test]
    fn object_commit_copy_delete_and_gc_preserve_references() -> Result<()> {
        let dir = tempdir()?;
        let db = Db::open(&dir.path().join("test.sqlite3"))?;
        assert!(db.create_bucket("bucket")?);

        let upload = UploadRecord {
            upload_id: "upload-1".to_string(),
            bucket: "bucket".to_string(),
            key: "one".to_string(),
            metadata: ObjectMetadata::default(),
            kind: "put".to_string(),
            created_at: now(),
        };
        db.create_upload(&upload)?;
        let staged = block(1, 3);
        let id = db.add_staged_block(&staged)?;
        let staged = BlockRef { id, ..staged };
        let part = PartRecord {
            upload_id: upload.upload_id.clone(),
            part_number: 0,
            size: 3,
            etag: "900150983cd24fb0d6963f7d28e17f72".to_string(),
            blocks: vec![staged.clone()],
        };
        db.replace_part(&upload.upload_id, 0, 3, &part.etag, &part.blocks)?;
        let etag = part.etag.clone();
        db.commit_upload(&upload.upload_id, 3, &etag, &[part])?
            .expect("commit");

        let source = db.get_object("bucket", "one")?.expect("source object");
        assert_eq!(source.blocks.len(), 1);
        db.copy_object(&source, "bucket", "two", &ObjectMetadata::default())?;
        db.delete_object("bucket", "one")?.expect("delete source");
        assert!(db.get_object("bucket", "two")?.is_some());
        assert!(db.gc_candidates(now(), 10)?.is_empty());

        db.delete_object("bucket", "two")?.expect("delete copy");
        let garbage = db.gc_candidates(now(), 10)?;
        assert_eq!(garbage.len(), 1);
        db.gc_success(garbage[0].block_id)?;
        assert!(db.gc_candidates(now(), 10)?.is_empty());
        assert_eq!(db.integrity_check()?, "ok");
        Ok(())
    }
}
