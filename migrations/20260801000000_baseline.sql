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
