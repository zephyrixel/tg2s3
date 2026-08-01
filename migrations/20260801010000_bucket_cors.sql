CREATE TABLE IF NOT EXISTS bucket_cors (
    bucket TEXT PRIMARY KEY,
    configuration_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
