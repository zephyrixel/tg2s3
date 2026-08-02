# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

tg2s3 is a single-process, S3-compatible object storage service that stores object bytes as Telegram documents. SQLite (under `TG2S3_DATA_DIR`, default `./data`) holds all metadata: buckets, objects, block→Telegram message references, multipart state, CORS overrides, and GC state. Rust edition 2024, minimum Rust 1.85.

## Commands

```sh
cargo build
cargo test                        # all tests; no network needed (Telegram is mocked in-process)
cargo test engine::tests          # one test module
cargo test test_name              # one test by name substring
cargo fmt
cargo clippy

# Local run (config comes from TG2S3_* env vars):
set -a; . ./.env; set +a
cargo run -- serve                # also: init, check (alias of doctor), inspect, gc [--limit N]

# Docker:
docker compose up -d --build
docker compose -f compose.yaml -f compose.local-bot-api.yaml up -d --build   # with local Bot API server
```

`inspect` is the only subcommand that skips Telegram connection/verification; `serve` additionally requires S3 credentials. All configuration is env-driven via clap's `env` feature in `src/config.rs` (see `.env.example` for the full list).

## Architecture

Layering (each layer only calls downward):

```
main.rs (CLI) → bootstrap::prepare() → s3/ (HTTP) → engine/ → { db/, telegram/, transfer/, limits }
```

- **`src/s3.rs` + `src/s3/`** — axum HTTP layer. A single `fallback` handler dispatches on path + query params (path-style routing only; no vhost buckets). SigV4 + presigned-URL verification lives in `src/auth.rs`. XML (de)serialization in `s3/xml.rs`. Unsupported S3 features deliberately return `NotImplemented`.
- **`src/engine.rs` + `src/engine/`** — business logic. `Engine` is the cloneable app core holding `Db`, `TelegramClient`, `Arc<Config>`, `TransferLimits`, per-direction concurrency semaphores, and a GC mutex. A plain PUT is internally modeled as a single-part upload with `upload_id = "put-{uuid}"` and part number 0; multipart uses the same upload/part machinery.
- **`src/transfer/`** — streaming plumbing. Objects are split into fixed-size blocks (`TG2S3_CHUNK_SIZE`), each sent as one Telegram document. Uploads with known `Content-Length` stream through a fixed 1 MiB bounded pipe (`STREAM_BUFFER_SIZE`); unknown-length bodies are spooled to `TG2S3_DATA_DIR/upload-spool` first (Telegram uploads need an exact size). Downloads (`range_stream`) stream Telegram chunks straight into the response — never assemble a whole block or range in memory. Preserving this bounded-memory property is a core design constraint.
- **`src/telegram/`** — `TelegramClient` wraps two backends: `BotApiClient` (default, `bot_api.rs`) and grammers MTProto (`grammers.rs` + `session.rs`). The backend is recorded per stored block (`TelegramBackend` in `model.rs`), so switching backends must not break reads of existing blocks. `bootstrap` enables grammers when it is the configured backend *or* when the database already contains grammers blocks.
- **`src/db.rs` + `src/db/`** — sqlx/SQLite (WAL mode). Migrations live in `migrations/` and are embedded via `sqlx::migrate!`; they are forward-only, and the baseline migration must adopt databases from older versions without data loss.
- **`src/limits.rs`** — global transfer admission (`TG2S3_MAX_ACTIVE_TRANSFERS` → `503 SlowDown` + `Retry-After`), max object size (`413 EntityTooLarge`), and global upload/download byte-rate throttling.

Error convention: engine/db functions `bail!` with S3 error-code strings (`"NoSuchBucket"`, `"NoSuchUpload"`, …) which the s3 layer maps to proper XML error responses.

### Cleanup / GC flow

S3 metadata deletion is immediate; released Telegram messages are deleted best-effort right away by a background task (`Engine::reclaim_blocks` spawns the deletion so requests don't wait on Telegram; `transfer::cleanup_block_refs` covers failed uploads), with failures queued in a GC table. Deletion is batched per backend (Bot API `deleteMessages`, ≤100/call) with per-message fallback. `serve` runs a background GC loop (`TG2S3_GC_INTERVAL`/`TG2S3_GC_LIMIT`) that retries; Bot API messages older than Telegram's deletion age limit get marked orphaned instead of blocking. When a DB write fails after blocks were uploaded, the caller must clean up the just-uploaded blocks (see `engine/object.rs` for the pattern).

## Tests

Tests are `#[cfg(test)]` modules next to their subject (`engine/tests.rs`, `db/tests.rs`, `s3/tests.rs`, `transfer/upload/tests.rs`). Engine/S3 tests spin up an in-process mock Telegram Bot API (an axum server on an ephemeral port) plus a `tempfile` SQLite database — tests are hermetic and don't touch the real Telegram API.

## Repo notes

- `data/` and `tmp/` contain local runtime artifacts (live SQLite DBs, grammers session, scratch files) — not source; don't commit or edit them.
- The README documents operational details worth knowing before changing behavior: local Bot API disk-sizing math, chunk-size defaults per backend (16 MiB Bot API / 64 MiB grammers), reverse-proxy `Host`-preservation requirement for SigV4, and the Telegram cleanup caveats.
