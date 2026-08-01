# tg2s3

S3-compatible object storage backed by a Telegram supergroup or channel.
Object bytes are stored as Telegram documents; SQLite under `./data` stores
metadata, Telegram message references, multipart state, CORS overrides, and
garbage-collection state.

This is a single-process local service. Telegram is not a replicated object
store: back up `./data` and preserve the same Bot identity. Objects are stored
in plaintext in the configured Telegram chat.

## Docker

Copy `.env.example` to `.env`, set the Bot token, chat id, S3 credentials, and
the external endpoint settings, then start the service:

```sh
cp .env.example .env
# edit .env
docker compose up -d --build
```

The container stores its SQLite database in the `tg2s3-data` volume. The
`serve` command automatically creates the directory, applies all SQLx
migrations, checks SQLite integrity, verifies Telegram permissions, creates
the buckets in `TG2S3_INIT_BUCKETS`, and starts the S3 endpoint. No AWS CLI,
rclone, Cloudreve, or `sqlx-cli` is required at runtime.

For a local Rust run:

```sh
set -a
. ./.env
set +a
cargo run -- serve
```

Useful commands are `cargo run -- init`, `cargo run -- check`,
`cargo run -- inspect`, and `cargo run -- gc`.

## Cloudreve

Use the public reverse-proxy URL as the Endpoint, keep the Bucket name equal
to one of `TG2S3_INIT_BUCKETS`, enable forced path-style Endpoint, and use the
same region, Access Key, and Secret Key configured for tg2s3. For example:

```text
Endpoint: https://s3.example.com
Bucket: A
Force path-style: enabled
Region: us-east-1
```

The default CORS policy is intentionally open for browser uploads:

- origins: `*`
- methods: `GET, POST, PUT, DELETE, HEAD`
- request headers: `*`
- exposed headers: `ETag`
- preflight: `OPTIONS`
- max age: `3600`

Cloudreve may call `PutBucketCors`; that configuration is stored as a
Bucket-level override in SQLite. Deleting the override returns to the
environment-variable defaults.

## Compatibility

The implemented surface includes bucket/object CRUD, ListObjects and
ListObjectsV2, metadata, conditional requests, Range GET, CopyObject,
DeleteObjects, multipart upload, SigV4, presigned URLs, CORS, and path-style
routing. `healthz` is a liveness endpoint and `readyz` verifies SQLite
readiness.

Versioning, ACL, tags, lifecycle, Object Lock, notifications, replication,
website hosting, and Select remain intentionally unimplemented and return
standard S3 `NotImplemented` responses.

## Telegram requirements

- Rust toolchain or Docker
- A BotFather bot
- A private Telegram supergroup or channel where the bot is an administrator
- Automatic message deletion disabled in that chat

The bot needs permission to post and delete messages. Public Bot API mode
defaults to 16 MiB chunks because of the public `getFile` download limit. A
colocated local Bot API Server can be selected with
`TG2S3_LOCAL_BOT_API=true` and a custom `TG2S3_TELEGRAM_API_URL`.

## Database migrations

Migration files are kept in the repository-level `migrations/` directory and
are embedded into the binary by SQLx. Production migrations are forward-only;
back up the SQLite volume before upgrading. Existing databases created by
older tg2s3 versions are adopted by the baseline migration without deleting
objects or Telegram references.

## Telegram cleanup caveat

S3 metadata is removed immediately on delete or overwrite. Telegram message
cleanup runs through the background or `gc` queue. Bot API message deletion is
limited by Telegram's message age rules; old messages are marked orphaned
instead of blocking S3 operations.
