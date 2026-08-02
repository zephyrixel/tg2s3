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

### Local Telegram Bot API with Compose

The optional `compose.local-bot-api.yaml` overlay builds the Bot API server
from the upstream Telegram source and runs it only on the Compose network. It
mounts the Bot API working directory at the same absolute path in both
containers because local `getFile` responses contain absolute file paths.

Set `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` in `.env`, log the bot out from
the public Bot API once, then start the overlay:

```sh
docker compose -f compose.yaml -f compose.local-bot-api.yaml up -d --build
docker compose -f compose.yaml -f compose.local-bot-api.yaml logs -f telegram-bot-api tg2s3
```

The overlay sets `TG2S3_LOCAL_BOT_API=true` and uses
`http://telegram-bot-api:8081`. Port 8081 is intentionally not published to
the host or Cloudflare. Its health check calls `getMe`, so tg2s3 waits for the
Bot API server to accept authenticated requests. `TELEGRAM_BOT_API_REF` pins
the upstream source ref; update it deliberately and rebuild the image when
upgrading.

The `telegram-bot-api-data` volume is a separate disk budget from
`tg2s3-data`. Local `getFile` may prepare complete files there, so size it for
the largest concurrent downloads plus the files retained by the Bot API; the
tg2s3 garbage collector does not prune this volume.

#### Local Bot API disk sizing

In local mode, `getFile` asks the Bot API server to download the complete
Telegram document and returns an absolute local path. tg2s3 then reads only
the requested range from that path. A Cloudreve range request can therefore
consume one complete tg2s3 block on the Bot API volume even when the client
requested only a few bytes.

Let `C` be `TG2S3_CHUNK_SIZE`, `D` be
`TG2S3_DOWNLOAD_CONCURRENCY`, and `U` be `TG2S3_UPLOAD_CONCURRENCY`:

```text
transient working set ~= C * (D + U)
recommended volume ~= (unique block bytes likely retained locally
                       + transient working set) * 1.25
```

The first term is the important one. It is not bounded by `C` or by the
concurrency settings, because tg2s3 does not control Bot API/TDLib file
eviction. For the default `16 MiB`, `D=4`, and `U=4`, reserve about `128 MiB`
for transient transfers, then add the unique data expected to be read through
the local Bot API. For example, fully reading a 4 GiB object normally makes
roughly 4 GiB of its blocks eligible for the local working directory, so a
5-6 GiB volume is a practical minimum for that workload. If 100 GiB of unique
objects may be read and retained, plan for at least 125 GiB and monitor the
volume. Keep additional free space for SQLite/TDLib metadata and filesystem
overhead.

Check the current usage with:

```sh
docker compose -f compose.yaml -f compose.local-bot-api.yaml \
  exec telegram-bot-api du -sh /var/lib/telegram-bot-api
```

Do not remove files from this volume while the Bot API server is running. To
reclaim all local copies, stop the stack and remove/recreate only the
`telegram-bot-api-data` volume after accepting that subsequent reads will
download the blocks again.

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

Set `RUST_LOG=info` to see S3 request and response status logs. Authentication
failures include the request path, Host, signed-header list, and payload hash
diagnostics without logging credentials or presigned query strings. If a
reverse proxy is used, it must preserve the original `Host` header used by
Cloudreve for SigV4 signing; do not mount tg2s3 under a URL path prefix.

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
