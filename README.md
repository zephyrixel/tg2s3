# tg2s3

S3-compatible object storage backed by a Telegram supergroup or channel.
Object bytes are uploaded as Telegram documents. SQLite under `./data` stores
the object index, Telegram message references, multipart state, and garbage
collection state.

This is a single-process local storage service. Telegram is not a substitute
for a replicated object store: keep backups of `./data` and preserve the same
Bot identity. Objects are stored in plaintext in the configured Telegram chat.

## Requirements

- Rust toolchain
- A BotFather bot
- A private Telegram supergroup or channel where the bot is an administrator
- Automatic message deletion disabled in that chat

The bot needs permission to post messages and delete messages. The service
checks these requirements during `serve` and `doctor` startup.

Public Bot API mode defaults to 16 MiB chunks because the public `getFile`
download limit is lower than the upload limit. A colocated local Bot API Server
can be selected with `TG2S3_LOCAL_BOT_API=true` and a custom
`TG2S3_TELEGRAM_API_URL`.

## Run

```sh
cp .env.example .env
# edit .env, then export the values for your shell
set -a
. ./.env
set +a

cargo run -- serve
```

The default S3 endpoint is `http://127.0.0.1:9000`. Anonymous mode is intended
only for a local endpoint. For a shared endpoint, configure both
`TG2S3_ACCESS_KEY` and `TG2S3_SECRET_KEY`; requests then require AWS SigV4.

Useful commands:

```sh
cargo run -- doctor
cargo run -- inspect
cargo run -- gc
```

## S3 examples

```sh
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./file.bin s3://demo/file.bin
aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://demo/file.bin ./downloaded.bin
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://demo/
```

The implemented compatibility surface includes bucket/object CRUD,
ListObjects/ListObjectsV2, metadata, conditional requests, Range GET,
CopyObject, multi-delete, multipart upload, SigV4, presigned URLs, and both
path-style and configured virtual-hosted-style routing.

Versioning, ACL, tags, lifecycle, Object Lock, notifications, replication,
website hosting, and Select are intentionally not implemented and return a
standard S3 `NotImplemented` response.

## Telegram cleanup caveat

S3 metadata is removed immediately on delete or overwrite. Telegram message
cleanup runs through the background/`gc` queue. Bot API message deletion is
limited by Telegram's message age rules; messages outside that window are
marked as orphaned and retained rather than blocking S3 operations.
