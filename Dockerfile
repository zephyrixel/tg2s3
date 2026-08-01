FROM rust:1-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /app tg2s3 \
    && mkdir -p /app/data \
    && chown -R tg2s3:tg2s3 /app

COPY --from=builder /build/target/release/tg2s3 /usr/local/bin/tg2s3

WORKDIR /app
USER tg2s3
ENV TG2S3_DATA_DIR=/app/data \
    TG2S3_DB_PATH=/app/data/tg2s3.sqlite3 \
    TG2S3_LISTEN=0.0.0.0:9000
VOLUME ["/app/data"]
EXPOSE 9000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:9000/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/tg2s3"]
CMD ["serve"]
