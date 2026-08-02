FROM alpine:3.22 AS builder

ARG TELEGRAM_BOT_API_REF=adfd7f6a8e990272851777eeb3ae0def4216f161

RUN apk add --no-cache \
    alpine-sdk \
    cmake \
    git \
    gperf \
    linux-headers \
    openssl-dev \
    zlib-dev

WORKDIR /usr/src/telegram-bot-api
RUN git clone --recursive --depth 1 \
      https://github.com/tdlib/telegram-bot-api.git . \
 && git fetch --depth 1 origin "${TELEGRAM_BOT_API_REF}" \
 && git checkout --detach "${TELEGRAM_BOT_API_REF}" \
 && git submodule update --init --recursive

RUN cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
 && cmake --build build --target install -j "$(nproc)" \
 && strip /usr/local/bin/telegram-bot-api

FROM alpine:3.22

RUN apk add --no-cache ca-certificates openssl libstdc++ wget zlib \
 && addgroup -S -g 101 telegram-bot-api \
 && adduser -S -D -H -u 101 -G telegram-bot-api \
      -h /var/lib/telegram-bot-api telegram-bot-api \
 && mkdir -p /var/lib/telegram-bot-api \
 && chown telegram-bot-api:telegram-bot-api /var/lib/telegram-bot-api

COPY --from=builder /usr/local/bin/telegram-bot-api /usr/local/bin/telegram-bot-api

USER telegram-bot-api
WORKDIR /var/lib/telegram-bot-api
VOLUME ["/var/lib/telegram-bot-api"]
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/telegram-bot-api"]
