# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /app

COPY Cargo.toml ./
COPY src ./src
COPY migrations ./migrations
COPY openapi.yaml ./openapi.yaml

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin honsemoe-backend-v2 \
    && cp /app/target/release/honsemoe-backend-v2 /tmp/umamoe-backend

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /nonexistent --shell /usr/sbin/nologin app

WORKDIR /app
COPY --from=builder /tmp/umamoe-backend /usr/local/bin/umamoe-backend

ENV HOST=0.0.0.0 \
    PORT=3001 \
    DEBUG_MODE=false

EXPOSE 3001 3201

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${PORT}/api/health" >/dev/null || exit 1

USER app
ENTRYPOINT ["/usr/local/bin/umamoe-backend"]