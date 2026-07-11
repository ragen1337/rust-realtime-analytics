FROM rust:1.96-slim-bookworm AS builder

WORKDIR /app

# Native build deps: the clickhouse crate's lz4 feature needs a C compiler.
# Installed only in the throwaway builder stage.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY events-contract/Cargo.toml events-contract/Cargo.toml
COPY ingestion-api/Cargo.toml ingestion-api/Cargo.toml
COPY workers/Cargo.toml workers/Cargo.toml
COPY analytics-api/Cargo.toml analytics-api/Cargo.toml

RUN mkdir -p events-contract/src ingestion-api/src workers/src analytics-api/src \
    && echo "" > events-contract/src/lib.rs \
    && echo "fn main() {}" > ingestion-api/src/main.rs \
    && echo "fn main() {}" > workers/src/main.rs \
    && echo "fn main() {}" > analytics-api/src/main.rs \
    && cargo build --release -p analytics-api \
    && rm -rf events-contract/src analytics-api/src

COPY events-contract/src events-contract/src
COPY analytics-api/src analytics-api/src
RUN touch analytics-api/src/main.rs && cargo build --release -p analytics-api

# --- runtime stage ---
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/analytics-api .

# Run as an unprivileged user; the service only needs to listen on 8080.
RUN useradd --system --no-create-home app
USER app

ENV RUST_LOG=info

CMD ["./analytics-api"]
