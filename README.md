# Real-Time Analytics Pipeline

A high-load event analytics system in Rust: HTTP ingestion → Kafka → batch workers → ClickHouse → analytics API.

Product interaction events (clicks, views, purchases) are ingested over HTTP, streamed through Kafka, batch-written into ClickHouse by a worker pool, and served back as aggregated analytics with caching and materialized views.

## Architecture

```
                 ┌──────────────┐   HTTP    ┌───────────────┐
   clients  ───► │ ingestion-api │ ───────► │     Kafka     │
                 │  (actix-web)  │  produce  │    (KRaft)    │
                 └──────────────┘           └───────┬───────┘
                                                    │ consume (group)
                                            ┌───────▼───────┐
                                            │    workers    │
                                            │ batch + retry │
                                            └───────┬───────┘
                                                    │ batch insert
                                            ┌───────▼───────┐
   clients  ◄─── analytics-api (actix-web) ◄│  ClickHouse   │
              cache + materialized views    │  (MergeTree)  │
                                            └───────────────┘
```

## Services

| Service | Stack | Role |
|---|---|---|
| `ingestion-api` | actix-web, rdkafka | Accepts events over HTTP, validates, produces to Kafka. Graceful drain + Kafka flush on shutdown. |
| `workers` | rdkafka, tokio, clickhouse | Consumer-group pool (`min(cpus, partitions)`). Batches by size (1000) **or** time (5s), inserts into ClickHouse, commits offset after write (at-least-once). Retries inserts with backoff. |
| `analytics-api` | actix-web, clickhouse, moka | Read API. Per-endpoint in-memory cache (TTL depends on period), 30s query timeout, reads materialized views where available. |
| `events-contract` | serde | Shared event model + Kafka topic names (single source of truth for both producer and consumers). |

Infrastructure: **Kafka** (KRaft, single node), **ClickHouse** (24.3), plus **Kafka UI** and **Tabix** for inspection. A one-shot `events-kafka-init` container pre-creates the topics before the workers start (a consumer subscribed to a not-yet-existing topic would otherwise wait for a metadata refresh — up to 5 minutes — before seeing it).

## Tech stack

Rust (edition 2024), actix-web 4, rdkafka 0.39, clickhouse 0.15, tokio, moka (cache), tracing (logging), Docker Compose.

## Prerequisites

- Docker + Docker Compose
- (local dev only) Rust toolchain, and `cmake`/`libsasl2` for rdkafka native build

## Quick start

**1. Configure** (defaults work out of the box for local):

```bash
cp .env.example .env
```

**2. Build and start the full stack:**

```bash
docker compose up -d --build
```

First boot takes a few minutes: the three Rust services compile inside Docker, then start in dependency order (Kafka healthy → topics pre-created by `events-kafka-init` → workers; ClickHouse healthy → APIs). On first boot ClickHouse also runs the SQL in `clickhouse/init/` (database, tables, materialized views) — this only happens on an **empty** volume. To re-run migrations after changing them: `docker compose down -v` (drops the volume) then `up` again.

**3. Check that everything is up:**

```bash
docker compose ps                              # all services Up, kafka/clickhouse healthy
curl localhost:8080/api/v1/events/health       # ingestion  -> OK!
curl localhost:8082/api/v1/analytics/health    # analytics  -> OK!
```

**4. Send a test event and see it come back:**

```bash
curl -X POST localhost:8080/api/v1/events/click \
  -H 'content-type: application/json' \
  -d "{
    \"user_id\": \"11111111-1111-1111-1111-111111111111\",
    \"product_id\": \"22222222-2222-2222-2222-222222222222\",
    \"session_id\": \"33333333-3333-3333-3333-333333333333\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"metadata\": {\"source\": \"search\", \"position\": 3, \"category\": \"electronics\"}
  }"
# -> {"status":"ok"}

# workers flush at most every 5s; then (counts events of the last 5 minutes):
curl "localhost:8082/api/v1/analytics/realtime-stats"
# -> {"clicks":1,"views":0,"purchases":0}
```

To watch the pipeline live: Kafka UI (http://localhost:8081) shows topics/messages/consumer lag, Tabix (http://localhost:8083, login `admin`/`secret`) runs SQL against ClickHouse.

### Ports (host)

| Service | URL |
|---|---|
| ingestion-api | http://localhost:8080 |
| analytics-api | http://localhost:8082 |
| Kafka UI | http://localhost:8081 |
| ClickHouse (HTTP) | http://localhost:8123 |
| Tabix (ClickHouse UI) | http://localhost:8083 |

## Configuration

All config is via environment variables (see `.env.example`). Key ones:

| Variable | Purpose |
|---|---|
| `KAFKA_BROKERS` | Kafka bootstrap servers (in-cluster: `events-kafka:29092`) |
| `KAFKA_NUM_PARTITIONS` | partitions per topic; caps the worker count |
| `CLICKHOUSE_URL` | ClickHouse HTTP endpoint (`http://events-clickhouse:8123`) |
| `CLICKHOUSE_DB` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | ClickHouse credentials |
| `RUST_LOG` | log level, e.g. `info`, `debug`, `analytics_api=debug,info` |

## API

### Ingestion — `POST /api/v1/events/{click|view|purchase}`

Common fields for every event type: `user_id`, `product_id`, `session_id`, `timestamp` (RFC3339). `event_id` is optional (generated server-side if omitted). It is the dedup key of the raw tables, not an idempotency key: a repeated `POST` with the same `event_id` is accepted again, and the duplicate is collapsed later in ClickHouse (see [Delivery semantics](#delivery-semantics)).

```bash
# click: + metadata {source, position, category}
curl -X POST localhost:8080/api/v1/events/click \
  -H 'content-type: application/json' \
  -d '{
    "user_id": "11111111-1111-1111-1111-111111111111",
    "product_id": "22222222-2222-2222-2222-222222222222",
    "session_id": "33333333-3333-3333-3333-333333333333",
    "timestamp": "2026-07-06T14:00:00Z",
    "metadata": {"source": "search", "position": 3, "category": "electronics"}
  }'
# -> 200 {"status":"ok"}

# view: + duration_ms, optional referrer
curl -X POST localhost:8080/api/v1/events/view \
  -H 'content-type: application/json' \
  -d '{
    "user_id": "11111111-1111-1111-1111-111111111111",
    "product_id": "22222222-2222-2222-2222-222222222222",
    "session_id": "33333333-3333-3333-3333-333333333333",
    "timestamp": "2026-07-06T14:00:00Z",
    "duration_ms": 4500,
    "referrer": "https://example.com/search"
  }'

# purchase: + order_id, quantity, unit_price_cents, currency,
#             payment_method (card|paypal|apple_pay|google_pay)
curl -X POST localhost:8080/api/v1/events/purchase \
  -H 'content-type: application/json' \
  -d '{
    "user_id": "11111111-1111-1111-1111-111111111111",
    "product_id": "22222222-2222-2222-2222-222222222222",
    "session_id": "33333333-3333-3333-3333-333333333333",
    "timestamp": "2026-07-06T14:00:00Z",
    "order_id": "44444444-4444-4444-4444-444444444444",
    "quantity": 2,
    "unit_price_cents": 1999,
    "currency": "USD",
    "payment_method": "card"
  }'
```

### Analytics — `GET /api/v1/analytics/...`

| Endpoint | Query params | Description |
|---|---|---|
| `/top-products` | `metric=clicks\|views\|purchases`, `period=1h\|24h\|7d`, `limit` | Top products by metric (clicks served from a materialized view) |
| `/user-activity/{user_id}` | `from`, `to` (RFC3339), `limit`, `offset` | A user's events across all types |
| `/conversion-rate` | `from`, `to` | purchases / views over the range |
| `/realtime-stats` | — | Event counters for the last 5 minutes |
| `/product-revenue` | `limit` | Revenue per product/currency (from a materialized view) |

```bash
curl "localhost:8082/api/v1/analytics/top-products?metric=clicks&period=1h&limit=10"
# -> [{"product_id":"2222...","metric_value":42}, ...]

curl "localhost:8082/api/v1/analytics/user-activity/11111111-1111-1111-1111-111111111111?from=2026-07-06T00:00:00Z&to=2026-07-06T23:59:59Z"
# -> [{"event_type":"click","event_id":"aaaa...","product_id":"2222...","timestamp":"2026-07-06T14:00:00Z"}, ...]

curl "localhost:8082/api/v1/analytics/conversion-rate?from=2026-07-06T00:00:00Z&to=2026-07-06T23:59:59Z"
# -> {"views":1200,"purchases":48,"conversion_rate":0.04}

curl "localhost:8082/api/v1/analytics/realtime-stats"
# -> {"clicks":150,"views":90,"purchases":12}

curl "localhost:8082/api/v1/analytics/product-revenue?limit=10"
# -> [{"product_id":"2222...","currency":"USD","revenue_cents":399800}, ...]
```

Responses are cached per endpoint (moka): TTL 60s–600s depending on the period, 5s for `realtime-stats`. On a cache miss only one request runs the ClickHouse query; concurrent requests for the same key await its result (dog-pile protection).

Errors use a uniform envelope: `{"error": "..."}` with status `400` (bad params), `404` (unknown route), `503` (ClickHouse unavailable/timeout).

## Data model

ClickHouse database `events` (see `clickhouse/init/`):

- **Tables** `clicks`, `views`, `purchases` — `ReplacingMergeTree`, partitioned by month, `ORDER BY (user_id, timestamp, event_id)`. Rows with an identical sort key are collapsed by background merges, so `event_id` is the dedup key; duplicates of one event share `user_id` and `timestamp`, and `user_id` stays the leading key for per-user reads.
- **Materialized views**:
  - `top_products_hourly` — hourly click counts per product (`AggregatingMergeTree`), backs `/top-products?metric=clicks`.
  - `product_revenue` — revenue per product/currency, backs `/product-revenue`.
  - The MVs do **not** dedup: they aggregate every inserted block and hold no `event_id`, so an event re-delivered in a different batch is counted twice in them permanently (merges do not undo it).
- **Reading duplicates.** Dedup is *eventual*: until a background merge touches the affected parts, plain `count()` and `SELECT` on the raw tables can include duplicates. Use `FINAL` (`SELECT count() FROM events.clicks FINAL`) for exact results; it merges at read time, which costs CPU on large scans, so keep it on time- or user-bounded queries. `analytics-api` uses `FINAL` for all raw-table reads (`views`/`purchases` top products, user activity, conversion, realtime stats); it does not for the MV reads.

  ```sql
  -- same event inserted twice in two separate inserts
  SELECT count() FROM events.clicks;         -- 2  (not merged yet)
  SELECT count() FROM events.clicks FINAL;   -- 1
  OPTIMIZE TABLE events.clicks FINAL;        -- force a merge
  SELECT count() FROM events.clicks;         -- 1
  ```

  The schema is only applied on an empty volume, so an existing stack needs `docker compose down -v` first (see [Quick start](#quick-start)).

### Delivery semantics

The pipeline is **at-least-once**: an event is never lost after ingestion acknowledged it, but it can be stored more than once. Sources of duplicates and how each is handled:

| Source | Handling |
|---|---|
| Worker crash between the ClickHouse insert and the Kafka offset commit | Batch is re-read and re-inserted; collapsed by `ReplacingMergeTree` (eventually), not in MVs |
| Consumer-group rebalance (uncommitted offsets go to another worker) | Same as above |
| `insert_batch` retry after an ambiguous failure (insert landed, client saw a timeout) | The retry sends the identical block, which ClickHouse drops at insert time (`non_replicated_deduplication_window = 1000` on the tables and MV targets); also covered by `ReplacingMergeTree` |
| Partial flush: clicks written, views failed, offset not committed | Whole batch is re-read; the clicks part is a duplicate, collapsed as above |
| Client retries the HTTP `POST` with the same `event_id` | Two Kafka messages, two rows; collapsed by `ReplacingMergeTree` (eventually), not in MVs |

Insert-time dedup only applies to byte-identical blocks; a re-read batch usually has different composition, so it relies on the merge-time dedup. Verified on ClickHouse 24.3: an identical block inserted twice is stored once in the table and once in the MV target, but the same event inserted in a different block gives `count()` = 2 until a merge (or `FINAL`), and the MVs count it twice.

## Local development

```bash
cargo build --workspace
cargo check -p analytics-api      # per-crate
cargo run -p ingestion-api        # needs Kafka/ClickHouse reachable (e.g. via compose)
```

## Testing

**Unit tests** (no infrastructure needed) cover the critical logic: the JSON wire
contract in `events-contract` (flattened context, `event_id` generation,
`payment_method` naming), the workers' batching/routing (`Batcher::accept`,
batch-size trigger, event→row mapping), and the analytics parameter layer
(query parsing, cache-key normalization, per-period TTLs).

```bash
cargo test --workspace
```

**End-to-end tests** live in `load-generator/tests/` and run against the real
stack: an ingested event is tracked through Kafka and the workers into
ClickHouse, then read back through the analytics API. They are `#[ignore]`d so
a plain `cargo test` stays green without Docker.

```bash
docker compose up -d
cd load-generator
cargo test -- --ignored
```

## Project structure

```
events-contract/   # shared event model + Kafka topic names
ingestion-api/     # HTTP -> Kafka producer
workers/           # Kafka consumer -> batch -> ClickHouse
analytics-api/     # ClickHouse -> cached JSON API
load-generator/    # dev-only HTTP load tester (standalone, outside the workspace)
clickhouse/init/   # SQL migrations (tables + materialized views)
compose.yaml       # full stack
.env.example       # configuration template
```

## Load testing

`load-generator/` is a small open-loop async HTTP load tester: a `tokio`
interval paces request *starts* at `--rps`, a semaphore caps requests in flight
at `--concurrency`, and every request's latency is recorded to report achieved
RPS, success rate and p50/p99 percentiles.

It is deliberately **not** a workspace member: it pulls a full HTTP/TLS stack
(`reqwest`) that has no business in the production crates' dependency graph or
the shared `Cargo.lock`. It builds and runs standalone, and never ships in any
Docker image.

```bash
# the stack must be up first (docker compose up -d)
cd load-generator

# ingestion baseline: POST randomized click events (the default --url/--method)
cargo run --release -- --rps 1000 --duration 60

# ingestion stress
cargo run --release -- --rps 5000 --duration 300 --concurrency 500

# analytics: GET any endpoint, query params go straight in the URL
cargo run --release -- --method get \
  --url 'http://localhost:8082/api/v1/analytics/top-products?metric=clicks&period=1h&limit=10' \
  --rps 5000 --duration 60 --concurrency 500
```

Results are collected in the [Performance](#performance) section below.

## Performance

Design targets: ingestion ≥ 3000 RPS, analytics ≥ 5000 RPS,
ingestion p99 < 150 ms, analytics p99 < 100 ms, success rate > 99%.

Measured with `load-generator` (open-loop, paced) against the full Docker
Compose stack. Environment: Apple M5 (10 cores, 24 GB RAM), Docker VM with
10 CPUs / 8 GB; generator and stack on the same machine.

| Scenario | Requests | RPS | p50 | p99 | Success |
|---|---|---|---|---|---|
| ingestion baseline — `POST /events/click`, 1000 rps / 60 s / conc 50 | 60 000 | **1000** | 5.2 ms | **8.8 ms** | 100.00% |
| ingestion stress — `POST /events/click`, 5000 rps / 300 s / conc 500 | 1 500 000 | **4990** | 4.2 ms | **8.6 ms** | 100.00% |
| analytics — `GET /top-products` (cached), 5000 rps / 60 s / conc 500 | 300 000 | **4981** | 0.64 ms | **1.7 ms** | 99.99% |
| analytics — `GET /realtime-stats` (5 s TTL), 5000 rps / 60 s / conc 500 | 300 000 | **4924** | 0.46 ms | **7.8 ms** | 100.00% |

All targets are exceeded with a wide margin: ingestion p99 is ~17× below the
150 ms budget at 5000 rps sustained for 5 minutes; analytics p99 stays under
8 ms thanks to the moka response cache (the p99 spikes on `realtime-stats`
correspond to its 5-second TTL expiring — those requests pay the real
ClickHouse round-trip).

The write path kept up end-to-end: after the 1.5 M-event stress run the
`events-workers` consumer group finished with **lag 0** on every partition,
i.e. Kafka → workers → ClickHouse sustained the full 5000 events/s, not just
the HTTP front.

Peak unthrottled throughput (no `--rps` pacing, closed-loop at concurrency 50)
measured ~6400 RPS on the ingestion API.
