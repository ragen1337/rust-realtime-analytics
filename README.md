# Real-time analytics pipeline in Rust

An online shop sends events: someone clicked a product, looked at it, bought it. This project takes those events over HTTP, pushes them through Kafka into ClickHouse, and serves analytics on top: top products, a user's activity, conversion, revenue, and live counters.

It handles about 5,000 events per second on a laptop, with a p99 latency under 10 ms. Every service runs in Docker Compose, so one command starts the whole stack.

```
  POST /events ──► ingestion-api ──► Kafka ──► workers ──► ClickHouse ──► analytics-api ──► GET /analytics
                   (validates)                (batches)                  (caches)
```

## How it works

**ingestion-api** accepts an event, validates it, and writes it to a Kafka topic (one topic per event type). It never touches the database, so a slow ClickHouse doesn't slow down clients. On shutdown it finishes open requests and flushes Kafka before exiting.

**workers** read from Kafka and write to ClickHouse in batches. A batch is flushed when it reaches 1,000 events or every 5 seconds, whichever comes first. ClickHouse prefers a few large inserts to many small ones. The Kafka offset is committed only after the insert succeeds, so a crash can cause a duplicate but never a lost event. Duplicates are removed on the ClickHouse side; [docs/data-model.md](docs/data-model.md) explains how. If ClickHouse goes down, a worker stops reading as soon as its batch is full, and the backlog waits in Kafka instead of piling up in memory.

**analytics-api** answers the read queries. Results are cached in memory for 5 seconds to 10 minutes, depending on the endpoint. Top clicked products over a day or a week, and revenue per product, come from materialized views that ClickHouse updates on every insert, so those queries don't scan the raw events.

**events-contract** is a small shared crate with the event types. Ingestion and workers both use it, so they can't disagree on the JSON format.

The stack also includes Kafka UI and Tabix to look inside Kafka and ClickHouse, and Prometheus and Grafana for metrics.

Stack: Rust 2024, actix-web, tokio, rdkafka, clickhouse-rs, moka, Kafka (KRaft), ClickHouse 24.3, Prometheus, Grafana.

## Running it

You need Docker with Compose. Rust is only needed if you want to run the tests or the load generator.

```bash
cp .env.example .env
docker compose up -d --build
```

The first build takes a few minutes, because the three Rust services compile inside Docker. When it's done:

```bash
curl localhost:8080/api/v1/events/health       # OK!
curl localhost:8082/api/v1/analytics/health    # OK!
```

Send a click and read it back:

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

# give the workers up to 5 seconds to flush, then:
curl localhost:8082/api/v1/analytics/realtime-stats
# {"clicks":1,"views":0,"purchases":0}
```

Everything you can open in a browser:

| What | Where |
|---|---|
| Grafana dashboard (no login) | http://localhost:3000 |
| Kafka UI: topics, messages, consumer lag | http://localhost:8081 |
| Tabix: SQL console for ClickHouse (`admin` / `secret`) | http://localhost:8083 |
| Prometheus | http://localhost:9090 |

The ingestion API is on port 8080 and the analytics API on 8082. All settings live in `.env`; the defaults work locally.

## More detail

- [docs/api.md](docs/api.md): every endpoint, the validation rules and the error format
- [docs/data-model.md](docs/data-model.md): the ClickHouse tables, and how duplicates appear and get removed
- [docs/observability.md](docs/observability.md): the metrics and the Grafana dashboard

## Performance

I set myself these targets: ingestion at 3,000 requests/s or more with p99 under 150 ms, analytics at 5,000 requests/s or more with p99 under 100 ms, and over 99% success.

The numbers below were measured with the load generator in this repo, on an Apple M5 laptop (10 cores, 24 GB). Docker had 10 CPUs and 8 GB, and the generator ran on the same machine as the stack.

| Scenario | RPS | p50 | p99 | Success |
|---|---|---|---|---|
| Ingestion, 1,000 rps for 60 s | 1,000 | 5.2 ms | 8.8 ms | 100% |
| Ingestion, 5,000 rps for 5 min | 4,990 | 4.2 ms | 8.6 ms | 100% |
| Analytics `/top-products`, 5,000 rps for 60 s | 4,981 | 0.64 ms | 1.7 ms | 99.99% |
| Analytics `/realtime-stats`, 5,000 rps for 60 s | 4,924 | 0.46 ms | 7.8 ms | 100% |

The HTTP numbers only tell half the story. What matters more is that after the 5-minute stress run (1.5 million events), the workers had zero lag in Kafka, so ClickHouse kept up with the full 5,000 events/s. The p99 of `/realtime-stats` is higher than for `/top-products` because its cache expires every 5 seconds, and the request that hits the expiry goes to ClickHouse.

Without a rate limit, ingestion peaks at about 6,400 requests/s.

To run the load tests yourself (with the stack running):

```bash
cd load-generator
cargo run --release -- --rps 1000 --duration 60                         # ingestion
cargo run --release -- --rps 5000 --duration 300 --concurrency 500      # ingestion, stress
cargo run --release -- --method get --rps 5000 --duration 60 --concurrency 500 \
  --url 'http://localhost:8082/api/v1/analytics/top-products?metric=clicks&period=1h&limit=10'
```

The generator is open-loop: it starts requests at a fixed rate, whether or not earlier ones have finished, so a slow server shows up as higher latency and doesn't quietly lower the load. It is not part of the Cargo workspace, which keeps its HTTP client out of the production dependency tree.

## Tests

```bash
cargo test --workspace
```

The unit tests need no infrastructure. They cover the JSON format of events, validation, batching in the workers, and query parameters and caching in the analytics API.

The end-to-end tests need the running stack. They send an event, wait until it shows up in ClickHouse, and read it back through the analytics API:

```bash
docker compose up -d
cd load-generator && cargo test -- --ignored
```

CI runs formatting, clippy and unit tests on every push to `main` and on pull requests. A separate job starts the whole stack in Docker and runs the end-to-end tests.

## Limitations, and what I'd change for production

This is a demo stack on one machine, and some shortcuts were deliberate:

- **Kafka and ClickHouse are single nodes, without replication.** Losing either one means downtime. In production I'd use replicated Kafka and ClickHouse with `ReplicatedReplacingMergeTree`.
- **Duplicates are removed eventually, not immediately.** Raw tables collapse them on merge, and queries use `FINAL` to see exact counts. The materialized views can over-count after a worker crash. The details are in [docs/data-model.md](docs/data-model.md).
- **A malformed message in Kafka is logged and skipped.** It doesn't block the worker, but it's also gone for good. A dead-letter topic would keep it for inspection.
- **Tables are sorted by user.** That makes one user's history fast, but queries over a time range (last 5 minutes, last hour) scan the whole month's partition. A projection or skip index on `timestamp` would fix that once the data grows.
- **The cache lives inside each analytics-api process.** Run two instances and each keeps its own copy. That's fine at this scale; with more replicas I'd look at a shared cache.
- **Data is up to about 5 seconds behind**, because that's the batch interval, plus whatever the cache holds.
- **There's no auth or rate limiting on ingestion**, and no migration tool: the schema is applied only when ClickHouse starts on an empty volume.

Other next steps: a schema registry for the event format, OpenTelemetry tracing through the whole pipeline, and alerts on consumer lag (the dashboard already shows it).

## Repository layout

```
events-contract/   shared event types and Kafka topic names
ingestion-api/     HTTP → Kafka
workers/           Kafka → batches → ClickHouse
analytics-api/     ClickHouse → cached JSON API
load-generator/    load tester and end-to-end tests (not part of the workspace)
clickhouse/init/   tables and materialized views
observability/     Prometheus config and Grafana dashboard
docs/              detailed documentation
```
