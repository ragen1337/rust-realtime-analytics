# Data model and delivery guarantees

The SQL lives in `clickhouse/init/`. ClickHouse runs it once, when its volume is empty. If you change the schema, recreate the volume with `docker compose down -v` and start the stack again.

## Tables

There is one table per event type: `clicks`, `views` and `purchases`. All three:

- are partitioned by month;
- are sorted by `(user_id, timestamp, event_id)`, so reading one user's history is fast;
- use the `ReplacingMergeTree` engine. When ClickHouse merges data parts in the background, rows with the same sort key collapse into one. Since a duplicated event has the same user, timestamp and `event_id`, duplicates disappear on merge.

Two materialized views keep running totals up to date as rows are inserted:

- `top_products_hourly`: clicks per product per hour. `/top-products` uses it for clicks over 24h and 7d.
- `product_revenue`: revenue per product and currency. `/product-revenue` reads it.

## Where duplicates come from

The pipeline is at-least-once. Once ingestion has answered `200`, the event will reach ClickHouse, but it can occasionally arrive twice:

| What happens | Result |
|---|---|
| A worker dies after writing a batch to ClickHouse but before committing the Kafka offset | Another worker reads the same messages again and writes them again |
| Kafka rebalances the consumer group | Same as above: uncommitted messages are re-delivered |
| A ClickHouse insert times out on the client side but actually succeeded, and the worker retries | The retry sends exactly the same block of rows |
| A flush writes clicks, then fails on views | The offset isn't committed, so the whole batch, clicks included, is written again |
| A client retries its HTTP request with the same `event_id` | Two copies go through Kafka |

## How they're handled

The exact-retry case is caught on insert. The tables have `non_replicated_deduplication_window = 1000`, so ClickHouse drops a block that is byte-for-byte identical to one it recently accepted. The materialized view tables have the same setting, which keeps a retried block from being counted twice there as well.

Every other case produces a batch that looks different, so insert-time dedup doesn't catch it. Those duplicates are stored and later collapsed by the merge. Until that happens, a plain `count()` sees them. `FINAL` makes ClickHouse merge at read time instead:

```sql
-- the same event inserted in two separate batches
SELECT count() FROM events.clicks;          -- 2, not merged yet
SELECT count() FROM events.clicks FINAL;    -- 1
OPTIMIZE TABLE events.clicks FINAL;         -- force the merge
SELECT count() FROM events.clicks;          -- 1
```

`FINAL` costs extra CPU, so it only makes sense on queries bounded by time or by user. The analytics API uses it for every read from the raw tables.

The materialized views are the weak spot. They add up every inserted block and don't store `event_id`, so a duplicate that slips through gets counted in them, and merges don't undo it. For the views' purpose (top products and revenue totals), a rare over-count after a worker crash is an acceptable error. Counts that must be exact should come from the raw tables with `FINAL`.

All of this was checked on ClickHouse 24.3. The same block inserted twice is stored once, both in the table and in the view. The same event inserted in two different batches shows `count() = 2` until the merge, and the view counts it twice.
