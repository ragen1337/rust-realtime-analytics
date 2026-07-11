-- Top products by hour: pre-aggregated clicks per (hour, product).
-- Populated incrementally by a trigger on every insert into events.clicks.

-- 1) Target table: stores PARTIAL aggregate STATES (not final numbers).
CREATE TABLE IF NOT EXISTS events.top_products_hourly
(
    hour       DateTime,
    product_id UUID,
    clicks     AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, product_id);

-- 2) MV trigger: on each insert into clicks, aggregates the new block into the target table.
CREATE MATERIALIZED VIEW IF NOT EXISTS events.top_products_hourly_mv
TO events.top_products_hourly
AS
SELECT
    toStartOfHour(timestamp) AS hour,
    product_id,
    countState() AS clicks
FROM events.clicks
GROUP BY hour, product_id;
