-- ReplacingMergeTree: a background merge collapses rows with an identical sort key,
-- so events re-delivered by the at-least-once pipeline (worker crash between insert
-- and offset commit, rebalance, retry after a timeout, partial flush, client POST retry)
-- end up stored once. event_id is in ORDER BY because it is the dedup key; duplicates of
-- one event share user_id+timestamp, so user_id stays the leading key for per-user reads.
-- Dedup is EVENTUAL: until a merge runs, count() can include duplicates (use FINAL).
-- Purchases
CREATE TABLE events.purchases (
    event_id UUID,
    user_id UUID,
    product_id UUID,
    session_id UUID,
    timestamp DateTime64(3),
    order_id UUID,
    quantity UInt32,
    unit_price_cents UInt64,
    currency LowCardinality(String),
    payment_method LowCardinality(String),
    date Date DEFAULT toDate(timestamp)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (user_id, timestamp, event_id)
SETTINGS non_replicated_deduplication_window = 1000;
