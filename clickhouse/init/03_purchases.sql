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
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (user_id, timestamp);
