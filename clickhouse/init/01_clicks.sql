-- Clicks
CREATE TABLE events.clicks (
    event_id UUID,
    user_id UUID,
    product_id UUID,
    session_id UUID,
    timestamp DateTime64(3),
    source String,
    position UInt32,
    category String,
    date Date DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (user_id, timestamp);
