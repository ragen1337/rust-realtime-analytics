-- Views
CREATE TABLE events.views (
    event_id UUID,
    user_id UUID,
    product_id UUID,
    session_id UUID,
    timestamp DateTime64(3),
    duration_ms UInt64,
    referrer Nullable(String),
    date Date DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (user_id, timestamp);
