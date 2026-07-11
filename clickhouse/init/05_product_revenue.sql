-- Revenue per product: sum of (price * quantity) from purchases.

-- 1) target table
CREATE TABLE IF NOT EXISTS events.product_revenue
(
    product_id    UUID,
    currency      LowCardinality(String),
    revenue_cents AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (product_id, currency);

-- 2) MV trigger: computes revenue on each insert into purchases
CREATE MATERIALIZED VIEW IF NOT EXISTS events.product_revenue_mv
TO events.product_revenue
AS
SELECT
    product_id,
    currency,
    sumState(unit_price_cents * quantity) AS revenue_cents
FROM events.purchases
GROUP BY product_id, currency;
