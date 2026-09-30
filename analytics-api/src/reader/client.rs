use chrono::{DateTime, Utc};
use clickhouse::Client;
use uuid::Uuid;

use crate::reader::rows::{
    ConversionRow, ProductRevenueRow, RealtimeStatsRow, TopProductRow, UserActivityRow,
};

/// chrono's Serialize renders `DateTime<Utc>` as RFC3339 ("2026-07-11T14:41:34Z"),
/// which ClickHouse cannot cast to DateTime/DateTime64 in a WHERE comparison.
/// Bind this canonical "YYYY-MM-DD hh:mm:ss" form instead (UTC, second precision —
/// enough for analytics range filters).
fn to_ch_datetime(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Raw-table reads use `FINAL` so events re-delivered by the at-least-once pipeline are
/// counted once even before a background merge has collapsed them (ReplacingMergeTree
/// dedup is eventual). Cost: FINAL merges rows at read time; ClickHouse parallelizes it
/// and every query here is time- or user-bounded, so it stays acceptable. The MV reads
/// (`top_products_hourly`, `product_revenue`) cannot use it: they hold aggregate states
/// with no event_id, so they may over-count re-delivered events.
#[derive(Clone)]
pub struct ChReader {
    client: Client,
}

impl ChReader {
    pub fn new() -> Self {
        let url =
            std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
        let database = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "events".to_string());
        let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "admin".to_string());
        let password =
            std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "secret".to_string());

        let client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(user)
            .with_password(password)
            // 30s budget for ClickHouse queries. The server aborts the query itself;
            // the error arrives as clickhouse::Error -> From -> 503.
            .with_setting("max_execution_time", "30");

        Self { client }
    }

    /// Top products via a raw table scan (`table` comes from `Metric::table()`).
    /// Slow at scale — used for metrics without an MV (views/purchases).
    pub async fn top_products_raw(
        &self,
        table: &str,
        since: DateTime<Utc>,
        limit: u64,
    ) -> clickhouse::error::Result<Vec<TopProductRow>> {
        self.client
            .query(&format!(
                "SELECT product_id, count() AS metric_value \
                 FROM {table} FINAL \
                 WHERE timestamp >= ? \
                 GROUP BY product_id \
                 ORDER BY metric_value DESC \
                 LIMIT ?"
            ))
            .bind(to_ch_datetime(since))
            .bind(limit)
            .fetch_all::<TopProductRow>()
            .await
    }

    /// Top products by clicks from the pre-aggregate (MV `top_products_hourly`).
    /// countMerge finalizes the partial states; filtering is done on hourly buckets.
    pub async fn top_products_hourly(
        &self,
        since: DateTime<Utc>,
        limit: u64,
    ) -> clickhouse::error::Result<Vec<TopProductRow>> {
        self.client
            .query(
                "SELECT product_id, countMerge(clicks) AS metric_value \
                 FROM top_products_hourly \
                 WHERE hour >= ? \
                 GROUP BY product_id \
                 ORDER BY metric_value DESC \
                 LIMIT ?",
            )
            .bind(to_ch_datetime(since))
            .bind(limit)
            .fetch_all::<TopProductRow>()
            .await
    }

    /// User event feed for [from, to] — a union of three tables.
    /// The `?` placeholders are positional: bind strictly in order of appearance.
    pub async fn user_activity(
        &self,
        user_id: Uuid,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u64,
        offset: u64,
    ) -> clickhouse::error::Result<Vec<UserActivityRow>> {
        self.client
            .query(
                "SELECT 'click' AS event_type, event_id, product_id, timestamp \
                 FROM clicks FINAL WHERE user_id = ? AND timestamp BETWEEN ? AND ? \
                 UNION ALL \
                 SELECT 'view' AS event_type, event_id, product_id, timestamp \
                 FROM views FINAL WHERE user_id = ? AND timestamp BETWEEN ? AND ? \
                 UNION ALL \
                 SELECT 'purchase' AS event_type, event_id, product_id, timestamp \
                 FROM purchases FINAL WHERE user_id = ? AND timestamp BETWEEN ? AND ? \
                 ORDER BY timestamp DESC \
                 LIMIT ? OFFSET ?",
            )
            .bind(user_id)
            .bind(to_ch_datetime(from))
            .bind(to_ch_datetime(to))
            .bind(user_id)
            .bind(to_ch_datetime(from))
            .bind(to_ch_datetime(to))
            .bind(user_id)
            .bind(to_ch_datetime(from))
            .bind(to_ch_datetime(to))
            .bind(limit)
            .bind(offset)
            .fetch_all::<UserActivityRow>()
            .await
    }

    /// Conversion over a period: views, purchases and their ratio (0 when views = 0).
    pub async fn conversion_rate(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> clickhouse::error::Result<ConversionRow> {
        self.client
            .query(
                // assumeNotNull: a scalar subquery is typed Nullable(UInt64), which
                // does not deserialize into the row's plain u64. count() never
                // returns NULL, so the assumption is safe.
                "SELECT views, purchases, if(views = 0, 0, purchases / views) AS conversion_rate \
                 FROM ( \
                     SELECT \
                         assumeNotNull((SELECT count() FROM views FINAL WHERE timestamp BETWEEN ? AND ?)) AS views, \
                         assumeNotNull((SELECT count() FROM purchases FINAL WHERE timestamp BETWEEN ? AND ?)) AS purchases \
                 )",
            )
            .bind(to_ch_datetime(from)).bind(to_ch_datetime(to))
            .bind(to_ch_datetime(from)).bind(to_ch_datetime(to))
            .fetch_one::<ConversionRow>()
            .await
    }

    /// Counters for the last 5 minutes across all three tables.
    pub async fn realtime_stats(&self) -> clickhouse::error::Result<RealtimeStatsRow> {
        self.client
            .query(
                // assumeNotNull: see conversion_rate — scalar subqueries are Nullable.
                "SELECT \
                     assumeNotNull((SELECT count() FROM clicks FINAL WHERE timestamp >= now() - INTERVAL 5 MINUTE)) AS clicks, \
                     assumeNotNull((SELECT count() FROM views FINAL WHERE timestamp >= now() - INTERVAL 5 MINUTE)) AS views, \
                     assumeNotNull((SELECT count() FROM purchases FINAL WHERE timestamp >= now() - INTERVAL 5 MINUTE)) AS purchases",
            )
            .fetch_one::<RealtimeStatsRow>()
            .await
    }

    /// Per-product revenue from the materialized view (broken down by currency).
    pub async fn product_revenue(
        &self,
        limit: u64,
    ) -> clickhouse::error::Result<Vec<ProductRevenueRow>> {
        self.client
            .query(
                "SELECT product_id, currency, sumMerge(revenue_cents) AS revenue_cents \
                 FROM product_revenue \
                 GROUP BY product_id, currency \
                 ORDER BY revenue_cents DESC \
                 LIMIT ?",
            )
            .bind(limit)
            .fetch_all::<ProductRevenueRow>()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_ch_datetime_uses_clickhouse_canonical_form() {
        let t: DateTime<Utc> = "2026-07-11T14:41:34.999Z".parse().unwrap();
        // no 'T', no 'Z', no fractional seconds — the only form ClickHouse
        // casts to DateTime without extra settings
        assert_eq!(to_ch_datetime(t), "2026-07-11 14:41:34");
    }
}
