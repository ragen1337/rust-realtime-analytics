use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Deserialize — read from ClickHouse; Serialize — emit in the JSON response.
// Field order in each Row = column order in the corresponding SELECT.

/// top-products: product + metric value (count/sum).
#[derive(Debug, Row, Deserialize, Serialize)]
pub struct TopProductRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub product_id: Uuid,
    pub metric_value: u64, // from count() -> UInt64
}

/// user-activity: a single user event. Built via UNION ALL over clicks/views/purchases;
/// event_type is a string literal from the SELECT ('click' | 'view' | 'purchase').
#[derive(Debug, Row, Deserialize, Serialize)]
pub struct UserActivityRow {
    pub event_type: String,
    #[serde(with = "clickhouse::serde::uuid")]
    pub event_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub product_id: Uuid,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

/// conversion-rate: a single summary row for the period (purchases / views).
#[derive(Debug, Row, Deserialize, Serialize)]
pub struct ConversionRow {
    pub views: u64,
    pub purchases: u64,
    pub conversion_rate: f64,
}

/// realtime-stats: counters for the most recent window (e.g. 5 minutes).
#[derive(Debug, Row, Deserialize, Serialize)]
pub struct RealtimeStatsRow {
    pub clicks: u64,
    pub views: u64,
    pub purchases: u64,
}

/// product_revenue (from the MV): product revenue, separately per currency.
#[derive(Debug, Row, Deserialize, Serialize)]
pub struct ProductRevenueRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub product_id: Uuid,
    pub currency: String,
    pub revenue_cents: u64,
}
