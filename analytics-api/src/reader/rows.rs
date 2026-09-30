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
    // Decode DateTime64(3) from ClickHouse's wire format (i64 millis), but keep chrono's
    // default Serialize (RFC 3339) so the JSON API matches what requests accept.
    #[serde(deserialize_with = "clickhouse::serde::chrono::datetime64::millis::deserialize")]
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde::de::IntoDeserializer;
    use serde::de::value::{Error as DeError, I64Deserializer};

    const EVENT_ID: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const PRODUCT_ID: &str = "22222222-2222-4222-8222-222222222222";

    fn row(timestamp: DateTime<Utc>) -> UserActivityRow {
        UserActivityRow {
            event_type: "click".to_owned(),
            event_id: EVENT_ID.parse().unwrap(),
            product_id: PRODUCT_ID.parse().unwrap(),
            timestamp,
        }
    }

    #[test]
    fn user_activity_serializes_rfc3339_and_uuid_strings() {
        let ts = Utc.with_ymd_and_hms(2026, 7, 6, 14, 0, 0).unwrap();
        let v = serde_json::to_value(row(ts)).unwrap();
        assert_eq!(v["timestamp"], "2026-07-06T14:00:00Z");
        assert_eq!(v["event_id"], EVENT_ID);
        assert_eq!(v["product_id"], PRODUCT_ID);
        assert_eq!(v["event_type"], "click");
    }

    #[test]
    fn user_activity_timestamp_keeps_millisecond_precision() {
        let ts = Utc.timestamp_millis_opt(1_783_346_400_123).unwrap();
        let v = serde_json::to_value(row(ts)).unwrap();
        assert_eq!(v["timestamp"], "2026-07-06T14:00:00.123Z");
    }

    #[test]
    fn user_activity_timestamp_decodes_clickhouse_millis() {
        // Same function the derive uses via `deserialize_with`.
        let d: I64Deserializer<DeError> = 1_783_346_400_123_i64.into_deserializer();
        let ts = clickhouse::serde::chrono::datetime64::millis::deserialize(d).unwrap();
        assert_eq!(ts, Utc.timestamp_millis_opt(1_783_346_400_123).unwrap());
    }
}
