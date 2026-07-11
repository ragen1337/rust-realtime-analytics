use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

use crate::cache::CacheKey;

#[derive(Debug, Deserialize)]
pub struct TopProductQuery {
    pub limit: Option<u64>, // default is applied in the handler: limit.unwrap_or(10)
    pub period: Period,     // enum -> garbage is rejected at parse time
    pub metric: Metric,
}

impl CacheKey for TopProductQuery {
    fn cache_key(&self) -> String {
        // key built from NORMALIZED values (limit.unwrap_or(10)), prefixed with the endpoint
        format!(
            "top-products|{}|{:?}|{}",
            self.metric.table(),
            self.period,
            self.limit.unwrap_or(10),
        )
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Metric {
    Clicks, // ?metric=clicks
    Views,
    Purchases,
}

impl Metric {
    /// Metric -> ClickHouse table name. Single source of truth; the value is safe
    /// to `format!` into SQL (it does not come from the user).
    pub fn table(&self) -> &'static str {
        match self {
            Metric::Clicks => "clicks",
            Metric::Views => "views",
            Metric::Purchases => "purchases",
        }
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub enum Period {
    #[serde(rename = "1h")] // variants aren't valid identifiers -> explicit rename
    OneHour,
    #[serde(rename = "24h")]
    OneDay,
    #[serde(rename = "7d")]
    SevenDays,
}

impl Period {
    /// Lower bound of the period: how far back from "now" to look.
    pub fn since(&self) -> DateTime<Utc> {
        let ago = match self {
            Period::OneHour => Duration::hours(1),
            Period::OneDay => Duration::hours(24),
            Period::SevenDays => Duration::days(7),
        };
        Utc::now() - ago
    }

    /// Cache TTL for this period: the wider the window, the longer we can cache.
    pub fn ttl(&self) -> std::time::Duration {
        let secs = match self {
            Period::OneHour => 60,
            Period::OneDay => 300,
            Period::SevenDays => 600,
        };
        std::time::Duration::from_secs(secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn period_parses_only_contract_values() {
        // the query-string values are the contract, not the Rust variant names
        assert!(matches!(
            serde_json::from_str::<Period>(r#""1h""#).unwrap(),
            Period::OneHour
        ));
        assert!(matches!(
            serde_json::from_str::<Period>(r#""24h""#).unwrap(),
            Period::OneDay
        ));
        assert!(matches!(
            serde_json::from_str::<Period>(r#""7d""#).unwrap(),
            Period::SevenDays
        ));
        assert!(serde_json::from_str::<Period>(r#""OneHour""#).is_err());
        assert!(serde_json::from_str::<Period>(r#""2h""#).is_err());
    }

    #[test]
    fn metric_parses_lowercase_and_maps_to_tables() {
        let m: Metric = serde_json::from_str(r#""purchases""#).unwrap();
        assert_eq!(m.table(), "purchases");
        assert!(serde_json::from_str::<Metric>(r#""Clicks""#).is_err());
    }

    #[test]
    fn period_since_is_ordered() {
        // wider period -> earlier lower bound
        assert!(Period::SevenDays.since() < Period::OneDay.since());
        assert!(Period::OneDay.since() < Period::OneHour.since());
        assert!(Period::OneHour.since() < Utc::now());
    }

    #[test]
    fn cache_key_normalizes_default_limit() {
        // limit=None and limit=10 must hit the same cache entry
        let implicit = TopProductQuery {
            limit: None,
            period: Period::OneHour,
            metric: Metric::Clicks,
        };
        let explicit = TopProductQuery {
            limit: Some(10),
            period: Period::OneHour,
            metric: Metric::Clicks,
        };
        assert_eq!(implicit.cache_key(), explicit.cache_key());

        // ...but a different limit/metric/period must not
        let other = TopProductQuery {
            limit: Some(20),
            period: Period::OneHour,
            metric: Metric::Clicks,
        };
        assert_ne!(implicit.cache_key(), other.cache_key());
    }
}
