use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::cache::CacheKey;

#[derive(Debug, Deserialize)]
pub struct UserActivityQuery {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

impl CacheKey for UserActivityQuery {
    fn cache_key(&self) -> String {
        // NOTE: user_id is not here (it lives in Path) — it is mixed in by the handler.
        // timestamp() is a stable numeric form of the time for the key.
        format!(
            "user-activity|{}|{}|{}|{}",
            self.from.timestamp(),
            self.to.timestamp(),
            self.limit.unwrap_or(50),
            self.offset.unwrap_or(0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(limit: Option<u64>, offset: Option<u64>) -> UserActivityQuery {
        UserActivityQuery {
            from: "2026-07-01T00:00:00Z".parse().unwrap(),
            to: "2026-07-02T00:00:00Z".parse().unwrap(),
            limit,
            offset,
        }
    }

    #[test]
    fn cache_key_normalizes_defaults() {
        assert_eq!(
            query(None, None).cache_key(),
            query(Some(50), Some(0)).cache_key()
        );
        assert_ne!(
            query(None, None).cache_key(),
            query(Some(50), Some(10)).cache_key()
        );
    }
}
