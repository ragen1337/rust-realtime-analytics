use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::cache::CacheKey;

#[derive(Debug, Deserialize)]
pub struct ConversionQuery {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl CacheKey for ConversionQuery {
    fn cache_key(&self) -> String {
        format!(
            "conversion-rate|{}|{}",
            self.from.timestamp(),
            self.to.timestamp(),
        )
    }
}
