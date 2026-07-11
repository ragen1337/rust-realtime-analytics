use serde::Deserialize;

use crate::cache::CacheKey;

#[derive(Debug, Deserialize)]
pub struct ProductRevenueQuery {
    pub limit: Option<u64>,
}

impl CacheKey for ProductRevenueQuery {
    fn cache_key(&self) -> String {
        format!("product-revenue|{}", self.limit.unwrap_or(10))
    }
}
