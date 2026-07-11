use std::time::{Duration, Instant};

use actix_web::web::Bytes;
use moka::Expiry;
use moka::future::Cache;

/// Cache value: a ready-to-send JSON response body + the TTL for this specific entry.
/// The TTL travels with the value so Expiry can read it.
#[derive(Clone)]
pub struct CachedResponse {
    pub body: Bytes,
    pub ttl: Duration,
}

/// The key is a normalized query string.
pub type ResponseCache = Cache<String, CachedResponse>;

/// Expiration policy: each entry's TTL = its own `ttl`.
struct PerEntryExpiry;

impl Expiry<String, CachedResponse> for PerEntryExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &CachedResponse,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

pub fn build_cache() -> ResponseCache {
    Cache::builder()
        .max_capacity(10_000)
        .expire_after(PerEntryExpiry) // per-entry TTL instead of a global time_to_live
        .build()
}

pub trait CacheKey {
    fn cache_key(&self) -> String;
}
