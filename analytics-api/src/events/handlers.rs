use std::sync::Arc;
use std::time::Duration;

use actix_web::web::Bytes;
use actix_web::{HttpResponse, get, web};
use uuid::Uuid;

use super::params::{
    ConversionQuery, ProductRevenueQuery, Source, TopProductQuery, UserActivityQuery,
    mv_lower_bound,
};
use crate::cache::{CacheKey, CachedResponse, ResponseCache};
use crate::error::{AppError, AppResult};
use crate::reader::ChReader;

/// Uniform JSON response from a prebuilt body (shared by cache-hit and cache-miss paths).
fn json_response(body: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("application/json")
        .body(body)
}

/// `try_get_with` shares one init result across concurrent waiters, so the error
/// arrives wrapped in `Arc`; clone our (cheap, unit-variant) error back out of it.
fn shared_err(e: Arc<AppError>) -> AppError {
    (*e).clone()
}

#[get("/health")]
pub async fn health() -> AppResult<HttpResponse> {
    Ok(HttpResponse::Ok().body("OK!"))
}

/// Fallback for unmatched paths: returns our JSON envelope with 404
/// instead of actix's empty default 404. Wired up via `default_service` in main.
pub async fn not_found() -> AppResult<HttpResponse> {
    Err(AppError::NotFound)
}

// All cached handlers use `cache.try_get_with(key, init)`: on a miss only ONE
// caller runs `init` (the ClickHouse query); concurrent requests for the same
// key await that result instead of stampeding the database (dog-pile protection).
// Errors are NOT cached — the next request retries.

#[get("/top-products")]
pub async fn top_products(
    query: web::Query<TopProductQuery>,
    reader: web::Data<ChReader>,
    cache: web::Data<ResponseCache>,
) -> AppResult<HttpResponse> {
    let params = query.into_inner();
    let key = params.cache_key();

    let cached = cache
        .try_get_with(key, async {
            let since = params.period.since();
            let limit = params.limit.unwrap_or(10);
            // 1h is an exact raw scan; clicks over 24h/7d use the hourly MV (hour-aligned edge)
            let rows = match Source::choose(params.metric, params.period) {
                Source::HourlyMv => {
                    reader
                        .top_products_hourly(mv_lower_bound(since), limit)
                        .await?
                }
                Source::Raw => {
                    reader
                        .top_products_raw(params.metric.table(), since, limit)
                        .await?
                }
            };

            let body = Bytes::from(serde_json::to_vec(&rows)?);
            let ttl = params.period.ttl(); // TTL depends on the period
            Ok::<_, AppError>(CachedResponse { body, ttl })
        })
        .await
        .map_err(shared_err)?;

    Ok(json_response(cached.body))
}

#[get("/user-activity/{user_id}")]
pub async fn user_activity(
    path: web::Path<Uuid>,
    query: web::Query<UserActivityQuery>,
    reader: web::Data<ChReader>,
    cache: web::Data<ResponseCache>,
) -> AppResult<HttpResponse> {
    let user_id = path.into_inner();
    let params = query.into_inner();
    // user_id lives in Path, not in CacheKey -> mix it into the key manually
    let key = format!("user-activity|{}|{}", user_id, params.cache_key());

    let cached = cache
        .try_get_with(key, async {
            let rows = reader
                .user_activity(
                    user_id,
                    params.from,
                    params.to,
                    params.limit.unwrap_or(50),
                    params.offset.unwrap_or(0),
                )
                .await?;

            let body = Bytes::from(serde_json::to_vec(&rows)?);
            Ok::<_, AppError>(CachedResponse {
                body,
                ttl: Duration::from_secs(60),
            })
        })
        .await
        .map_err(shared_err)?;

    Ok(json_response(cached.body))
}

#[get("/conversion-rate")]
pub async fn conversion_rate(
    query: web::Query<ConversionQuery>,
    reader: web::Data<ChReader>,
    cache: web::Data<ResponseCache>,
) -> AppResult<HttpResponse> {
    let params = query.into_inner();
    let key = params.cache_key();

    let cached = cache
        .try_get_with(key, async {
            // a single summary row, not a Vec
            let row = reader.conversion_rate(params.from, params.to).await?;

            let body = Bytes::from(serde_json::to_vec(&row)?);
            Ok::<_, AppError>(CachedResponse {
                body,
                ttl: Duration::from_secs(60),
            })
        })
        .await
        .map_err(shared_err)?;

    Ok(json_response(cached.body))
}

#[get("/product-revenue")]
pub async fn product_revenue(
    query: web::Query<ProductRevenueQuery>,
    reader: web::Data<ChReader>,
    cache: web::Data<ResponseCache>,
) -> AppResult<HttpResponse> {
    let params = query.into_inner();
    let key = params.cache_key();

    let cached = cache
        .try_get_with(key, async {
            // read the pre-aggregate from the product_revenue MV
            let rows = reader.product_revenue(params.limit.unwrap_or(10)).await?;

            let body = Bytes::from(serde_json::to_vec(&rows)?);
            Ok::<_, AppError>(CachedResponse {
                body,
                ttl: Duration::from_secs(60),
            })
        })
        .await
        .map_err(shared_err)?;

    Ok(json_response(cached.body))
}

#[get("/realtime-stats")]
pub async fn realtime_stats(
    reader: web::Data<ChReader>,
    cache: web::Data<ResponseCache>,
) -> AppResult<HttpResponse> {
    // no parameters -> fixed key
    let key = "realtime-stats".to_string();

    let cached = cache
        .try_get_with(key, async {
            let row = reader.realtime_stats().await?;

            let body = Bytes::from(serde_json::to_vec(&row)?);
            // realtime — short TTL
            Ok::<_, AppError>(CachedResponse {
                body,
                ttl: Duration::from_secs(5),
            })
        })
        .await
        .map_err(shared_err)?;

    Ok(json_response(cached.body))
}
