//! End-to-end tests against a RUNNING stack (`docker compose up -d`).
//!
//! They are `#[ignore]`d so a plain `cargo test` stays green without Docker.
//! Run them explicitly:
//!
//! ```bash
//! docker compose up -d          # from the repo root
//! cd load-generator
//! cargo test -- --ignored
//! ```
//!
//! Endpoints/credentials come from env vars with the same defaults as compose:
//! INGESTION_URL, ANALYTICS_URL, CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD.

use std::time::Duration;

use events_contract::model::{
    ClickEvent, EventContext, PaymentMethod, ProductEventMetadata, PurchaseEvent,
};
use uuid::Uuid;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn ingestion_url() -> String {
    env_or("INGESTION_URL", "http://localhost:8080")
}

fn analytics_url() -> String {
    env_or("ANALYTICS_URL", "http://localhost:8082")
}

fn context() -> EventContext {
    EventContext {
        event_id: Uuid::new_v4(),
        user_id: Uuid::new_v4(),
        product_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
        timestamp: chrono::Utc::now(),
    }
}

/// Runs a query against ClickHouse over HTTP and returns the raw text response.
async fn clickhouse_query(client: &reqwest::Client, query: &str) -> String {
    let url = env_or("CLICKHOUSE_URL", "http://localhost:8123");
    let user = env_or("CLICKHOUSE_USER", "admin");
    let password = env_or("CLICKHOUSE_PASSWORD", "secret");

    client
        .post(format!("{url}/?user={user}&password={password}"))
        .body(query.to_string())
        .send()
        .await
        .expect("clickhouse must be reachable")
        .text()
        .await
        .unwrap()
}

/// Polls ClickHouse until `query` returns `expected` (workers flush at most
/// every 5s, so a freshly ingested event needs a few seconds to land).
async fn wait_for_row(client: &reqwest::Client, query: &str, expected: &str) {
    const ATTEMPTS: u32 = 20;
    for _ in 0..ATTEMPTS {
        if clickhouse_query(client, query).await.trim() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("gave up waiting for: {query}");
}

/// The write path: HTTP -> Kafka -> workers -> ClickHouse.
#[tokio::test]
#[ignore = "requires the docker compose stack"]
async fn click_event_travels_the_whole_pipeline() {
    let client = reqwest::Client::new();

    let event = ClickEvent {
        context: context(),
        metadata: ProductEventMetadata {
            source: "e2e".to_string(),
            position: 1,
            category: "test".to_string(),
        },
    };
    let event_id = event.context.event_id;

    let resp = client
        .post(format!("{}/api/v1/events/click", ingestion_url()))
        .json(&event)
        .send()
        .await
        .expect("ingestion-api must be reachable");
    assert!(resp.status().is_success(), "ingestion rejected the event");

    wait_for_row(
        &client,
        &format!("SELECT count() FROM events.clicks FINAL WHERE event_id = '{event_id}'"),
        "1",
    )
    .await;
}

/// The read path on top of the write path: a purchase becomes visible
/// through the analytics API (user-activity + conversion inputs).
#[tokio::test]
#[ignore = "requires the docker compose stack"]
async fn purchase_shows_up_in_analytics() {
    let client = reqwest::Client::new();

    let event = PurchaseEvent {
        context: context(),
        order_id: Uuid::new_v4(),
        quantity: 1,
        unit_price_cents: 4999,
        currency: "USD".to_string(),
        payment_method: PaymentMethod::Card,
    };
    let event_id = event.context.event_id;
    let user_id = event.context.user_id;

    let resp = client
        .post(format!("{}/api/v1/events/purchase", ingestion_url()))
        .json(&event)
        .send()
        .await
        .expect("ingestion-api must be reachable");
    assert!(resp.status().is_success(), "ingestion rejected the event");

    // wait until the workers have landed it in ClickHouse...
    wait_for_row(
        &client,
        &format!("SELECT count() FROM events.purchases FINAL WHERE event_id = '{event_id}'"),
        "1",
    )
    .await;

    // ...then it must be served back by the analytics API
    let body = client
        .get(format!(
            "{}/api/v1/analytics/user-activity/{user_id}?from=2026-01-01T00:00:00Z&to=2036-01-01T00:00:00Z",
            analytics_url()
        ))
        .send()
        .await
        .expect("analytics-api must be reachable")
        .text()
        .await
        .unwrap();
    assert!(
        body.contains(&event_id.to_string()),
        "user-activity must return the purchase, got: {body}"
    );
}

/// Every analytics endpoint answers 200 with a JSON body.
#[tokio::test]
#[ignore = "requires the docker compose stack"]
async fn analytics_endpoints_respond() {
    let client = reqwest::Client::new();
    let base = analytics_url();

    let endpoints = [
        "/api/v1/analytics/top-products?metric=clicks&period=1h&limit=5".to_string(),
        "/api/v1/analytics/top-products?metric=purchases&period=7d&limit=5".to_string(),
        "/api/v1/analytics/realtime-stats".to_string(),
        "/api/v1/analytics/conversion-rate?from=2026-01-01T00:00:00Z&to=2036-01-01T00:00:00Z"
            .to_string(),
        "/api/v1/analytics/product-revenue?limit=5".to_string(),
        format!(
            "/api/v1/analytics/user-activity/{}?from=2026-01-01T00:00:00Z&to=2036-01-01T00:00:00Z",
            Uuid::new_v4()
        ),
    ];

    for path in endpoints {
        let resp = client.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(resp.status(), 200, "GET {path} must return 200");
    }

    // bad params must produce a clean 400, not a 500
    let resp = client
        .get(format!(
            "{base}/api/v1/analytics/top-products?metric=nonsense&period=1h"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "garbage params must be rejected as 400");
}
