use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Fields shared by every product event. Flattened into each concrete event
/// so the JSON stays flat (no nested `context` object on the wire).
#[derive(Serialize, Deserialize, Debug)]
pub struct EventContext {
    /// Unique id for this event. Generated server-side if the client omits it.
    /// It is the dedup key of the raw ClickHouse tables (ReplacingMergeTree sorts by
    /// `(user_id, timestamp, event_id)`), so a re-delivered or client-retried event is
    /// collapsed there. This is NOT an idempotency key at the API: every POST is accepted,
    /// delivery is at-least-once, and dedup is eventual (until a background merge, plain
    /// `count()` sees duplicates; query with `FINAL`). The materialized views do not dedup.
    #[serde(default = "Uuid::new_v4")]
    pub event_id: Uuid,
    pub user_id: Uuid,
    pub product_id: Uuid,
    pub session_id: Uuid,
    pub timestamp: DateTime<Utc>,
}

/// Where in the UI an interaction originated.
#[derive(Serialize, Deserialize, Debug)]
pub struct ProductEventMetadata {
    pub source: String,
    pub position: u32,
    pub category: String,
}

/// A user clicked a product (e.g. from a listing or search results).
#[derive(Serialize, Deserialize, Debug)]
pub struct ClickEvent {
    #[serde(flatten)]
    pub context: EventContext,
    pub metadata: ProductEventMetadata,
}

/// A product was shown to the user.
#[derive(Serialize, Deserialize, Debug)]
pub struct ViewEvent {
    #[serde(flatten)]
    pub context: EventContext,
    /// How long the product stayed on screen, in milliseconds.
    pub duration_ms: u64,
    /// Page/URL the view came from, if known.
    pub referrer: Option<String>,
}

/// A user completed a purchase of a product.
#[derive(Serialize, Deserialize, Debug)]
pub struct PurchaseEvent {
    #[serde(flatten)]
    pub context: EventContext,
    pub order_id: Uuid,
    pub quantity: u32,
    /// Unit price in minor units (cents) to avoid floating-point money bugs.
    pub unit_price_cents: u64,
    /// ISO 4217 currency code, e.g. "USD".
    pub currency: String,
    pub payment_method: PaymentMethod,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum PaymentMethod {
    Card,
    PayPal,
    ApplePay,
    GooglePay,
}

impl From<PaymentMethod> for String {
    fn from(method: PaymentMethod) -> Self {
        match method {
            PaymentMethod::Card => "card",
            PaymentMethod::PayPal => "paypal",
            PaymentMethod::ApplePay => "apple_pay",
            PaymentMethod::GooglePay => "google_pay",
        }
        .to_string()
    }
}

// The JSON shape below is the wire contract: HTTP clients, Kafka messages and
// the workers all rely on it. These tests pin it down.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn click_event_json_is_flat() {
        // context is #[serde(flatten)]: all fields sit at the top level,
        // there must be no nested "context" object on the wire.
        let json = r#"{
            "user_id": "11111111-1111-1111-1111-111111111111",
            "product_id": "22222222-2222-2222-2222-222222222222",
            "session_id": "33333333-3333-3333-3333-333333333333",
            "timestamp": "2026-07-06T14:00:00Z",
            "metadata": {"source": "search", "position": 3, "category": "electronics"}
        }"#;

        let event: ClickEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.metadata.position, 3);

        let out = serde_json::to_value(&event).unwrap();
        assert!(out.get("context").is_none(), "context must stay flattened");
        assert_eq!(out["user_id"], "11111111-1111-1111-1111-111111111111");
    }

    #[test]
    fn event_id_is_generated_when_omitted() {
        let json = r#"{
            "user_id": "11111111-1111-1111-1111-111111111111",
            "product_id": "22222222-2222-2222-2222-222222222222",
            "session_id": "33333333-3333-3333-3333-333333333333",
            "timestamp": "2026-07-06T14:00:00Z",
            "metadata": {"source": "search", "position": 1, "category": "books"}
        }"#;

        let a: ClickEvent = serde_json::from_str(json).unwrap();
        let b: ClickEvent = serde_json::from_str(json).unwrap();
        // generated (non-nil) and unique per deserialization
        assert!(!a.context.event_id.is_nil());
        assert_ne!(a.context.event_id, b.context.event_id);
    }

    #[test]
    fn client_supplied_event_id_is_kept() {
        let json = r#"{
            "event_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "user_id": "11111111-1111-1111-1111-111111111111",
            "product_id": "22222222-2222-2222-2222-222222222222",
            "session_id": "33333333-3333-3333-3333-333333333333",
            "timestamp": "2026-07-06T14:00:00Z",
            "metadata": {"source": "search", "position": 1, "category": "books"}
        }"#;

        let event: ClickEvent = serde_json::from_str(json).unwrap();
        assert_eq!(
            event.context.event_id,
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                .parse::<Uuid>()
                .unwrap()
        );
    }

    #[test]
    fn payment_method_uses_snake_case_on_the_wire() {
        let m: PaymentMethod = serde_json::from_str(r#""apple_pay""#).unwrap();
        assert!(matches!(m, PaymentMethod::ApplePay));
        // "ApplePay" (the Rust variant name) is not part of the contract
        assert!(serde_json::from_str::<PaymentMethod>(r#""ApplePay""#).is_err());
        // From<PaymentMethod> must agree with the serde names
        assert_eq!(String::from(PaymentMethod::GooglePay), "google_pay");
        assert_eq!(String::from(PaymentMethod::Card), "card");
    }

    #[test]
    fn purchase_event_deserializes_from_api_shape() {
        let json = r#"{
            "user_id": "11111111-1111-1111-1111-111111111111",
            "product_id": "22222222-2222-2222-2222-222222222222",
            "session_id": "33333333-3333-3333-3333-333333333333",
            "timestamp": "2026-07-06T14:00:00Z",
            "order_id": "44444444-4444-4444-4444-444444444444",
            "quantity": 2,
            "unit_price_cents": 1999,
            "currency": "USD",
            "payment_method": "card"
        }"#;

        let event: PurchaseEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.quantity, 2);
        assert_eq!(event.unit_price_cents, 1999);
        assert!(matches!(event.payment_method, PaymentMethod::Card));
    }
}
