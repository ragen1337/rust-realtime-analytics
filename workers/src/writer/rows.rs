use chrono::{DateTime, Utc};
use clickhouse::Row;
use events_contract::model::{ClickEvent, PurchaseEvent, ViewEvent};
use serde::Serialize;
use uuid::Uuid;

#[derive(Row, Serialize)]
pub struct ClickRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub event_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub user_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub product_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub session_id: Uuid,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub source: String,
    pub position: u32,
    pub category: String,
    // the `date` column is deliberately omitted — it has a DEFAULT in the table
}

impl From<ClickEvent> for ClickRow {
    fn from(e: ClickEvent) -> Self {
        Self {
            event_id: e.context.event_id,
            user_id: e.context.user_id,
            product_id: e.context.product_id,
            session_id: e.context.session_id,
            timestamp: e.context.timestamp,
            source: e.metadata.source,
            position: e.metadata.position,
            category: e.metadata.category,
        }
    }
}

#[derive(Row, Serialize)]
pub struct ViewRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub event_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub user_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub product_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub session_id: Uuid,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub duration_ms: u64,
    /// Option maps to Nullable, no adapter needed.
    pub referrer: Option<String>,
    // the `date` column is deliberately omitted — it has a DEFAULT in the table
}

impl From<ViewEvent> for ViewRow {
    fn from(e: ViewEvent) -> Self {
        Self {
            event_id: e.context.event_id,
            user_id: e.context.user_id,
            product_id: e.context.product_id,
            session_id: e.context.session_id,
            timestamp: e.context.timestamp,
            duration_ms: e.duration_ms,
            referrer: e.referrer,
        }
    }
}

#[derive(Row, Serialize)]
pub struct PurchaseRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub event_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub user_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub product_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    pub session_id: Uuid,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::uuid")]
    pub order_id: Uuid,
    pub quantity: u32,
    pub unit_price_cents: u64,
    pub currency: String,
    /// Deliberately String, not an enum: RowBinary enums break the insert.
    pub payment_method: String,
    // the `date` column is deliberately omitted — it has a DEFAULT in the table
}

impl From<PurchaseEvent> for PurchaseRow {
    fn from(e: PurchaseEvent) -> Self {
        Self {
            event_id: e.context.event_id,
            user_id: e.context.user_id,
            product_id: e.context.product_id,
            session_id: e.context.session_id,
            timestamp: e.context.timestamp,
            order_id: e.order_id,
            quantity: e.quantity,
            unit_price_cents: e.unit_price_cents,
            currency: e.currency,
            payment_method: e.payment_method.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use events_contract::model::{EventContext, PaymentMethod, ProductEventMetadata};

    fn context() -> EventContext {
        EventContext {
            event_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            product_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn click_row_maps_all_fields() {
        let event = ClickEvent {
            context: context(),
            metadata: ProductEventMetadata {
                source: "search".into(),
                position: 7,
                category: "books".into(),
            },
        };
        let expected_id = event.context.event_id;

        let row = ClickRow::from(event);
        assert_eq!(row.event_id, expected_id);
        assert_eq!(row.source, "search");
        assert_eq!(row.position, 7);
        assert_eq!(row.category, "books");
    }

    #[test]
    fn view_row_keeps_optional_referrer() {
        let event = ViewEvent {
            context: context(),
            duration_ms: 1500,
            referrer: None,
        };
        let row = ViewRow::from(event);
        assert_eq!(row.duration_ms, 1500);
        assert_eq!(row.referrer, None);
    }

    #[test]
    fn purchase_row_stringifies_payment_method() {
        let event = PurchaseEvent {
            context: context(),
            order_id: Uuid::new_v4(),
            quantity: 3,
            unit_price_cents: 2500,
            currency: "USD".into(),
            payment_method: PaymentMethod::ApplePay,
        };
        let row = PurchaseRow::from(event);
        // must match the ClickHouse enum values in the schema
        assert_eq!(row.payment_method, "apple_pay");
        assert_eq!(row.quantity, 3);
        assert_eq!(row.unit_price_cents, 2500);
    }
}
