use crate::model::{ClickEvent, EventContext, PurchaseEvent, ViewEvent};
use chrono::{DateTime, Duration, Utc};
use std::fmt;
use uuid::Uuid;

/// How far ahead of the server clock an event timestamp may be. Client clocks
/// drift, so a small allowance avoids rejecting honest events; anything beyond
/// it is a bug or garbage that would pollute time-bucketed analytics.
pub const MAX_FUTURE_SKEW: Duration = Duration::minutes(5);

/// Upper bound for `metadata.source` / `metadata.category`: they are short UI
/// labels, so a long value means misuse rather than a real source.
pub const MAX_LABEL_LEN: usize = 64;

/// Upper bound for `referrer`; 2048 is the de-facto maximum URL length.
pub const MAX_REFERRER_LEN: usize = 2048;

/// The first rule an event breaks. Both parts are static so the error is
/// cheap to build and safe to echo to clients (no user data inside).
#[derive(Debug, PartialEq, Eq)]
pub struct ValidationError {
    pub field: &'static str,
    pub reason: &'static str,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.reason)
    }
}

impl std::error::Error for ValidationError {}

/// Business-rule checks that serde cannot express. `now` is injectable so
/// tests do not depend on the wall clock.
pub trait Validate {
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ValidationError>;

    fn validate(&self) -> Result<(), ValidationError> {
        self.validate_at(Utc::now())
    }
}

fn err(field: &'static str, reason: &'static str) -> Result<(), ValidationError> {
    Err(ValidationError { field, reason })
}

fn check_id(field: &'static str, id: Uuid) -> Result<(), ValidationError> {
    if id.is_nil() {
        return err(field, "must not be the nil UUID");
    }
    Ok(())
}

fn check_label(field: &'static str, value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        return err(field, "must not be empty");
    }
    if value.chars().count() > MAX_LABEL_LEN {
        return err(field, "must be at most 64 characters");
    }
    Ok(())
}

impl Validate for EventContext {
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ValidationError> {
        check_id("user_id", self.user_id)?;
        check_id("product_id", self.product_id)?;
        check_id("session_id", self.session_id)?;
        if self.timestamp > now + MAX_FUTURE_SKEW {
            return err("timestamp", "must not be more than 5 minutes in the future");
        }
        Ok(())
    }
}

impl Validate for ClickEvent {
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ValidationError> {
        self.context.validate_at(now)?;
        check_label("metadata.source", &self.metadata.source)?;
        check_label("metadata.category", &self.metadata.category)
    }
}

impl Validate for ViewEvent {
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ValidationError> {
        self.context.validate_at(now)?;
        match &self.referrer {
            Some(r) if r.chars().count() > MAX_REFERRER_LEN => {
                err("referrer", "must be at most 2048 characters")
            }
            _ => Ok(()),
        }
    }
}

impl Validate for PurchaseEvent {
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ValidationError> {
        self.context.validate_at(now)?;
        if self.quantity < 1 {
            return err("quantity", "must be >= 1");
        }
        // Shape check only (ISO 4217 style); we deliberately do not embed the
        // full currency list, which would need maintaining.
        let c = self.currency.as_bytes();
        if c.len() != 3 || !c.iter().all(u8::is_ascii_uppercase) {
            return err("currency", "must be 3 uppercase ASCII letters (e.g. USD)");
        }
        // unit_price_cents == 0 is allowed on purpose: free items and promos exist.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{PaymentMethod, ProductEventMetadata};

    fn now() -> DateTime<Utc> {
        "2026-07-06T14:00:00Z".parse().unwrap()
    }

    fn context() -> EventContext {
        EventContext {
            event_id: Uuid::new_v4(),
            user_id: Uuid::from_u128(1),
            product_id: Uuid::from_u128(2),
            session_id: Uuid::from_u128(3),
            timestamp: now(),
        }
    }

    fn click() -> ClickEvent {
        ClickEvent {
            context: context(),
            metadata: ProductEventMetadata {
                source: "search".into(),
                position: 1,
                category: "books".into(),
            },
        }
    }

    fn view() -> ViewEvent {
        ViewEvent {
            context: context(),
            duration_ms: 100,
            referrer: Some("https://example.com".into()),
        }
    }

    fn purchase() -> PurchaseEvent {
        PurchaseEvent {
            context: context(),
            order_id: Uuid::from_u128(4),
            quantity: 1,
            unit_price_cents: 1999,
            currency: "USD".into(),
            payment_method: PaymentMethod::Card,
        }
    }

    fn field<T: Validate>(e: &T) -> &'static str {
        e.validate_at(now()).unwrap_err().field
    }

    #[test]
    fn valid_events_pass() {
        assert!(click().validate_at(now()).is_ok());
        assert!(view().validate_at(now()).is_ok());
        assert!(purchase().validate_at(now()).is_ok());
    }

    #[test]
    fn nil_ids_are_rejected() {
        let mut e = click();
        e.context.user_id = Uuid::nil();
        assert_eq!(field(&e), "user_id");
        let mut e = click();
        e.context.product_id = Uuid::nil();
        assert_eq!(field(&e), "product_id");
        let mut e = click();
        e.context.session_id = Uuid::nil();
        assert_eq!(field(&e), "session_id");
    }

    #[test]
    fn timestamp_allows_small_skew_but_not_far_future() {
        let mut e = click();
        e.context.timestamp = now() + MAX_FUTURE_SKEW;
        assert!(e.validate_at(now()).is_ok());
        e.context.timestamp = now() + MAX_FUTURE_SKEW + Duration::seconds(1);
        assert_eq!(field(&e), "timestamp");
        // the past is fine: clients may deliver events late
        e.context.timestamp = now() - Duration::days(30);
        assert!(e.validate_at(now()).is_ok());
    }

    #[test]
    fn click_source_and_category_must_be_non_blank() {
        let mut e = click();
        e.metadata.source = String::new();
        assert_eq!(field(&e), "metadata.source");
        let mut e = click();
        e.metadata.source = "   ".into();
        assert_eq!(field(&e), "metadata.source");
        let mut e = click();
        e.metadata.category = String::new();
        assert_eq!(field(&e), "metadata.category");
    }

    #[test]
    fn click_labels_are_length_capped() {
        let mut e = click();
        e.metadata.source = "x".repeat(MAX_LABEL_LEN);
        assert!(e.validate_at(now()).is_ok());
        e.metadata.source = "x".repeat(MAX_LABEL_LEN + 1);
        assert_eq!(field(&e), "metadata.source");
        let mut e = click();
        e.metadata.category = "x".repeat(MAX_LABEL_LEN + 1);
        assert_eq!(field(&e), "metadata.category");
    }

    #[test]
    fn view_referrer_is_optional_but_capped() {
        let mut e = view();
        e.referrer = None;
        assert!(e.validate_at(now()).is_ok());
        e.referrer = Some("x".repeat(MAX_REFERRER_LEN));
        assert!(e.validate_at(now()).is_ok());
        e.referrer = Some("x".repeat(MAX_REFERRER_LEN + 1));
        assert_eq!(field(&e), "referrer");
    }

    #[test]
    fn view_and_purchase_check_the_shared_context() {
        let mut v = view();
        v.context.timestamp = now() + Duration::days(1);
        assert_eq!(field(&v), "timestamp");
        let mut p = purchase();
        p.context.user_id = Uuid::nil();
        assert_eq!(field(&p), "user_id");
    }

    #[test]
    fn purchase_quantity_must_be_positive() {
        let mut e = purchase();
        e.quantity = 0;
        assert_eq!(field(&e), "quantity");
    }

    #[test]
    fn purchase_currency_must_be_three_uppercase_letters() {
        for bad in ["", "US", "USDD", "usd", "Usd", "U$D", "US1", "ЕВР", "€€€"] {
            let mut e = purchase();
            e.currency = bad.into();
            assert_eq!(field(&e), "currency", "{bad:?} must be rejected");
        }
        let mut e = purchase();
        e.currency = "EUR".into();
        assert!(e.validate_at(now()).is_ok());
    }

    #[test]
    fn purchase_zero_price_is_allowed() {
        let mut e = purchase();
        e.unit_price_cents = 0;
        assert!(e.validate_at(now()).is_ok());
    }

    #[test]
    fn error_display_names_field_and_reason() {
        let mut e = purchase();
        e.quantity = 0;
        assert_eq!(
            e.validate_at(now()).unwrap_err().to_string(),
            "quantity: must be >= 1"
        );
    }
}
