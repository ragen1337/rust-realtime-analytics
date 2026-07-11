use std::fmt;

#[derive(Clone, Copy)]
pub enum KafkaTopic {
    ClickEvents,
    ViewEvents,
    PurchaseEvents,
}

impl KafkaTopic {
    /// Parse a Kafka topic name back into the enum.
    /// `as_ref()` stays the single source of truth for the strings.
    pub fn from_topic(topic: &str) -> Option<Self> {
        [Self::ClickEvents, Self::ViewEvents, Self::PurchaseEvents]
            .into_iter()
            .find(|t| t.as_ref() == topic)
    }
}

impl AsRef<str> for KafkaTopic {
    fn as_ref(&self) -> &str {
        match self {
            KafkaTopic::ClickEvents => "events.clicks",
            KafkaTopic::ViewEvents => "events.views",
            KafkaTopic::PurchaseEvents => "events.purchases",
        }
    }
}

impl fmt::Display for KafkaTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_topic_roundtrips_every_variant() {
        for topic in [
            KafkaTopic::ClickEvents,
            KafkaTopic::ViewEvents,
            KafkaTopic::PurchaseEvents,
        ] {
            let parsed = KafkaTopic::from_topic(topic.as_ref()).unwrap();
            assert_eq!(parsed.as_ref(), topic.as_ref());
        }
    }

    #[test]
    fn from_topic_rejects_unknown() {
        assert!(KafkaTopic::from_topic("events.unknown").is_none());
        assert!(KafkaTopic::from_topic("").is_none());
    }
}
