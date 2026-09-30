use std::time::Duration;

use rdkafka::Message;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::error::WorkerError;
use crate::kafka::EventConsumer;
use crate::writer::{ChWriter, ClickRow, PurchaseRow, ViewRow};
use events_contract::model::{ClickEvent, PurchaseEvent, ViewEvent};
use events_contract::topics::KafkaTopic;

/// Accumulates Kafka events into per-type batches and flushes them to ClickHouse
/// when a threshold is reached (BATCH_SIZE) or on a timer (FLUSH_INTERVAL).
pub struct Batcher {
    /// Only used to tell workers apart in logs.
    worker_id: usize,
    writer: ChWriter,
    clicks: Vec<ClickRow>,
    views: Vec<ViewRow>,
    purchases: Vec<PurchaseRow>,
}

impl Batcher {
    const BATCH_SIZE: usize = 1000;
    const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
    /// Pause before re-flushing a full batch after a failure (ClickHouse is likely down).
    const RETRY_PAUSE: Duration = Duration::from_secs(1);

    pub fn new(worker_id: usize, writer: ChWriter) -> Self {
        Self {
            worker_id,
            writer,
            clicks: Vec::new(),
            views: Vec::new(),
            purchases: Vec::new(),
        }
    }

    /// Main loop: reads from Kafka, batches, flushes on size or on a timer.
    /// Exits on `shutdown` after one final flush.
    ///
    /// Backpressure: a full batch must land in ClickHouse before we consume more.
    /// While the flush keeps failing we stop calling `recv()`, so the backlog
    /// accumulates in the broker instead of growing this process's memory unboundedly.
    pub async fn run(mut self, consumer: EventConsumer, shutdown: CancellationToken) {
        let mut ticker = interval(Self::FLUSH_INTERVAL);

        loop {
            // `cancelled()` is level-triggered: a token cancelled while we were busy
            // inside `flush()` stays cancelled, so the signal cannot be lost between
            // `select!` calls. Checked here too so a worker stuck retrying against a
            // dead ClickHouse still exits.
            if shutdown.is_cancelled() {
                break;
            }

            if self.is_full() {
                if let Err(e) = self.flush(&consumer).await {
                    error!(worker_id = self.worker_id, error = %e, "flush failed, pausing consumption");
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(Self::RETRY_PAUSE) => {}
                    }
                }
                continue;
            }

            tokio::select! {
                // shutdown first: don't keep consuming while messages are flowing
                biased;
                _ = shutdown.cancelled() => break,
                // a message arrived from Kafka
                result = consumer.recv() => {
                    match result {
                        Ok(msg) => {
                            if let Err(e) = self.accept(msg.topic(), msg.payload().unwrap_or_default()) {
                                warn!(worker_id = self.worker_id, error = %e, "bad payload");
                            }
                        }
                        Err(e) => error!(worker_id = self.worker_id, error = %e, "kafka recv error"),
                    }
                }
                // FLUSH_INTERVAL elapsed — write whatever has accumulated
                _ = ticker.tick() => {
                    if let Err(e) = self.flush(&consumer).await {
                        error!(worker_id = self.worker_id, error = %e, "flush failed");
                    }
                },
            }
        }

        info!(worker_id = self.worker_id, "shutdown: final flush");
        if let Err(e) = self.flush(&consumer).await {
            // offsets were not committed, so Kafka re-delivers these events after restart
            error!(
                worker_id = self.worker_id,
                error = %e,
                "final flush failed, uncommitted events will be re-delivered after restart (at-least-once)"
            );
        }
    }

    /// Deserializes a message based on its topic and puts it into the matching batch.
    /// Deserialization errors are propagated up (poison messages are logged in `run`).
    fn accept(&mut self, topic: &str, payload: &[u8]) -> serde_json::Result<()> {
        match KafkaTopic::from_topic(topic) {
            Some(KafkaTopic::ClickEvents) => self
                .clicks
                .push(serde_json::from_slice::<ClickEvent>(payload)?.into()),
            Some(KafkaTopic::ViewEvents) => self
                .views
                .push(serde_json::from_slice::<ViewEvent>(payload)?.into()),
            Some(KafkaTopic::PurchaseEvents) => self
                .purchases
                .push(serde_json::from_slice::<PurchaseEvent>(payload)?.into()),
            None => warn!(topic, "unknown topic"),
        }
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.clicks.len() >= Self::BATCH_SIZE
            || self.views.len() >= Self::BATCH_SIZE
            || self.purchases.len() >= Self::BATCH_SIZE
    }

    /// Writes non-empty batches to ClickHouse and commits the offset.
    /// Commit happens only after a successful write (at-least-once).
    /// On error, `?` aborts the flush before the commit, so unwritten data
    /// will be re-read from Kafka.
    async fn flush(&mut self, consumer: &EventConsumer) -> Result<(), WorkerError> {
        if self.clicks.is_empty() && self.views.is_empty() && self.purchases.is_empty() {
            return Ok(());
        }

        if !self.clicks.is_empty() {
            self.writer.write_clicks(&self.clicks).await?;
            self.clicks.clear();
        }
        if !self.views.is_empty() {
            self.writer.write_views(&self.views).await?;
            self.views.clear();
        }
        if !self.purchases.is_empty() {
            self.writer.write_purchases(&self.purchases).await?;
            self.purchases.clear();
        }

        consumer.commit_state()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ChWriter::new() only builds a lazy HTTP client — no network happens
    // until a flush, so constructing a Batcher in tests is safe.
    fn batcher() -> Batcher {
        Batcher::new(0, ChWriter::new())
    }

    const CLICK_JSON: &str = r#"{
        "user_id": "11111111-1111-1111-1111-111111111111",
        "product_id": "22222222-2222-2222-2222-222222222222",
        "session_id": "33333333-3333-3333-3333-333333333333",
        "timestamp": "2026-07-06T14:00:00Z",
        "metadata": {"source": "search", "position": 1, "category": "books"}
    }"#;

    #[test]
    fn accept_routes_event_to_matching_batch() {
        let mut b = batcher();
        b.accept("events.clicks", CLICK_JSON.as_bytes()).unwrap();
        assert_eq!(b.clicks.len(), 1);
        assert!(b.views.is_empty());
        assert!(b.purchases.is_empty());
    }

    #[test]
    fn accept_propagates_bad_payload_and_batches_nothing() {
        let mut b = batcher();
        assert!(b.accept("events.clicks", b"not json").is_err());
        // a click payload is not a valid purchase — wrong-shape JSON must also fail
        assert!(b.accept("events.purchases", CLICK_JSON.as_bytes()).is_err());
        assert!(b.clicks.is_empty());
        assert!(b.purchases.is_empty());
    }

    #[test]
    fn accept_ignores_unknown_topic() {
        let mut b = batcher();
        // unknown topic is logged and skipped, not an error (poison-pill safety)
        b.accept("events.unknown", CLICK_JSON.as_bytes()).unwrap();
        assert!(b.clicks.is_empty() && b.views.is_empty() && b.purchases.is_empty());
    }

    #[test]
    fn is_full_triggers_exactly_at_batch_size() {
        let mut b = batcher();
        for _ in 0..Batcher::BATCH_SIZE - 1 {
            b.accept("events.clicks", CLICK_JSON.as_bytes()).unwrap();
        }
        assert!(!b.is_full());
        b.accept("events.clicks", CLICK_JSON.as_bytes()).unwrap();
        assert!(b.is_full());
    }

    // rdkafka connects lazily, so a consumer can be built without a broker.
    // With the token already cancelled and empty batches, `run` must skip
    // consuming and return right after the (no-op) final flush.
    #[tokio::test]
    async fn run_returns_promptly_when_already_cancelled() {
        let consumer = EventConsumer::new("batcher-shutdown-test", &["events.clicks"]).unwrap();
        let token = CancellationToken::new();
        token.cancel();

        tokio::time::timeout(Duration::from_secs(5), batcher().run(consumer, token))
            .await
            .expect("run must exit on a cancelled token");
    }
}
