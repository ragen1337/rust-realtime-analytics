use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;

pub struct EventProducer {
    producer: FutureProducer,
}

impl EventProducer {
    pub fn new() -> Self {
        let brokers =
            std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());

        let producer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .set("compression.codec", "lz4")
            .create()
            .expect("Failed to create Kafka producer");

        Self { producer }
    }

    pub async fn send_event(
        &self,
        topic: &str,
        key: &str,
        payload: &str,
    ) -> Result<(), rdkafka::error::KafkaError> {
        self.producer
            .send(
                FutureRecord::to(topic).payload(payload).key(key),
                Duration::from_secs(5),
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| e)
    }

    /// Deliver everything still sitting in rdkafka's internal queue to the broker.
    /// Called on graceful shutdown so buffered events are not lost.
    pub fn flush(&self, timeout: Duration) {
        if let Err(e) = self.producer.flush(timeout) {
            tracing::error!(error = %e, "kafka flush on shutdown failed");
        }
    }
}
