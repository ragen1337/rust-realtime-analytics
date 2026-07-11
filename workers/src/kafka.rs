use rdkafka::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;

pub struct EventConsumer {
    consumer: StreamConsumer,
}

impl EventConsumer {
    pub fn new(group_id: &str, topics: &[&str]) -> Result<Self, KafkaError> {
        let brokers =
            std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false") // commit manually after processing
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .create()?;

        consumer.subscribe(topics)?;

        Ok(Self { consumer })
    }

    pub async fn recv(&self) -> Result<BorrowedMessage<'_>, KafkaError> {
        self.consumer.recv().await
    }

    /// Commits the consumer's current position across all partitions —
    /// i.e. everything read via `recv()`.
    /// Call after the batch has been successfully written to ClickHouse.
    pub fn commit_state(&self) -> Result<(), KafkaError> {
        self.consumer.commit_consumer_state(CommitMode::Sync)
    }
}
