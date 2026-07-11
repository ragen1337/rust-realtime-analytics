use thiserror::Error;

/// Errors from the flush stage: writing to ClickHouse and committing the Kafka offset.
/// `#[from]` enables automatic conversion via `?`.
#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("clickhouse write failed: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),

    #[error("kafka commit failed: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
}
