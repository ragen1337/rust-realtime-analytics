mod batcher;
mod error;
mod kafka;
mod writer;

use batcher::Batcher;
use events_contract::topics::KafkaTopic;
use kafka::EventConsumer;
use writer::ChWriter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let writer = ChWriter::new();
    tracing::info!("starting workers");

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    // no point running more workers than partitions per topic — the extras sit idle
    let partitions = std::env::var("KAFKA_NUM_PARTITIONS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(3);
    let workers = cpus.min(partitions);

    let mut handles = Vec::new();
    for _ in 0..workers {
        let writer = writer.clone();
        handles.push(tokio::spawn(async move {
            let topics = [
                KafkaTopic::ClickEvents.as_ref(),
                KafkaTopic::ViewEvents.as_ref(),
                KafkaTopic::PurchaseEvents.as_ref(),
            ];

            let consumer = EventConsumer::new("events-workers", &topics)
                .expect("failed to create kafka consumer");

            Batcher::new(writer).run(consumer).await;
        }));
    }

    // wait for all workers to finish (otherwise main exits immediately and kills the tasks)
    for h in handles {
        let _ = h.await;
    }
}
