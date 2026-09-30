mod batcher;
mod error;
mod kafka;
mod writer;

use batcher::Batcher;
use events_contract::topics::KafkaTopic;
use kafka::EventConsumer;
use tokio::signal;
use tokio_util::sync::CancellationToken;
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

    // Signal handlers are installed once here; workers only observe the token.
    // A cancelled token stays cancelled, so a signal arriving while a worker is
    // busy flushing is still seen on its next check.
    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            wait_for_signal().await;
            tracing::info!("shutdown signal received, stopping workers");
            shutdown.cancel();
        }
    });

    let mut handles = Vec::new();
    for worker_id in 0..workers {
        let writer = writer.clone();
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let topics = [
                KafkaTopic::ClickEvents.as_ref(),
                KafkaTopic::ViewEvents.as_ref(),
                KafkaTopic::PurchaseEvents.as_ref(),
            ];

            let consumer = EventConsumer::new("events-workers", &topics)
                .expect("failed to create kafka consumer");

            Batcher::new(worker_id, writer)
                .run(consumer, shutdown)
                .await;
        }));
    }

    // wait for all workers to finish (otherwise main exits immediately and kills the tasks)
    for h in handles {
        let _ = h.await;
    }
}

/// Completes on SIGTERM (docker/k8s asking us to stop) or Ctrl-C (local debugging).
/// Unix only — prod runs in a Linux container, dev on macOS.
async fn wait_for_signal() {
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    tokio::select! {
        _ = signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }
}
