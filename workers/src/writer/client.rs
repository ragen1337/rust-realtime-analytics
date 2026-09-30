use std::time::{Duration, Instant};

use crate::writer::rows::{ClickRow, PurchaseRow, ViewRow};
use clickhouse::{Client, RowOwned, RowWrite};

// TODO: if retry is needed elsewhere (e.g. connecting to ClickHouse at startup),
// replace the manual loop with the `backon` crate instead of copy-pasting our own implementation.
#[derive(Clone)]
pub struct ChWriter {
    client: Client,
}

impl ChWriter {
    const MAX_RETRIES: u32 = 3;

    pub fn new() -> Self {
        let url =
            std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
        let database = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "events".to_string());
        let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "admin".to_string());
        let password =
            std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "secret".to_string());

        let client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(user)
            .with_password(password);

        Self { client }
    }

    async fn try_insert<T>(&self, table: &str, batch: &[T]) -> clickhouse::error::Result<()>
    where
        T: RowOwned + RowWrite,
    {
        let mut insert = self.client.insert::<T>(table).await?;
        for row in batch {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn insert_batch<T>(&self, table: &str, batch: &[T]) -> clickhouse::error::Result<()>
    where
        T: RowOwned + RowWrite,
    {
        let started = Instant::now();
        let result = self.insert_with_retries(table, batch).await;

        // whole call, retries included: this is the latency the batcher actually waits
        metrics::histogram!("worker_insert_duration_seconds", "table" => table.to_string())
            .record(started.elapsed().as_secs_f64());
        match &result {
            Ok(()) => {
                metrics::histogram!("worker_batch_rows", "table" => table.to_string())
                    .record(batch.len() as f64);
            }
            // final failure, after all retries
            Err(_) => {
                metrics::counter!("worker_insert_errors_total", "table" => table.to_string())
                    .increment(1);
            }
        }
        result
    }

    async fn insert_with_retries<T>(
        &self,
        table: &str,
        batch: &[T],
    ) -> clickhouse::error::Result<()>
    where
        T: RowOwned + RowWrite,
    {
        let mut attempt = 0;

        loop {
            match self.try_insert(table, batch).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < Self::MAX_RETRIES => {
                    metrics::counter!("worker_insert_retries_total", "table" => table.to_string())
                        .increment(1);
                    let delay = Duration::from_millis(100 * 2u64.pow(attempt)); // 100, 200, 400
                    tracing::warn!(table, attempt, error = %e, ?delay, "clickhouse insert failed, retrying");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    // the public methods become one-liners
    pub async fn write_clicks(&self, batch: &[ClickRow]) -> clickhouse::error::Result<()> {
        self.insert_batch("clicks", batch).await
    }
    pub async fn write_views(&self, batch: &[ViewRow]) -> clickhouse::error::Result<()> {
        self.insert_batch("views", batch).await
    }
    pub async fn write_purchases(&self, batch: &[PurchaseRow]) -> clickhouse::error::Result<()> {
        self.insert_batch("purchases", batch).await
    }
}
