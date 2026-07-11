use clap::{Parser, ValueEnum};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::time::{Duration, MissedTickBehavior, interval};
use uuid::Uuid;

use events_contract::model::{ClickEvent, EventContext, ProductEventMetadata};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Method {
    /// POST with a randomized ClickEvent JSON body (ingestion endpoints)
    Post,
    /// GET with no body (analytics endpoints; query params go in --url)
    Get,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Full target URL, including path and query params
    #[arg(long, default_value = "http://localhost:8080/api/v1/events/click")]
    url: String,

    /// HTTP method: post (ingestion, JSON body) or get (analytics, no body)
    #[arg(long, value_enum, default_value = "post")]
    method: Method,

    /// Target request rate (requests started per second)
    #[arg(long, default_value = "1000")]
    rps: u32,

    /// Test duration in seconds
    #[arg(long, default_value = "10")]
    duration: u32,

    /// Max requests in flight at once
    #[arg(long, default_value = "50")]
    concurrency: u32,
}

fn make_event() -> ClickEvent {
    ClickEvent {
        context: EventContext {
            event_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            product_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
        },
        metadata: ProductEventMetadata {
            source: "search".to_string(),
            position: 1,
            category: "electronics".to_string(),
        },
    }
}

/// p-th percentile of a sorted latency list (e.g. p = 0.99).
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    assert!(!sorted.is_empty(), "no latencies collected");
    let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[idx]
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = reqwest::Client::new();

    let total = args.rps * args.duration;
    let url = args.url.clone();
    let method = args.method;

    let semaphore = Arc::new(Semaphore::new(args.concurrency as usize));
    let mut handles = Vec::with_capacity(total as usize);

    // Pacing: start one request every 1/rps seconds. If the semaphore stalls
    // the loop, skip missed ticks instead of firing a burst to catch up.
    let period = Duration::from_secs_f64(1.0 / args.rps as f64);
    let mut ticker = interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let run_start = Instant::now();
    for _ in 0..total {
        ticker.tick().await;
        // ждём свободный слот: если в полёте уже `concurrency` задач — тут пауза
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let client = client.clone();
        let url = url.clone();

        let handle = tokio::spawn(async move {
            let req = match method {
                Method::Post => client.post(&url).json(&make_event()),
                Method::Get => client.get(&url),
            };

            let start = Instant::now();
            let resp = req.send().await;
            let latency = start.elapsed();

            drop(permit); // освобождаем слот
            let ok = matches!(resp, Ok(r) if r.status().is_success());

            (latency, ok)
        });
        handles.push(handle);
    }

    let mut latencies = Vec::with_capacity(total as usize);
    let mut ok = 0u32;
    for handle in handles {
        if let Ok((lat, success)) = handle.await {
            latencies.push(lat);
            if success {
                ok += 1;
            }
        }
    }
    let elapsed = run_start.elapsed();

    latencies.sort();

    let achieved_rps = total as f64 / elapsed.as_secs_f64();
    let success_rate = ok as f64 / total as f64 * 100.0;

    println!("target:       {:?} {}", method, args.url);
    println!("requests:     {total}");
    println!("duration:     {:.2}s", elapsed.as_secs_f64());
    println!("achieved RPS: {:.0}", achieved_rps);
    println!("success rate: {:.2}%", success_rate);
    println!("latency p50:  {:?}", percentile(&latencies, 0.50));
    println!("latency p99:  {:?}", percentile(&latencies, 0.99));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_picks_expected_elements() {
        let lat: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        assert_eq!(percentile(&lat, 0.50), Duration::from_millis(51));
        assert_eq!(percentile(&lat, 0.99), Duration::from_millis(100));
        assert_eq!(percentile(&lat, 0.0), Duration::from_millis(1));
    }

    #[test]
    fn percentile_single_element() {
        let lat = vec![Duration::from_millis(7)];
        assert_eq!(percentile(&lat, 0.50), Duration::from_millis(7));
        assert_eq!(percentile(&lat, 0.99), Duration::from_millis(7));
    }

    #[test]
    fn make_event_randomizes_ids() {
        let a = make_event();
        let b = make_event();
        assert_ne!(a.context.event_id, b.context.event_id);
        assert_ne!(a.context.user_id, b.context.user_id);
    }
}
