use std::time::{Duration, Instant};

use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{Error, HttpResponse, get, web};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

/// Latency buckets tuned for a millisecond-scale API: 0.5ms .. 2.5s.
const LATENCY_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
];

/// Installs the global recorder. Must be called once, inside the actix runtime.
pub fn install() -> PrometheusHandle {
    // Without explicit buckets the exporter renders summaries, which cannot be
    // aggregated across instances or fed to histogram_quantile().
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), LATENCY_BUCKETS)
        .expect("valid buckets")
        .install_recorder()
        .expect("metrics recorder installed twice");

    // No built-in listener here, so histogram maintenance is ours to drive.
    let upkeep = handle.clone();
    actix_web::rt::spawn(async move {
        let mut tick = actix_web::rt::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            upkeep.run_upkeep();
        }
    });
    handle
}

/// Prometheus text exposition, served at the root (outside the /api/v1 scope).
#[get("/metrics")]
pub async fn render(handle: web::Data<PrometheusHandle>) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(handle.render())
}

/// Records request count and latency. `path` is the route template
/// (e.g. `/users/{id}`), never the raw URL, so label cardinality stays bounded.
pub async fn track(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    // scraping must not pollute the numbers it reports
    if req.path() == "/metrics" {
        return next.call(req).await;
    }

    let method = req.method().to_string();
    let start = Instant::now();
    let res = next.call(req).await;
    let elapsed = start.elapsed().as_secs_f64();

    // the pattern is resolved by routing, i.e. only known after the inner call
    let (path, status) = match &res {
        Ok(r) => (path_label(r.request().match_pattern()), r.status()),
        Err(e) => ("unmatched".to_string(), e.as_response_error().status_code()),
    };
    metrics::counter!(
        "http_requests_total",
        "method" => method.clone(),
        "path" => path.clone(),
        "status" => status.as_u16().to_string()
    )
    .increment(1);
    metrics::histogram!("http_request_duration_seconds", "method" => method, "path" => path)
        .record(elapsed);
    res
}

fn path_label(pattern: Option<String>) -> String {
    pattern.unwrap_or_else(|| "unmatched".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, middleware::from_fn, test};

    #[actix_web::test]
    async fn path_label_falls_back_for_unmatched() {
        assert_eq!(path_label(None), "unmatched");
        assert_eq!(path_label(Some("/a/{id}".into())), "/a/{id}");
    }

    // The global recorder can be installed once per process, so a single test
    // owns the whole middleware -> /metrics round trip.
    #[actix_web::test]
    async fn middleware_records_and_metrics_endpoint_renders() {
        let handle = install();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(handle))
                .wrap(from_fn(track))
                .route("/items/{id}", web::get().to(|| async { "ok" }))
                .service(render),
        )
        .await;

        test::call_service(&app, test::TestRequest::get().uri("/items/42").to_request()).await;
        test::call_service(&app, test::TestRequest::get().uri("/nope").to_request()).await;
        let req = test::TestRequest::get().uri("/metrics").to_request();
        let body = test::call_and_read_body(&app, req).await;
        let text = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            text.contains(r#"http_requests_total{method="GET",path="/items/{id}",status="200"} 1"#),
            "{text}"
        );
        assert!(text.contains(r#"path="unmatched",status="404""#), "{text}");
        assert!(
            text.contains("http_request_duration_seconds_bucket"),
            "{text}"
        );
        assert!(!text.contains(r#"path="/metrics""#), "{text}");
    }
}
