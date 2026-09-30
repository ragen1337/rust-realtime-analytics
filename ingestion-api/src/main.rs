use crate::config::json_config;
use crate::kafka::producer::EventProducer;
use actix_web::middleware::from_fn;
use actix_web::{App, HttpServer, web};

mod config;
mod error;
mod events;
mod kafka;
mod metrics;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let metrics_handle = web::Data::new(metrics::install());
    let producer = web::Data::new(EventProducer::new());
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let port = std::env::var("PORT").unwrap_or("8080".to_string());
    let addr = format!("{}:{}", host, port);

    tracing::info!(%addr, "starting ingestion-api");

    let producer_for_shutdown = producer.clone();

    HttpServer::new(move || {
        App::new()
            .app_data(producer.clone())
            .app_data(metrics_handle.clone())
            .app_data(json_config())
            .wrap(from_fn(metrics::track))
            .service(metrics::render)
            .service(
                web::scope("/api/v1/events")
                    .service(events::handlers::health)
                    .service(events::handlers::click)
                    .service(events::handlers::view)
                    .service(events::handlers::purchase),
            )
            // any unmatched path -> our JSON envelope with 404
            .default_service(web::route().to(events::handlers::not_found))
    })
    .shutdown_timeout(30)
    .bind(&addr)?
    .run()
    .await?;

    producer_for_shutdown.flush(std::time::Duration::from_secs(10));

    Ok(())
}
