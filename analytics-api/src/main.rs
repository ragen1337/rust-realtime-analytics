use actix_web::middleware::from_fn;
use actix_web::{App, HttpServer, web};

use cache::build_cache;
use config::{path_config, query_config};
use reader::ChReader;

mod cache;
mod config;
mod error;
mod events;
mod metrics;
mod reader;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let port = std::env::var("PORT").unwrap_or("8080".to_string());
    let addr = format!("{}:{}", host, port);

    tracing::info!(%addr, "starting analytics-api");

    let metrics_handle = web::Data::new(metrics::install());
    let reader = web::Data::new(ChReader::new());
    let cache = web::Data::new(build_cache());

    HttpServer::new(move || {
        App::new()
            .app_data(query_config())
            .app_data(path_config())
            .app_data(reader.clone())
            .app_data(cache.clone())
            .app_data(metrics_handle.clone())
            .wrap(from_fn(metrics::track))
            .service(metrics::render)
            .service(
                web::scope("/api/v1/analytics")
                    .service(events::handlers::health)
                    .service(events::handlers::top_products)
                    .service(events::handlers::user_activity)
                    .service(events::handlers::conversion_rate)
                    .service(events::handlers::product_revenue)
                    .service(events::handlers::realtime_stats),
            )
            // any unmatched path -> our JSON envelope with 404
            .default_service(web::route().to(events::handlers::not_found))
    })
    .shutdown_timeout(30)
    .bind(&addr)?
    .run()
    .await?;

    Ok(())
}
