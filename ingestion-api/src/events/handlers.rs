use crate::error::{AppError, AppResult};
use crate::kafka::producer::EventProducer;
use actix_web::{HttpResponse, get, post, web};
use events_contract::model::{ClickEvent, PurchaseEvent, ViewEvent};
use events_contract::topics::KafkaTopic;
use serde_json::json;

#[get("/health")]
pub async fn health() -> AppResult<HttpResponse> {
    Ok(HttpResponse::Ok().body("OK!"))
}

/// Fallback for unmatched paths: returns our JSON envelope with 404
/// instead of actix's empty default 404. Wired up via `default_service` in main.
pub async fn not_found() -> AppResult<HttpResponse> {
    Err(AppError::NotFound)
}

#[post("/click")]
pub async fn click(
    info: web::Json<ClickEvent>,
    producer: web::Data<EventProducer>,
) -> AppResult<HttpResponse> {
    let event = info.into_inner();

    let payload = serde_json::to_string(&event)?;

    producer
        .send_event(
            KafkaTopic::ClickEvents.as_ref(),
            &event.context.user_id.to_string(),
            &payload,
        )
        .await?;

    Ok(HttpResponse::Ok().json(json!({ "status": "ok" })))
}

#[post("/view")]
pub async fn view(
    info: web::Json<ViewEvent>,
    producer: web::Data<EventProducer>,
) -> AppResult<HttpResponse> {
    let event = info.into_inner();

    let payload = serde_json::to_string(&event)?;

    producer
        .send_event(
            KafkaTopic::ViewEvents.as_ref(),
            &event.context.user_id.to_string(),
            &payload,
        )
        .await?;

    Ok(HttpResponse::Ok().json(json!({ "status": "ok" })))
}

#[post("/purchase")]
pub async fn purchase(
    info: web::Json<PurchaseEvent>,
    producer: web::Data<EventProducer>,
) -> AppResult<HttpResponse> {
    let event = info.into_inner();

    let payload = serde_json::to_string(&event)?;

    producer
        .send_event(
            KafkaTopic::PurchaseEvents.as_ref(),
            &event.context.user_id.to_string(),
            &payload,
        )
        .await?;

    Ok(HttpResponse::Ok().json(json!({ "status": "ok" })))
}
