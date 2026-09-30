use actix_web::{HttpResponse, error::ResponseError, http::StatusCode};
use derive_more::derive::{Display, Error};
use events_contract::validate::ValidationError;

#[derive(Debug, Display, Error)]
pub enum AppError {
    #[display("Not found")]
    NotFound, // -> 404 (unknown path, see default_service in main)

    #[display("Validation error")]
    Validation, // -> 400 (malformed JSON in the request body, see config::json_config)

    #[display("invalid event: {_0}")]
    InvalidEvent(#[error(not(source))] ValidationError), // -> 400 (parsed fine but breaks a business rule, see events_contract::validate)

    #[display("Internal error")]
    Internal, // -> 500 (catch-all for anything not meant for the client)

    #[display("Service unavailable")]
    ServiceUnavailable, // -> 503 (Kafka unavailable, etc.)
}

impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::NotFound => StatusCode::NOT_FOUND,
            AppError::Validation | AppError::InvalidEvent(_) => StatusCode::BAD_REQUEST,
            AppError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(serde_json::json!({
            "error": self.to_string(),
        }))
    }
}

impl From<ValidationError> for AppError {
    fn from(e: ValidationError) -> Self {
        AppError::InvalidEvent(e)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(_: serde_json::Error) -> Self {
        AppError::Internal
    }
}

impl From<rdkafka::error::KafkaError> for AppError {
    fn from(e: rdkafka::error::KafkaError) -> Self {
        tracing::error!(error = %e, "kafka send failed");
        AppError::ServiceUnavailable
    }
}

pub type AppResult<T> = Result<T, AppError>;
