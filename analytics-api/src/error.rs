use actix_web::{HttpResponse, error::ResponseError, http::StatusCode};
use derive_more::derive::{Display, Error};

// Clone: moka's `try_get_with` hands the same error to every waiting caller
// as `Arc<AppError>`, so each handler clones its own copy out of the Arc.
#[derive(Debug, Display, Error, Clone)]
pub enum AppError {
    #[display("Not found")]
    NotFound, // -> 404 (unknown path, see default_service in main)

    #[display("Validation error")]
    Validation, // -> 400 (malformed query params: limit/period/metric, from>to)

    #[display("Internal error")]
    Internal, // -> 500 (catch-all for anything not meant for the client)

    #[display("Service unavailable")]
    ServiceUnavailable, // -> 503 (ClickHouse unavailable / timeout)
}

impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::NotFound => StatusCode::NOT_FOUND,
            AppError::Validation => StatusCode::BAD_REQUEST,
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

// The client only sees a bare 503, so the real cause must be logged here.
impl From<clickhouse::error::Error> for AppError {
    fn from(e: clickhouse::error::Error) -> Self {
        tracing::error!(error = %e, "clickhouse query failed");
        AppError::ServiceUnavailable
    }
}

// Manual JSON serialization of the response (serde_json::to_vec in handlers) can fail —
// that's our fault, not the client's, hence Internal (500).
impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        tracing::error!(error = %e, "response serialization failed");
        AppError::Internal
    }
}

pub type AppResult<T> = Result<T, AppError>;
