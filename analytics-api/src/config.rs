use crate::error::AppError;
use actix_web::web;

// malformed query params: ?metric=foo, broken date, missing required `from`
pub fn query_config() -> web::QueryConfig {
    web::QueryConfig::default().error_handler(|_err, _| AppError::Validation.into())
}

// malformed path param: /user-activity/not-a-uuid
pub fn path_config() -> web::PathConfig {
    web::PathConfig::default().error_handler(|_err, _| AppError::Validation.into())
}
