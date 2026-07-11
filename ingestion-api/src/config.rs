use crate::error::AppError;
use actix_web::web;

pub fn json_config() -> web::JsonConfig {
    web::JsonConfig::default().error_handler(|_err, _| AppError::Validation.into())
}
