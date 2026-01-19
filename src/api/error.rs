use axum::{
    Json,
    response::{IntoResponse, Response},
};
use reqwest::StatusCode;
use tracing::{error, warn};

use crate::{
    api::types::Message, job_manager::error::JobManagerError,
    middleware::udf::rhai::RhaiMiddlewareError,
};

impl IntoResponse for JobManagerError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            JobManagerError::JobNotFound(name) => {
                (StatusCode::NOT_FOUND, format!("Job '{}' not found", name))
            }
            JobManagerError::JobAlreadyExists(name) => (
                StatusCode::CONFLICT,
                format!("Job '{}' already exists", name),
            ),
            JobManagerError::ServiceNotFound(name) => (
                StatusCode::NOT_FOUND,
                format!("Service '{}' not found", name),
            ),
            JobManagerError::ServiceAlreadyExists(name) => (
                StatusCode::CONFLICT,
                format!("Service '{}' already exists", name),
            ),
            JobManagerError::ServiceInUse(name, used_by) => (
                StatusCode::CONFLICT,
                format!("Service '{}' is in use by jobs: {}", name, used_by),
            ),
            JobManagerError::InternalError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal error: {}", msg),
            ),
            JobManagerError::Anyhow(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal error: {}", e),
            ),
        };

        if status.is_server_error() {
            error!("API Error: {}", message);
        } else {
            warn!("API Client Error: {}", message);
        }

        let body = Json(Message {
            message,
            item: None::<()>,
        });

        (status, body).into_response()
    }
}

impl IntoResponse for RhaiMiddlewareError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            RhaiMiddlewareError::FileNotFound { path } => (
                StatusCode::NOT_FOUND,
                format!("Script file not found: {}", path),
            ),
            RhaiMiddlewareError::FileReadError { source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to read script file: {}", source),
            ),
            RhaiMiddlewareError::CompileError { source } => (
                StatusCode::BAD_REQUEST,
                format!("Script compilation failed: {}", source),
            ),
            RhaiMiddlewareError::MissingTransformFunction => (
                StatusCode::BAD_REQUEST,
                "Script must define a 'transform(doc, meta)' function".to_string(),
            ),
            RhaiMiddlewareError::ExecutionError { message, path } => (
                StatusCode::BAD_REQUEST,
                format!("Script execution failed: {} ({})", message, path),
            ),
            RhaiMiddlewareError::DecodeError { source, path } => (
                StatusCode::BAD_REQUEST,
                format!("Failed to decode output: {} ({})", source, path),
            ),
            RhaiMiddlewareError::IoError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("IO error: {}", e),
            ),
        };

        if status.is_server_error() {
            error!("Transform API Error: {}", message);
        } else {
            warn!("Transform API Error: {}", message);
        }

        let body = Json(Message {
            message,
            item: None::<()>,
        });

        (status, body).into_response()
    }
}
