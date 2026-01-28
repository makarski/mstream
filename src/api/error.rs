use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::{error, warn};

use crate::{
    api::types::Message, job_manager::error::JobManagerError,
    middleware::udf::rhai::RhaiMiddlewareError, schema::introspect::SchemaIntrospectError,
};

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error(transparent)]
    JobManager(#[from] JobManagerError),

    #[error(transparent)]
    SchemaIntrospect(#[from] SchemaIntrospectError),

    #[error(transparent)]
    RhaiMiddleware(#[from] RhaiMiddlewareError),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::JobManager(err) => err.response_data(),
            ApiError::SchemaIntrospect(err) => err.response_data(),
            ApiError::RhaiMiddleware(err) => err.response_data(),

            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
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

trait ApiErrorDetails {
    fn response_data(&self) -> (StatusCode, String);
}

impl ApiErrorDetails for JobManagerError {
    fn response_data(&self) -> (StatusCode, String) {
        match &self {
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
        }
    }
}

impl ApiErrorDetails for SchemaIntrospectError {
    fn response_data(&self) -> (StatusCode, String) {
        match &self {
            SchemaIntrospectError::MongodbError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("MongoDB error: {}", e),
            ),
            SchemaIntrospectError::EmptySample => (
                StatusCode::NOT_FOUND,
                "No documents found in collection".to_string(),
            ),
        }
    }
}

impl ApiErrorDetails for RhaiMiddlewareError {
    fn response_data(&self) -> (StatusCode, String) {
        match &self {
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
                "Script must define a 'transform(input, attributes)' function".to_string(),
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    // Helper to extract status and message from response
    async fn extract_response(response: Response) -> (StatusCode, String) {
        let status = response.status();
        let body = response.into_body();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let message = json["message"].as_str().unwrap().to_string();
        (status, message)
    }

    /// Helper to assert an API error returns expected status and message substring
    async fn assert_api_error(error: ApiError, expected_status: StatusCode, expected_msg: &str) {
        let (status, message) = extract_response(error.into_response()).await;
        assert_eq!(status, expected_status, "unexpected status code");
        assert!(
            message.contains(expected_msg),
            "expected message containing '{}', got '{}'",
            expected_msg,
            message
        );
    }

    mod job_manager_error_tests {
        use super::*;

        #[tokio::test]
        async fn job_not_found_returns_404() {
            let error: ApiError = JobManagerError::JobNotFound("test-job".to_string()).into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::NOT_FOUND);
            assert!(message.contains("test-job"));
            assert!(message.contains("not found"));
        }

        #[tokio::test]
        async fn job_already_exists_returns_409() {
            let error: ApiError = JobManagerError::JobAlreadyExists("my-job".to_string()).into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::CONFLICT);
            assert!(message.contains("my-job"));
        }

        #[tokio::test]
        async fn service_not_found_returns_404() {
            let error: ApiError = JobManagerError::ServiceNotFound("mongo-prod".to_string()).into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::NOT_FOUND);
            assert!(message.contains("mongo-prod"));
            assert!(message.contains("not found"));
        }

        #[tokio::test]
        async fn service_already_exists_returns_409() {
            let error: ApiError =
                JobManagerError::ServiceAlreadyExists("kafka-dev".to_string()).into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::CONFLICT);
            assert!(message.contains("kafka-dev"));
        }

        #[tokio::test]
        async fn service_in_use_returns_409() {
            let error: ApiError =
                JobManagerError::ServiceInUse("mongo".to_string(), "job1, job2".to_string()).into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::CONFLICT);
            assert!(message.contains("mongo"));
            assert!(message.contains("job1, job2"));
        }

        #[tokio::test]
        async fn internal_error_returns_500() {
            let error: ApiError =
                JobManagerError::InternalError("db connection failed".to_string()).into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(message.contains("db connection failed"));
        }

        #[tokio::test]
        async fn anyhow_error_returns_500() {
            let error: ApiError =
                JobManagerError::Anyhow(anyhow::anyhow!("unexpected failure")).into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(message.contains("unexpected failure"));
        }
    }

    mod rhai_middleware_error_tests {
        use super::*;

        #[tokio::test]
        async fn file_not_found_returns_404() {
            let error: ApiError = RhaiMiddlewareError::FileNotFound {
                path: "/scripts/missing.rhai".to_string(),
            }
            .into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::NOT_FOUND);
            assert!(message.contains("missing.rhai"));
        }

        #[tokio::test]
        async fn file_read_error_returns_500() {
            let error: ApiError = RhaiMiddlewareError::FileReadError {
                source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied"),
            }
            .into();
            assert_api_error(error, StatusCode::INTERNAL_SERVER_ERROR, "Failed to read").await;
        }

        #[tokio::test]
        async fn missing_transform_returns_400() {
            let error: ApiError = RhaiMiddlewareError::MissingTransformFunction.into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(message.contains("transform"));
        }

        #[tokio::test]
        async fn execution_error_returns_400() {
            let error: ApiError = RhaiMiddlewareError::ExecutionError {
                message: "variable not found".to_string(),
                path: "script.rhai".to_string(),
            }
            .into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(message.contains("variable not found"));
            assert!(message.contains("script.rhai"));
        }

        #[tokio::test]
        async fn io_error_returns_500() {
            let error: ApiError = RhaiMiddlewareError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "disk full",
            ))
            .into();
            assert_api_error(error, StatusCode::INTERNAL_SERVER_ERROR, "disk full").await;
        }
    }

    mod api_error_tests {
        use super::*;

        #[tokio::test]
        async fn not_found_returns_404() {
            let error = ApiError::NotFound("resource missing".to_string());
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::NOT_FOUND);
            assert!(message.contains("resource missing"));
        }

        #[tokio::test]
        async fn internal_returns_500() {
            let error = ApiError::Internal("something broke".to_string());
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(message.contains("something broke"));
        }

        #[tokio::test]
        async fn schema_introspect_empty_sample_returns_404() {
            let error: ApiError = SchemaIntrospectError::EmptySample.into();
            let (status, message) = extract_response(error.into_response()).await;

            assert_eq!(status, StatusCode::NOT_FOUND);
            assert!(message.contains("No documents found"));
        }
    }
}
