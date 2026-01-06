use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::config::{Connector, Masked, Service};
use crate::job_manager::{JobManager, JobManagerError, JobMetadata};

struct MaskedJson<T>(T);

impl<T> IntoResponse for MaskedJson<T>
where
    T: Serialize + Masked,
{
    fn into_response(self) -> Response {
        let masked_data = self.0.masked();
        let json_data = Json(masked_data);
        json_data.into_response()
    }
}

/// API Error type that maps to appropriate HTTP status codes
#[derive(Debug)]
pub enum ApiError {
    /// 404 Not Found - resource doesn't exist
    NotFound {
        resource: String,
        identifier: String,
    },

    /// 409 Conflict - request conflicts with current state
    Conflict(String),

    /// 500 Internal Server Error - unexpected server-side error
    InternalError(String),
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message, details) = match self {
            ApiError::NotFound {
                resource,
                identifier,
            } => (
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                format!("{} '{}' not found", resource, identifier),
                None,
            ),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, "CONFLICT", msg, None),
            ApiError::InternalError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                msg,
                None,
            ),
        };

        let error_response = ErrorResponse {
            error: ErrorDetail {
                code: code.to_string(),
                message: message.clone(),
                details,
            },
        };

        // Log errors appropriately
        match status {
            StatusCode::NOT_FOUND | StatusCode::CONFLICT => {
                warn!("[{}] {}", code, message);
            }
            _ => {
                error!("[{}] {}", code, message);
            }
        }

        (status, Json(error_response)).into_response()
    }
}

impl From<JobManagerError> for ApiError {
    fn from(err: JobManagerError) -> Self {
        match err {
            JobManagerError::JobNotFound(name) => ApiError::NotFound {
                resource: "job".to_string(),
                identifier: name,
            },
            JobManagerError::JobAlreadyExists(name) => {
                ApiError::Conflict(format!("Job '{}' already exists", name))
            }
            JobManagerError::ServiceNotFound(name) => ApiError::NotFound {
                resource: "service".to_string(),
                identifier: name,
            },
            JobManagerError::ServiceAlreadyExists(name) => {
                ApiError::Conflict(format!("Service '{}' already exists", name))
            }
            JobManagerError::InternalError(msg) => ApiError::InternalError(msg),
            JobManagerError::ServiceInUse(name, jobs) => {
                ApiError::Conflict(format!("Service '{}' is in use by jobs: {}", name, jobs))
            }
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub(crate) pub job_manager: Arc<Mutex<JobManager>>,
}

impl AppState {
    pub fn new(jb: Arc<Mutex<JobManager>>) -> Self {
        Self { job_manager: jb }
    }
}

pub async fn start_server(state: AppState, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .merge(crate::ui::ui_routes())
        .route("/jobs", get(list_jobs))
        .route("/jobs", post(create_start_job))
        .route("/jobs/{name}/stop", post(stop_job))
        .route("/jobs/{name}/restart", post(restart_job))
        .route("/services", get(list_services))
        .route("/services/{name}", get(get_one_service))
        .route("/services", post(create_service))
        .route("/services/{name}", delete(remove_service))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("web server listening on: {}", addr);

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// GET /jobs
async fn list_jobs(State(state): State<AppState>) -> (StatusCode, Json<Vec<JobMetadata>>) {
    let jm = state.job_manager.lock().await;
    match jm.list_jobs().await {
        Ok(jobs) => (StatusCode::OK, Json(jobs)),
        Err(e) => {
            error!("failed to list jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::new()))
        }
    }
}

/// POST /jobs/{name}/stop
async fn stop_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Message<()>>), ApiError> {
    info!("stopping job: {}", name);
    let mut jm = state.job_manager.lock().await;

    // Stop the job - will return JobNotFound if not found
    jm.stop_job(&name).await?;

    Ok((
        StatusCode::OK,
        Json(Message {
            message: format!("job {} stopped successfully", name),
            item: None,
        }),
    ))
}

/// POST /jobs/{name}/restart
async fn restart_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Message<JobMetadata>>), ApiError> {
    info!("restarting job: {}", name);
    let mut jm = state.job_manager.lock().await;

    // Restart the job - will return JobNotFound if not found
    let metadata = jm.restart_job(&name).await?;

    Ok((
        StatusCode::OK,
        Json(Message {
            message: format!("job {} restarted successfully", name),
            item: Some(metadata),
        }),
    ))
}

/// POST /jobs
async fn create_start_job(
    State(state): State<AppState>,
    Json(conn_cfg): Json<Connector>,
) -> Result<(StatusCode, Json<Message<JobMetadata>>), ApiError> {
    info!("creating new job: {}", conn_cfg.name);

    let mut jm = state.job_manager.lock().await;

    // Start the job - will return JobAlreadyExists if it already exists
    let job_metadata = jm.start_job(conn_cfg).await?;

    Ok((
        StatusCode::CREATED,
        Json(Message {
            message: "job created successfully".to_string(),
            item: Some(job_metadata),
        }),
    ))
}

/// GET /services
async fn list_services(State(state): State<AppState>) -> impl IntoResponse {
    let jm = state.job_manager.lock().await;
    match jm.list_services().await {
        Ok(services) => (StatusCode::OK, MaskedJson(services)).into_response(),
        Err(e) => {
            error!("failed to list services: {}", e);

            let msg: Message<()> = Message {
                message: format!("failed to list services: {}", e),
                item: None,
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response()
        }
    }
}

/// POST /services
async fn create_service(
    State(state): State<AppState>,
    Json(service_cfg): Json<Service>,
) -> Result<impl IntoResponse, ApiError> {
    info!("creating new service: {}", service_cfg.name());

    let jm = state.job_manager.lock().await;
    let service = service_cfg.clone();

    // Create the service - will return ServiceAlreadyExists if it already exists
    jm.create_service(service_cfg).await?;

    Ok((
        StatusCode::CREATED,
        MaskedJson(Message {
            message: "service created successfully".to_string(),
            item: Some(service),
        }),
    ))
}

/// DELETE /services/{name}
async fn remove_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let jm = state.job_manager.lock().await;
    match jm.remove_service(&name).await {
        Ok(()) => {
            let msg = Message::<()> {
                message: format!("service {} removed successfully", name),
                item: None,
            };
            (StatusCode::OK, Json(msg)).into_response()
        }
        Err(e) => {
            let msg = Message::<()> {
                message: format!("failed to remove service {}: {}", name, e),
                item: None,
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response()
        }
    }
}

async fn get_one_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let jm = state.job_manager.lock().await;
    match jm.get_service(&name).await {
        Ok(service) => (StatusCode::OK, MaskedJson(service)).into_response(),
        Err(err) => {
            error!("failed to get a service: {}: {}", name, err);
            let msg = Message::<()> {
                message: format!("failed to get service {}: {}", name, err),
                item: None,
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response()
        }
    }
}

#[derive(Serialize, Clone)]
pub struct Message<T> {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<T>,
}

impl<T: Masked> Masked for Message<T> {
    fn masked(&self) -> Self {
        let masked_item = match &self.item {
            Some(item) => Some(item.masked()),
            None => None,
        };

        Message {
            message: self.message.clone(),
            item: masked_item,
        }
    }
}

impl<T> IntoResponse for Message<T>
where
    T: Serialize + Masked,
{
    fn into_response(self) -> Response {
        MaskedJson(self).into_response()
    }
}

impl<T> Masked for Vec<T>
where
    T: Masked,
{
    fn masked(&self) -> Self {
        self.iter().map(|item| item.masked()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test helper that tracks whether masked() was called
    #[derive(Clone, Serialize, PartialEq, Debug)]
    struct TestItem {
        public: String,
        secret: String,
    }

    impl Masked for TestItem {
        fn masked(&self) -> Self {
            Self {
                public: self.public.clone(),
                secret: "****".to_string(),
            }
        }
    }

    mod message_masked_tests {
        use super::*;

        #[test]
        fn masks_item_when_present() {
            let msg = Message {
                message: "test message".to_string(),
                item: Some(TestItem {
                    public: "visible".to_string(),
                    secret: "hunter2".to_string(),
                }),
            };

            let masked = msg.masked();

            assert_eq!(masked.message, "test message");
            assert!(masked.item.is_some());
            let item = masked.item.unwrap();
            assert_eq!(item.public, "visible");
            assert_eq!(item.secret, "****");
        }

        #[test]
        fn preserves_none_item() {
            let msg: Message<TestItem> = Message {
                message: "no item".to_string(),
                item: None,
            };

            let masked = msg.masked();

            assert_eq!(masked.message, "no item");
            assert!(masked.item.is_none());
        }

        #[test]
        fn message_unchanged() {
            let msg = Message {
                message: "secret info in message".to_string(),
                item: Some(TestItem {
                    public: "x".to_string(),
                    secret: "y".to_string(),
                }),
            };

            let masked = msg.masked();

            // Message field is not masked, only item
            assert_eq!(masked.message, "secret info in message");
        }
    }

    mod vec_masked_tests {
        use super::*;

        #[test]
        fn masks_all_elements() {
            let items = vec![
                TestItem {
                    public: "a".to_string(),
                    secret: "secret1".to_string(),
                },
                TestItem {
                    public: "b".to_string(),
                    secret: "secret2".to_string(),
                },
                TestItem {
                    public: "c".to_string(),
                    secret: "secret3".to_string(),
                },
            ];

            let masked = items.masked();

            assert_eq!(masked.len(), 3);
            assert_eq!(masked[0].public, "a");
            assert_eq!(masked[0].secret, "****");
            assert_eq!(masked[1].public, "b");
            assert_eq!(masked[1].secret, "****");
            assert_eq!(masked[2].public, "c");
            assert_eq!(masked[2].secret, "****");
        }

        #[test]
        fn handles_empty_vec() {
            let items: Vec<TestItem> = vec![];

            let masked = items.masked();

            assert!(masked.is_empty());
        }

        #[test]
        fn handles_single_element() {
            let items = vec![TestItem {
                public: "only".to_string(),
                secret: "one".to_string(),
            }];

            let masked = items.masked();

            assert_eq!(masked.len(), 1);
            assert_eq!(masked[0].secret, "****");
        }
    }
}

impl<T: Masked> Masked for Message<T> {
    fn masked(&self) -> Self {
        let masked_item = match &self.item {
            Some(item) => Some(item.masked()),
            None => None,
        };

        Message {
            message: self.message.clone(),
            item: masked_item,
        }
    }
}

impl<T> IntoResponse for Message<T>
where
    T: Serialize + Masked,
{
    fn into_response(self) -> Response {
        MaskedJson(self).into_response()
    }
}

impl<T> Masked for Vec<T>
where
    T: Masked,
{
    fn masked(&self) -> Self {
        self.iter().map(|item| item.masked()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test helper that tracks whether masked() was called
    #[derive(Clone, Serialize, PartialEq, Debug)]
    struct TestItem {
        public: String,
        secret: String,
    }

    impl Masked for TestItem {
        fn masked(&self) -> Self {
            Self {
                public: self.public.clone(),
                secret: "****".to_string(),
            }
        }
    }

    mod message_masked_tests {
        use super::*;

        #[test]
        fn masks_item_when_present() {
            let msg = Message {
                message: "test message".to_string(),
                item: Some(TestItem {
                    public: "visible".to_string(),
                    secret: "hunter2".to_string(),
                }),
            };

            let masked = msg.masked();

            assert_eq!(masked.message, "test message");
            assert!(masked.item.is_some());
            let item = masked.item.unwrap();
            assert_eq!(item.public, "visible");
            assert_eq!(item.secret, "****");
        }

        #[test]
        fn preserves_none_item() {
            let msg: Message<TestItem> = Message {
                message: "no item".to_string(),
                item: None,
            };

            let masked = msg.masked();

            assert_eq!(masked.message, "no item");
            assert!(masked.item.is_none());
        }

        #[test]
        fn message_unchanged() {
            let msg = Message {
                message: "secret info in message".to_string(),
                item: Some(TestItem {
                    public: "x".to_string(),
                    secret: "y".to_string(),
                }),
            };

            let masked = msg.masked();

            // Message field is not masked, only item
            assert_eq!(masked.message, "secret info in message");
        }
    }

    mod vec_masked_tests {
        use super::*;

        #[test]
        fn masks_all_elements() {
            let items = vec![
                TestItem {
                    public: "a".to_string(),
                    secret: "secret1".to_string(),
                },
                TestItem {
                    public: "b".to_string(),
                    secret: "secret2".to_string(),
                },
                TestItem {
                    public: "c".to_string(),
                    secret: "secret3".to_string(),
                },
            ];

            let masked = items.masked();

            assert_eq!(masked.len(), 3);
            assert_eq!(masked[0].public, "a");
            assert_eq!(masked[0].secret, "****");
            assert_eq!(masked[1].public, "b");
            assert_eq!(masked[1].secret, "****");
            assert_eq!(masked[2].public, "c");
            assert_eq!(masked[2].secret, "****");
        }

        #[test]
        fn handles_empty_vec() {
            let items: Vec<TestItem> = vec![];

            let masked = items.masked();

            assert!(masked.is_empty());
        }

        #[test]
        fn handles_single_element() {
            let items = vec![TestItem {
                public: "only".to_string(),
                secret: "one".to_string(),
            }];

            let masked = items.masked();

            assert_eq!(masked.len(), 1);
            assert_eq!(masked[0].secret, "****");
        }
    }
}
