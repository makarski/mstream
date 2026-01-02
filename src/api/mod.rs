use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::config::{Connector, Service};
use crate::job_manager::{JobManager, JobMetadata, ServiceStatus, JobManagerError};

pub mod validation;

pub use validation::{Validate, ValidationError, validate_resource_name};

/// API Error type that maps to appropriate HTTP status codes
#[derive(Debug)]
pub enum ApiError {
    /// 400 Bad Request - client sent invalid data
    BadRequest(String),
    
    /// 400 Bad Request - input validation failed
    InvalidInput { 
        field: String, 
        reason: String 
    },
    
    /// 404 Not Found - resource doesn't exist
    NotFound { 
        resource: String, 
        identifier: String 
    },
    
    /// 409 Conflict - request conflicts with current state
    Conflict(String),
    
    /// 500 Internal Server Error - unexpected server-side error
    InternalError(String),
}

impl ApiError {
    /// Create a "job not found" error
    pub fn job_not_found(name: &str) -> Self {
        ApiError::NotFound {
            resource: "job".to_string(),
            identifier: name.to_string(),
        }
    }

    /// Create a "service not found" error
    pub fn service_not_found(name: &str) -> Self {
        ApiError::NotFound {
            resource: "service".to_string(),
            identifier: name.to_string(),
        }
    }

    /// Create a "job already exists" error
    pub fn job_already_exists(name: &str) -> Self {
        ApiError::Conflict(format!("Job '{}' already exists", name))
    }

    /// Create a "service already exists" error
    pub fn service_already_exists(name: &str) -> Self {
        ApiError::Conflict(format!("Service '{}' already exists", name))
    }

    /// Create an "invalid field" error
    pub fn invalid_field(field: &str, reason: &str) -> Self {
        ApiError::InvalidInput {
            field: field.to_string(),
            reason: reason.to_string(),
        }
    }
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
            ApiError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                msg,
                None,
            ),
            ApiError::InvalidInput { field, reason } => (
                StatusCode::BAD_REQUEST,
                "INVALID_INPUT",
                format!("Invalid field '{}': {}", field, reason),
                Some(json!({
                    "field": field,
                    "reason": reason
                })),
            ),
            ApiError::NotFound { resource, identifier } => (
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                format!("{} '{}' not found", resource, identifier),
                None,
            ),
            ApiError::Conflict(msg) => (
                StatusCode::CONFLICT,
                "CONFLICT",
                msg,
                None,
            ),
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
            StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND | StatusCode::CONFLICT => {
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
            JobManagerError::JobNotFound(name) => {
                ApiError::NotFound {
                    resource: "job".to_string(),
                    identifier: name,
                }
            }
            JobManagerError::JobAlreadyExists(name) => {
                ApiError::Conflict(format!("Job '{}' already exists", name))
            }
            JobManagerError::ServiceNotFound(name) => {
                ApiError::NotFound {
                    resource: "service".to_string(),
                    identifier: name,
                }
            }
            JobManagerError::ServiceInUse(name, jobs) => {
                ApiError::Conflict(format!(
                    "Service '{}' is in use by jobs: {}",
                    name,
                    jobs.join(", ")
                ))
            }
            JobManagerError::MissingPipeline(name) => {
                ApiError::InternalError(format!(
                    "Cannot restart job '{}': missing pipeline configuration",
                    name
                ))
            }
            JobManagerError::InvalidServiceReference(reason) => {
                ApiError::InternalError(format!("Invalid service reference: {}", reason))
            }
            JobManagerError::StorageError(err) => {
                ApiError::InternalError(format!("Storage error: {}", err))
            }
        }
    }
}


#[derive(Clone)]
pub struct AppState {
    pub job_manager: Arc<Mutex<JobManager>>,
}

impl AppState {
    pub fn new(jb: Arc<Mutex<JobManager>>) -> Self {
        Self { job_manager: jb }
    }
}

pub async fn start_server(state: AppState, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
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

/// POST /jobs/{id}/stop
async fn stop_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Message<String>>), ApiError> {
    // Validate job name format
    validate_resource_name(&name, "job")
        .map_err(|e| ApiError::InvalidInput {
            field: e.field,
            reason: e.reason,
        })?;

    info!("stopping job: {}", name);
    let mut jm = state.job_manager.lock().await;
    
    // Stop the job (returns JobNotFound error if not found)
    jm.stop_job(&name).await.map_err(|e| {
        // Convert anyhow::Error to JobManagerError for proper error mapping
        if e.to_string().contains("not found") {
            JobManagerError::JobNotFound(name.clone()).into()
        } else {
            ApiError::InternalError(format!("failed to stop job: {}", e))
        }
    })?;

    Ok((
        StatusCode::OK,
        Json(Message {
            message: format!("job {} stopped successfully", name),
            item: None,
        }),
    ))
}

/// POST /jobs/{id}/restart
async fn restart_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Message<JobMetadata>>), ApiError> {
    // Validate job name format
    validate_resource_name(&name, "job")
        .map_err(|e| ApiError::InvalidInput {
            field: e.field,
            reason: e.reason,
        })?;

    info!("restarting job: {}", name);
    let mut jm = state.job_manager.lock().await;
    
    let metadata = jm.restart_job(&name).await.map_err(|e| {
        // Convert anyhow::Error to appropriate ApiError
        let err_str = e.to_string();
        if err_str.contains("not found") {
            ApiError::NotFound {
                resource: "job".to_string(),
                identifier: name.clone(),
            }
        } else if err_str.contains("missing pipeline") {
            ApiError::InternalError(format!("Cannot restart job '{}': missing pipeline configuration", name))
        } else {
            ApiError::InternalError(format!("failed to restart job: {}", e))
        }
    })?;

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
    // Validate connector name format
    validate_resource_name(&conn_cfg.name, "job")
        .map_err(|e| ApiError::InvalidInput {
            field: e.field,
            reason: e.reason,
        })?;

    info!("creating new job: {}", conn_cfg.name);

    let mut jm = state.job_manager.lock().await;
    
    // Check if job already exists
    let job_exists = jm.list_jobs().await
        .ok()
        .map(|jobs| jobs.iter().any(|j| j.name == conn_cfg.name))
        .unwrap_or(false);
    
    if job_exists {
        return Err(ApiError::job_already_exists(&conn_cfg.name));
    }

    // Start the job
    let job_metadata = jm.start_job(conn_cfg).await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("not found") || err_str.contains("service") {
            ApiError::NotFound {
                resource: "service".to_string(),
                identifier: "unknown".to_string(),
            }
        } else if err_str.contains("already") {
            ApiError::Conflict(err_str)
        } else {
            ApiError::InternalError(format!("failed to create job: {}", e))
        }
    })?;

    Ok((
        StatusCode::CREATED,
        Json(Message {
            message: "job created successfully".to_string(),
            item: Some(job_metadata),
        }),
    ))
}

/// GET /services
async fn list_services(State(state): State<AppState>) -> (StatusCode, Json<Vec<ServiceStatus>>) {
    let jm = state.job_manager.lock().await;
    match jm.list_services().await {
        Ok(services) => (StatusCode::OK, Json(services)),
        Err(e) => {
            error!("failed to list services: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::new()))
        }
    }
}

/// POST /services
async fn create_service(
    State(state): State<AppState>,
    Json(service_cfg): Json<Service>,
) -> Result<(StatusCode, Json<Message<Service>>), ApiError> {
    let service_name = service_cfg.name();
    
    // Validate service name format
    validate_resource_name(service_name, "service")
        .map_err(|e| ApiError::InvalidInput {
            field: e.field,
            reason: e.reason,
        })?;

    let jm = state.job_manager.lock().await;
    
    // Check if service already exists
    let service_exists = jm.list_services().await
        .ok()
        .map(|services| services.iter().any(|s| s.service.name() == service_name))
        .unwrap_or(false);
    
    if service_exists {
        return Err(ApiError::service_already_exists(service_name));
    }

    let service = service_cfg.clone();
    
    // Create the service
    jm.create_service(service_cfg).await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("invalid") || err_str.contains("format") {
            ApiError::BadRequest(format!("Invalid service configuration: {}", err_str))
        } else {
            ApiError::InternalError(format!("failed to create service: {}", err_str))
        }
    })?;

    Ok((
        StatusCode::CREATED,
        Json(Message {
            message: "service created successfully".to_string(),
            item: Some(service),
        }),
    ))
}

/// DELETE /services/{name}
async fn remove_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Message<()>>), ApiError> {
    // Validate service name format
    validate_resource_name(&name, "service")
        .map_err(|e| ApiError::InvalidInput {
            field: e.field,
            reason: e.reason,
        })?;

    let jm = state.job_manager.lock().await;
    
    // This will return ServiceInUse error if jobs are using it
    jm.remove_service(&name).await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("not found") {
            ApiError::NotFound {
                resource: "service".to_string(),
                identifier: name.clone(),
            }
        } else if err_str.contains("in use") {
            ApiError::Conflict(err_str)
        } else {
            ApiError::InternalError(format!("failed to remove service: {}", e))
        }
    })?;

    Ok((
        StatusCode::OK,
        Json(Message {
            message: format!("service {} removed successfully", name),
            item: None,
        }),
    ))
}

async fn get_one_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Service>), ApiError> {
    // Validate service name format
    validate_resource_name(&name, "service")
        .map_err(|e| ApiError::InvalidInput {
            field: e.field,
            reason: e.reason,
        })?;

    let jm = state.job_manager.lock().await;
    let service = jm.get_service(&name).await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("not found") {
            ApiError::NotFound {
                resource: "service".to_string(),
                identifier: name.clone(),
            }
        } else {
            ApiError::InternalError(format!("failed to get service: {}", e))
        }
    })?;

    Ok((StatusCode::OK, Json(service)))
}

#[derive(Serialize)]
struct Message<T> {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    item: Option<T>,
}
