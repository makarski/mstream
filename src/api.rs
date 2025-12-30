use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::config::{Connector, Service};
use crate::job_manager::{JobManager, JobMetadata, ServiceStatus};
use crate::mcp;

#[derive(Clone)]
pub struct AppState {
    job_manager: Arc<Mutex<JobManager>>,
    mcp_state: mcp::http::McpState,
}

impl AppState {
    pub fn new(jb: Arc<Mutex<JobManager>>) -> Self {
        Self {
            job_manager: jb.clone(),
            mcp_state: mcp::http::McpState::new(jb),
        }
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
        .route("/mcp", post(handle_mcp))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("web server listening on: {}", addr);

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// POST /mcp
async fn handle_mcp(State(state): State<AppState>, Json(payload): Json<Value>) -> Response {
    mcp::http::handle_mcp_request(&state.mcp_state, payload).await
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
    Path(id): Path<String>,
) -> (StatusCode, Json<Message<String>>) {
    info!("stopping job: {}", id);
    let mut jm = state.job_manager.lock().await;
    match jm.stop_job(&id).await {
        Ok(_) => (
            StatusCode::OK,
            Json(Message {
                message: format!("job {} stopped successfully", id),
                item: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Message {
                message: format!("failed to stop job {}: {}", id, e),
                item: None,
            }),
        ),
    }
}

/// POST /jobs/{id}/restart
async fn restart_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> (StatusCode, Json<Message<String>>) {
    info!("restarting job: {}", name);
    let mut jm = state.job_manager.lock().await;
    match jm.restart_job(&name).await {
        Ok(_) => (
            StatusCode::OK,
            Json(Message {
                message: format!("job {} restarted successfully", name),
                item: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Message {
                message: format!("failed to restart job {}: {}", name, e),
                item: None,
            }),
        ),
    }
}

/// POST /jobs
async fn create_start_job(
    State(state): State<AppState>,
    Json(conn_cfg): Json<Connector>,
) -> (StatusCode, Json<Message<JobMetadata>>) {
    info!("creating new job: {}", conn_cfg.name);

    let mut jm = state.job_manager.lock().await;
    match jm.start_job(conn_cfg).await {
        Ok(job_metadata) => (
            StatusCode::CREATED,
            Json(Message {
                message: "job created successfully".to_string(),
                item: Some(job_metadata),
            }),
        ),
        Err(e) => {
            error!("{}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(Message {
                    message: format!("failed to create job: {}", e),
                    item: None,
                }),
            )
        }
    }
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
) -> (StatusCode, Json<Message<Service>>) {
    let jm = state.job_manager.lock().await;
    let service = service_cfg.clone();
    match jm.create_service(service_cfg).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(Message {
                message: "service created successfully".to_string(),
                item: Some(service),
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Message {
                message: format!("failed to create service: {}", e),
                item: None,
            }),
        ),
    }
}

/// DELETE /services/{name}
async fn remove_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> (StatusCode, Json<Message<()>>) {
    let jm = state.job_manager.lock().await;
    match jm.remove_service(&name).await {
        Ok(()) => (
            StatusCode::OK,
            Json(Message {
                message: format!("service {} removed successfully", name),
                item: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Message {
                message: format!("failed to remove service {}: {}", name, e),
                item: None,
            }),
        ),
    }
}

async fn get_one_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> (StatusCode, Json<Option<Service>>) {
    let jm = state.job_manager.lock().await;
    match jm.get_service(&name).await {
        Ok(service) => (StatusCode::OK, Json(Some(service))),
        Err(err) => {
            error!("failed to get a service: {}: {}", name, err);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(None))
        }
    }
}

#[derive(Serialize)]
struct Message<T> {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    item: Option<T>,
}
