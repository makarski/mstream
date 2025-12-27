use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::config::{Connector, Service};
use crate::job_manager::{JobManager, JobMetadata, ServiceStatus};

#[derive(Clone)]
pub struct AppState {
    job_manager: Arc<Mutex<JobManager>>,
}

impl AppState {
    pub fn new(jb: Arc<Mutex<JobManager>>) -> Self {
        Self { job_manager: jb }
    }
}

pub async fn start_server(state: AppState, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/jobs", get(list_jobs))
        .route("/jobs/{id}", delete(stop_job))
        .route("/jobs", post(create_job))
        .route("/services", get(list_services))
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
async fn list_jobs(State(state): State<AppState>) -> Json<Vec<JobMetadata>> {
    let jm = state.job_manager.lock().await;
    let jobs = jm.list_jobs();

    Json(jobs)
}

/// DELETE /jobs/{id}
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

/// POST /jobs
async fn create_job(
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
async fn list_services(State(state): State<AppState>) -> Json<Vec<ServiceStatus>> {
    let jm = state.job_manager.lock().await;
    let services = jm.list_services().await;
    Json(services)
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

#[derive(Serialize)]
struct Message<T> {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    item: Option<T>,
}
