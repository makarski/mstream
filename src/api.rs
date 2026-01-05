use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::config::{Connector, Masked, Service};
use crate::job_manager::{JobManager, JobMetadata};

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

#[derive(Clone)]
pub struct AppState {
    pub(crate) job_manager: Arc<Mutex<JobManager>>,
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
) -> impl IntoResponse {
    let jm = state.job_manager.lock().await;
    let service = service_cfg.clone();
    match jm.create_service(service_cfg).await {
        Ok(()) => (
            StatusCode::CREATED,
            MaskedJson(Message {
                message: "service created successfully".to_string(),
                item: Some(service),
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            MaskedJson(Message {
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
struct Message<T> {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    item: Option<T>,
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
