use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::info;

use crate::job_manager::{JobManager, JobMetadata};

#[derive(Clone)]
struct AppState {
    job_manager: Arc<Mutex<JobManager>>,
}

pub async fn start_server(jb: Arc<Mutex<JobManager>>, port: u16) -> anyhow::Result<()> {
    let state = AppState { job_manager: jb };

    let app = Router::new()
        .route("/jobs", get(list_jobs))
        .route("/jobs/{id}", delete(stop_job))
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
    let jobs = jm.list_jobs().into_iter().collect::<Vec<JobMetadata>>();

    Json(jobs)
}

/// DELETE /jobs/{id}
async fn stop_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Json<Message<JobMetadata>> {
    info!("stopping job: {}", id);
    let mut jm = state.job_manager.lock().await;
    match jm.stop_job(&id).await {
        Ok(job_metadata) => Json(Message {
            message: format!("job {} stopped successfully", id),
            item: Some(job_metadata),
        }),
        Err(e) => Json(Message {
            message: format!("failed to stop job {}: {}", id, e),
            item: None,
        }),
    }
}

#[derive(Serialize)]
struct Message<T> {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    item: Option<T>,
}
