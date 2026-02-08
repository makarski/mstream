use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use tracing::info;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::{CheckpointResponse, Message};
use crate::config::Connector;
use crate::job_manager::{JobMetadata, error::JobManagerError};

/// GET /jobs
pub async fn list_jobs(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let jm = state.job_manager.lock().await;
    let jobs = jm
        .list_jobs()
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to list jobs: {}", e)))?;

    Ok((StatusCode::OK, Json(jobs)))
}

/// POST /jobs/{name}/stop
pub async fn stop_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Message<()>>), ApiError> {
    info!(job_name = %name, "stopping job");
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
pub async fn restart_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Message<JobMetadata>>), ApiError> {
    info!(job_name = %name, "restarting job");
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
pub async fn create_start_job(
    State(state): State<AppState>,
    Json(conn_cfg): Json<Connector>,
) -> Result<(StatusCode, Json<Message<JobMetadata>>), ApiError> {
    info!(job_name = %conn_cfg.name, "creating new job");

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

/// GET /jobs/{name}/checkpoints
pub async fn list_checkpoints(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let jm = state.job_manager.lock().await;

    // Verify job exists
    jm.get_job(&name)
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to get job: {}", e)))?
        .ok_or_else(|| JobManagerError::JobNotFound(name.clone()))?;

    let checkpoints: Vec<CheckpointResponse> = jm
        .list_checkpoints(&name)
        .await
        .into_iter()
        .map(CheckpointResponse::from)
        .collect();

    Ok((StatusCode::OK, Json(checkpoints)))
}
