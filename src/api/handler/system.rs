use std::sync::atomic::Ordering;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::{HealthStatus, JobMetrics, SystemStats};
use crate::job_manager::error::JobManagerError;

/// GET /health
pub async fn health(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let jm = state.job_manager.lock().await;
    let counts = jm
        .job_state_counts()
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to get job counts: {}", e)))?;

    let status = if counts.failed > 0 {
        "degraded"
    } else {
        "healthy"
    };

    let uptime = state.start_time.elapsed().as_secs();

    Ok((
        StatusCode::OK,
        Json(HealthStatus {
            status,
            version: env!("CARGO_PKG_VERSION"),
            uptime_seconds: uptime,
        }),
    ))
}

/// GET /stats
pub async fn stats(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let jm = state.job_manager.lock().await;
    let counts = jm
        .job_state_counts()
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to get job counts: {}", e)))?;

    let uptime = state.start_time.elapsed().as_secs();

    // TODO: wire total_docs_processed / total_bytes_transferred once
    // in-engine metrics collection is implemented
    Ok((
        StatusCode::OK,
        Json(SystemStats {
            total_docs_processed: 0,
            total_bytes_transferred: 0,
            uptime_seconds: uptime,
            running_jobs: counts.running,
            stopped_jobs: counts.stopped,
            error_jobs: counts.failed,
        }),
    ))
}

/// GET /jobs/{name}/metrics
pub async fn job_metrics(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let jm = state.job_manager.lock().await;

    let job = jm
        .get_job(&name)
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to get job: {}", e)))?
        .ok_or_else(|| JobManagerError::JobNotFound(name.clone()))?;

    let metrics = match jm.job_metrics(&name) {
        Some(m) => {
            let events = m.events_processed.load(Ordering::Relaxed);
            let bytes = m.bytes_processed.load(Ordering::Relaxed);
            let errors = m.errors.load(Ordering::Relaxed);
            let last_ts = m.last_processed_at.load(Ordering::Relaxed);

            let elapsed = (chrono::Utc::now() - job.started_at).num_seconds().max(1) as f64;
            let throughput = events as f64 / elapsed;

            let last_processed_at = if last_ts > 0 {
                chrono::DateTime::from_timestamp_millis(last_ts).map(|dt| dt.to_rfc3339())
            } else {
                None
            };

            JobMetrics {
                events_processed: events,
                bytes_processed: bytes,
                current_lag_seconds: 0.0,
                throughput_per_second: throughput,
                total_errors: errors,
                last_processed_at,
            }
        }
        None => JobMetrics {
            events_processed: 0,
            bytes_processed: 0,
            current_lag_seconds: 0.0,
            throughput_per_second: 0.0,
            total_errors: 0,
            last_processed_at: None,
        },
    };

    Ok((StatusCode::OK, Json(metrics)))
}
