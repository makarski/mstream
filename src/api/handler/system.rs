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

    let (total_events, total_bytes, _) = jm.aggregate_metrics();

    Ok((
        StatusCode::OK,
        Json(SystemStats {
            total_events_processed: total_events,
            total_bytes_processed: total_bytes,
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
            let source_ts = m.last_source_timestamp.load(Ordering::Relaxed);

            let now = chrono::Utc::now();
            let now_ms = now.timestamp_millis();
            let elapsed = (now - job.started_at).num_seconds().max(1) as f64;
            let throughput = events as f64 / elapsed;

            let current_lag_seconds = compute_lag_seconds(source_ts, now_ms);

            let last_processed_at = if last_ts > 0 {
                chrono::DateTime::from_timestamp_millis(last_ts).map(|dt| dt.to_rfc3339())
            } else {
                None
            };

            JobMetrics {
                events_processed: events,
                bytes_processed: bytes,
                current_lag_seconds,
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

fn compute_lag_seconds(source_timestamp_ms: i64, now_ms: i64) -> f64 {
    if source_timestamp_ms > 0 {
        ((now_ms - source_timestamp_ms) as f64 / 1000.0).max(0.0)
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_positive_when_source_is_behind() {
        let now_ms = 1_700_000_010_000;
        let source_ms = 1_700_000_005_000;
        let lag = compute_lag_seconds(source_ms, now_ms);
        assert!((lag - 5.0).abs() < 0.001);
    }

    #[test]
    fn lag_zero_when_no_source_timestamp() {
        assert_eq!(compute_lag_seconds(0, 1_700_000_000_000), 0.0);
    }

    #[test]
    fn lag_clamped_to_zero_when_source_ahead() {
        let now_ms = 1_700_000_000_000;
        let source_ms = 1_700_000_001_000;
        assert_eq!(compute_lag_seconds(source_ms, now_ms), 0.0);
    }

    #[test]
    fn lag_sub_second_precision() {
        let now_ms = 1_700_000_000_500;
        let source_ms = 1_700_000_000_000;
        let lag = compute_lag_seconds(source_ms, now_ms);
        assert!((lag - 0.5).abs() < 0.001);
    }
}
