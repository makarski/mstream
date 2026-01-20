use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::api::AppState;
use crate::logs::{LogEntry, LogFilter, LogLevel};

/// Query parameters for GET /logs
#[derive(Debug, Deserialize, Default)]
pub struct LogsQuery {
    /// Filter by job name
    pub job_name: Option<String>,
    /// Minimum log level (trace, debug, info, warn, error)
    pub level: Option<String>,
    /// Maximum number of entries to return
    pub limit: Option<usize>,
    /// Only entries after this timestamp (RFC3339)
    pub since: Option<String>,
}

/// Response for GET /logs
#[derive(Debug, Serialize)]
pub struct LogsResponse {
    pub logs: Vec<Arc<LogEntry>>,
}

/// Query parameters for GET /logs/stream
#[derive(Debug, Deserialize, Default)]
pub struct LogsStreamQuery {
    /// Filter by job name
    pub job_name: Option<String>,
    /// Minimum log level (trace, debug, info, warn, error)
    pub level: Option<String>,
    /// Number of historical entries to send initially
    pub initial: Option<usize>,
}

impl LogsQuery {
    fn to_filter(&self) -> LogFilter {
        let mut filter = LogFilter::new();

        if let Some(ref job_name) = self.job_name {
            filter = filter.with_job_name(job_name);
        }

        if let Some(ref level_str) = self.level {
            if let Some(level) = LogLevel::from_str(level_str) {
                filter = filter.with_min_level(level);
            }
        }

        if let Some(ref since_str) = self.since {
            if let Ok(since) = since_str.parse::<DateTime<Utc>>() {
                filter = filter.with_since(since);
            }
        }

        filter
    }
}

impl LogsStreamQuery {
    fn to_filter(&self) -> LogFilter {
        let mut filter = LogFilter::new();

        if let Some(ref job_name) = self.job_name {
            filter = filter.with_job_name(job_name);
        }

        if let Some(ref level_str) = self.level {
            if let Some(level) = LogLevel::from_str(level_str) {
                filter = filter.with_min_level(level);
            }
        }

        filter
    }
}

/// GET /logs - retrieve log entries with optional filters
pub async fn get_logs(
    State(state): State<AppState>,
    Query(query): Query<LogsQuery>,
) -> impl IntoResponse {
    let filter = query.to_filter();
    let limit = query.limit.unwrap_or(100);

    let logs = state.log_buffer.get_recent(limit, &filter);

    (StatusCode::OK, Json(LogsResponse { logs }))
}

/// GET /logs/stream - SSE stream of log entries
pub async fn stream_logs(
    State(state): State<AppState>,
    Query(query): Query<LogsStreamQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let filter = query.to_filter();
    let initial_count = query
        .initial
        .unwrap_or(state.logs_config.stream_preload_count);

    // Get initial historical entries
    let initial_logs = state.log_buffer.get_recent(initial_count, &filter);

    // Subscribe to live updates
    let receiver = state.log_buffer.subscribe();

    // Clone filter for the stream closure
    let stream_filter = query.to_filter();

    // Create stream that first yields initial logs, then live updates
    let initial_stream = tokio_stream::iter(initial_logs.into_iter().map(|entry| {
        let json = serde_json::to_string(&entry).unwrap_or_default();
        Ok(Event::default().data(json))
    }));

    let live_stream = BroadcastStream::new(receiver).filter_map(move |result| {
        match result {
            Ok(entry) if stream_filter.matches(&entry) => {
                let json = serde_json::to_string(&entry).unwrap_or_default();
                Some(Ok(Event::default().data(json)))
            }
            Ok(_) => None,  // Filtered out
            Err(_) => None, // Lagged, skip
        }
    });

    let combined_stream = initial_stream.chain(live_stream);

    Sse::new(combined_stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(30)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logs_query_to_filter_empty() {
        let query = LogsQuery::default();
        let filter = query.to_filter();

        assert!(filter.job_name.is_none());
        assert!(filter.min_level.is_none());
        assert!(filter.since.is_none());
    }

    #[test]
    fn test_logs_query_to_filter_with_job_name() {
        let query = LogsQuery {
            job_name: Some("my-job".to_string()),
            ..Default::default()
        };
        let filter = query.to_filter();

        assert_eq!(filter.job_name, Some("my-job".to_string()));
    }

    #[test]
    fn test_logs_query_to_filter_with_level() {
        let query = LogsQuery {
            level: Some("warn".to_string()),
            ..Default::default()
        };
        let filter = query.to_filter();

        assert_eq!(filter.min_level, Some(LogLevel::Warn));
    }

    #[test]
    fn test_logs_query_to_filter_with_invalid_level() {
        let query = LogsQuery {
            level: Some("invalid".to_string()),
            ..Default::default()
        };
        let filter = query.to_filter();

        assert!(filter.min_level.is_none());
    }

    #[test]
    fn test_logs_query_to_filter_with_since() {
        let query = LogsQuery {
            since: Some("2024-01-15T10:30:00Z".to_string()),
            ..Default::default()
        };
        let filter = query.to_filter();

        assert!(filter.since.is_some());
    }

    #[test]
    fn test_logs_query_to_filter_with_invalid_since() {
        let query = LogsQuery {
            since: Some("not-a-date".to_string()),
            ..Default::default()
        };
        let filter = query.to_filter();

        assert!(filter.since.is_none());
    }

    #[test]
    fn test_stream_query_to_filter() {
        let query = LogsStreamQuery {
            job_name: Some("streaming-job".to_string()),
            level: Some("error".to_string()),
            initial: Some(50),
        };
        let filter = query.to_filter();

        assert_eq!(filter.job_name, Some("streaming-job".to_string()));
        assert_eq!(filter.min_level, Some(LogLevel::Error));
    }
}
