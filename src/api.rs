use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::checkpoint::Checkpoint;
use crate::config::{Connector, Masked, Service};
use crate::job_manager::{JobManager, JobMetadata, error::JobManagerError};
use crate::kafka::KafkaOffset;

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

impl IntoResponse for JobManagerError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            JobManagerError::JobNotFound(name) => {
                (StatusCode::NOT_FOUND, format!("Job '{}' not found", name))
            }
            JobManagerError::JobAlreadyExists(name) => (
                StatusCode::CONFLICT,
                format!("Job '{}' already exists", name),
            ),
            JobManagerError::ServiceNotFound(name) => (
                StatusCode::NOT_FOUND,
                format!("Service '{}' not found", name),
            ),
            JobManagerError::ServiceAlreadyExists(name) => (
                StatusCode::CONFLICT,
                format!("Service '{}' already exists", name),
            ),
            JobManagerError::ServiceInUse(name, used_by) => (
                StatusCode::CONFLICT,
                format!("Service '{}' is in use by jobs: {}", name, used_by),
            ),
            JobManagerError::InternalError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal error: {}", msg),
            ),
            JobManagerError::Anyhow(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal error: {}", e),
            ),
        };

        if status.is_server_error() {
            error!("API Error: {}", message);
        } else {
            warn!("API Client Error: {}", message);
        }

        let body = Json(Message {
            message,
            item: None::<()>,
        });

        (status, body).into_response()
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
        .route("/jobs/{name}/checkpoints", get(list_checkpoints))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("web server listening on: {}", addr);

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// GET /jobs
async fn list_jobs(State(state): State<AppState>) -> Result<impl IntoResponse, JobManagerError> {
    let jm = state.job_manager.lock().await;
    let jobs = jm
        .list_jobs()
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to list jobs: {}", e)))?;

    Ok((StatusCode::OK, Json(jobs)))
}

/// POST /jobs/{name}/stop
async fn stop_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Message<()>>), JobManagerError> {
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
) -> Result<(StatusCode, Json<Message<JobMetadata>>), JobManagerError> {
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
) -> Result<(StatusCode, Json<Message<JobMetadata>>), JobManagerError> {
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
async fn list_services(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, JobManagerError> {
    let jm = state.job_manager.lock().await;
    let services = jm
        .list_services()
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to list services: {}", e)))?;

    Ok((StatusCode::OK, MaskedJson(services)))
}

/// POST /services
async fn create_service(
    State(state): State<AppState>,
    Json(service_cfg): Json<Service>,
) -> Result<impl IntoResponse, JobManagerError> {
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
) -> Result<impl IntoResponse, JobManagerError> {
    let jm = state.job_manager.lock().await;
    jm.remove_service(&name).await?;
    Ok((
        StatusCode::OK,
        Json(Message::<()> {
            message: format!("service {} removed successfully", name),
            item: None,
        }),
    ))
}

async fn get_one_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, JobManagerError> {
    let jm = state.job_manager.lock().await;
    let service = jm.get_service(&name).await?;
    Ok((StatusCode::OK, MaskedJson(service)))
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

/// Human-readable checkpoint response for API
#[derive(Clone, Debug, Serialize)]
struct CheckpointResponse {
    job_name: String,
    updated_at: String,
    cursor: CursorInfo,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
enum CursorInfo {
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    #[serde(rename = "mongodb")]
    MongoDB {
        data: serde_json::Value,
    },
    Unknown {
        raw_bytes: usize,
    },
}

impl From<Checkpoint> for CheckpointResponse {
    fn from(cp: Checkpoint) -> Self {
        let cursor = decode_cursor(&cp.cursor);
        let updated_at = chrono::DateTime::from_timestamp_millis(cp.updated_at)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        CheckpointResponse {
            job_name: cp.job_name,
            updated_at,
            cursor,
        }
    }
}

fn decode_cursor(cursor: &[u8]) -> CursorInfo {
    // Try Kafka offset first
    if let Ok(kafka) = mongodb::bson::from_slice::<KafkaOffset>(cursor) {
        return CursorInfo::Kafka {
            topic: kafka.topic,
            partition: kafka.partition,
            offset: kafka.offset,
        };
    }

    // Try MongoDB cursor (any BSON document - resume token, _id, or custom field)
    if let Ok(bson_doc) = mongodb::bson::from_slice::<mongodb::bson::Document>(cursor) {
        if let Ok(json_value) = serde_json::to_value(&bson_doc) {
            return CursorInfo::MongoDB { data: json_value };
        }
    }

    // Fallback to unknown
    CursorInfo::Unknown {
        raw_bytes: cursor.len(),
    }
}

/// GET /jobs/{name}/checkpoints
async fn list_checkpoints(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, JobManagerError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::Checkpoint;

    mod checkpoint_response_tests {
        use super::*;

        #[test]
        fn decode_cursor_kafka_offset() {
            let kafka_offset = KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 2,
                offset: 12345,
            };
            let cursor = mongodb::bson::to_vec(&kafka_offset).unwrap();

            let result = decode_cursor(&cursor);

            match result {
                CursorInfo::Kafka {
                    topic,
                    partition,
                    offset,
                } => {
                    assert_eq!(topic, "test-topic");
                    assert_eq!(partition, 2);
                    assert_eq!(offset, 12345);
                }
                _ => panic!("expected Kafka cursor info"),
            }
        }

        #[test]
        fn decode_cursor_mongodb_resume_token() {
            let doc = mongodb::bson::doc! { "_data": "82696425F900000001" };
            let cursor = mongodb::bson::to_vec(&doc).unwrap();

            let result = decode_cursor(&cursor);

            match result {
                CursorInfo::MongoDB { data } => {
                    assert!(data.is_object());
                    assert_eq!(data["_data"], "82696425F900000001");
                }
                _ => panic!("expected MongoDB cursor info"),
            }
        }

        #[test]
        fn decode_cursor_unknown_format() {
            let cursor = vec![0x00, 0x01, 0x02, 0x03];

            let result = decode_cursor(&cursor);

            match result {
                CursorInfo::Unknown { raw_bytes } => {
                    assert_eq!(raw_bytes, 4);
                }
                _ => panic!("expected Unknown cursor info"),
            }
        }

        #[test]
        fn checkpoint_response_from_kafka_checkpoint() {
            let kafka_offset = KafkaOffset {
                topic: "events".to_string(),
                partition: 0,
                offset: 999,
            };
            let cursor = mongodb::bson::to_vec(&kafka_offset).unwrap();

            let checkpoint = Checkpoint {
                job_name: "kafka-job".to_string(),
                cursor,
                updated_at: 1704067200000, // 2024-01-01 00:00:00 UTC
            };

            let response = CheckpointResponse::from(checkpoint);

            assert_eq!(response.job_name, "kafka-job");
            assert_eq!(response.updated_at, "2024-01-01 00:00:00 UTC");
            match response.cursor {
                CursorInfo::Kafka {
                    topic,
                    partition,
                    offset,
                } => {
                    assert_eq!(topic, "events");
                    assert_eq!(partition, 0);
                    assert_eq!(offset, 999);
                }
                _ => panic!("expected Kafka cursor"),
            }
        }

        #[test]
        fn checkpoint_response_formats_timestamp() {
            let checkpoint = Checkpoint {
                job_name: "test".to_string(),
                cursor: vec![0x00],
                updated_at: 1704153600000, // 2024-01-02 00:00:00 UTC
            };

            let response = CheckpointResponse::from(checkpoint);

            assert_eq!(response.updated_at, "2024-01-02 00:00:00 UTC");
        }
    }

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
