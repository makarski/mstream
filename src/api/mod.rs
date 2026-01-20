use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::info;

use crate::api::types::{
    CheckpointResponse, MaskedJson, Message, TransformTestRequest, TransformTestResponse,
};
use crate::config::system::LogsConfig;
use crate::config::{Connector, Encoding, Service};
use crate::job_manager::{JobManager, JobMetadata, error::JobManagerError};
use crate::logs::LogBuffer;
use crate::middleware::udf::rhai::{RhaiMiddleware, RhaiMiddlewareError};
use crate::source::SourceEvent;

pub(crate) mod error;
pub(crate) mod logs;
pub(crate) mod types;

#[derive(Clone)]
pub struct AppState {
    pub(crate) job_manager: Arc<Mutex<JobManager>>,
    pub(crate) log_buffer: LogBuffer,
    pub(crate) logs_config: LogsConfig,
}

impl AppState {
    pub fn new(jb: Arc<Mutex<JobManager>>, log_buffer: LogBuffer, logs_config: LogsConfig) -> Self {
        Self {
            job_manager: jb,
            log_buffer,
            logs_config,
        }
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
        .route("/transform/run", post(transform_run))
        .route("/logs", get(logs::get_logs))
        .route("/logs/stream", get(logs::stream_logs))
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
async fn restart_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Message<JobMetadata>>), JobManagerError> {
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
async fn create_start_job(
    State(state): State<AppState>,
    Json(conn_cfg): Json<Connector>,
) -> Result<(StatusCode, Json<Message<JobMetadata>>), JobManagerError> {
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

async fn transform_run(
    Json(transform_req): Json<TransformTestRequest>,
) -> Result<impl IntoResponse, RhaiMiddlewareError> {
    let mut middleware = RhaiMiddleware::with_script(transform_req.script)?;

    let source_event = SourceEvent {
        cursor: None,
        attributes: transform_req.attributes,
        encoding: Encoding::Json,
        is_framed_batch: false,
        raw_bytes: transform_req.payload.as_bytes().to_vec(),
    };

    let transformed = middleware.transform(source_event).await?;
    let json_value: serde_json::Value =
        serde_json::from_slice(&transformed.raw_bytes).map_err(|e| {
            RhaiMiddlewareError::ExecutionError {
                message: format!("Output is not valid JSON: {}", e),
                path: "<inline>".to_string(),
            }
        })?;

    let response = TransformTestResponse {
        document: json_value,
        attributes: transformed.attributes,
    };

    Ok((
        StatusCode::OK,
        Json(Message {
            message: "success".to_string(),
            item: Some(response),
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
    use serde::Serialize;

    use super::*;
    use crate::{checkpoint::Checkpoint, config::Masked};

    mod checkpoint_response_tests {
        use crate::{
            api::types::{CursorInfo, decode_cursor},
            kafka::KafkaOffset,
        };

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

    mod transform_request_tests {
        use super::*;

        #[test]
        fn deserialize_minimal_request() {
            let json = r#"{
                "script": "fn transform(data, attributes) { result(data, attributes) }",
                "payload": "{\"name\": \"test\"}"
            }"#;

            let req: TransformTestRequest = serde_json::from_str(json).unwrap();

            assert_eq!(
                req.script,
                "fn transform(data, attributes) { result(data, attributes) }"
            );
            assert_eq!(req.payload, "{\"name\": \"test\"}");
            assert!(req.attributes.is_none());
        }

        #[test]
        fn deserialize_full_request() {
            let json = r#"{
                "script": "fn transform(data, attributes) { result(data, attributes) }",
                "payload": "[{\"a\": 1}, {\"a\": 2}]",
                "attributes": {"source": "test", "env": "dev"}
            }"#;

            let req: TransformTestRequest = serde_json::from_str(json).unwrap();

            assert_eq!(req.payload, "[{\"a\": 1}, {\"a\": 2}]");
            let attrs = req.attributes.unwrap();
            assert_eq!(attrs.get("source"), Some(&"test".to_string()));
            assert_eq!(attrs.get("env"), Some(&"dev".to_string()));
        }
    }

    mod transform_response_tests {
        use std::collections::HashMap;

        use super::*;

        #[test]
        fn serialize_response_with_document() {
            let response = TransformTestResponse {
                document: serde_json::json!({"masked": true, "email": "j***@example.com"}),
                attributes: None,
            };

            let json = serde_json::to_string(&response).unwrap();

            assert!(json.contains("\"masked\":true"));
            assert!(json.contains("\"email\":\"j***@example.com\""));
        }

        #[test]
        fn serialize_response_with_attributes() {
            let mut attrs = HashMap::new();
            attrs.insert("processed".to_string(), "true".to_string());
            attrs.insert("source".to_string(), "playground".to_string());

            let response = TransformTestResponse {
                document: serde_json::json!({"data": "test"}),
                attributes: Some(attrs),
            };

            let json = serde_json::to_string(&response).unwrap();

            assert!(json.contains("\"processed\":\"true\""));
            assert!(json.contains("\"source\":\"playground\""));
        }

        #[test]
        fn serialize_batch_response() {
            let response = TransformTestResponse {
                document: serde_json::json!([
                    {"id": 1, "masked": true},
                    {"id": 2, "masked": true}
                ]),
                attributes: None,
            };

            let json = serde_json::to_string(&response).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

            assert!(parsed["document"].is_array());
            assert_eq!(parsed["document"].as_array().unwrap().len(), 2);
        }
    }

    mod transform_run_integration_tests {
        use std::collections::HashMap;

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        async fn transform_single_document() {
            let script = r#"
                fn transform(data, attributes) {
                    data.processed = true;
                    result(data, attributes)
                }
            "#;
            let payload = r#"{"name": "test"}"#;

            let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();
            let source_event = SourceEvent {
                cursor: None,
                attributes: None,
                encoding: Encoding::Json,
                is_framed_batch: false,
                raw_bytes: payload.as_bytes().to_vec(),
            };

            let transformed = middleware.transform(source_event).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&transformed.raw_bytes).unwrap();

            assert_eq!(json["name"], "test");
            assert_eq!(json["processed"], true);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn transform_with_attributes() {
            let script = r#"
                fn transform(data, attributes) {
                    attributes["modified"] = "yes";
                    result(data, attributes)
                }
            "#;
            let payload = r#"{"value": 42}"#;

            let mut attrs = HashMap::new();
            attrs.insert("source".to_string(), "test".to_string());

            let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();
            let source_event = SourceEvent {
                cursor: None,
                attributes: Some(attrs),
                encoding: Encoding::Json,
                is_framed_batch: false,
                raw_bytes: payload.as_bytes().to_vec(),
            };

            let transformed = middleware.transform(source_event).await.unwrap();

            let result_attrs = transformed.attributes.unwrap();
            assert_eq!(result_attrs.get("source"), Some(&"test".to_string()));
            assert_eq!(result_attrs.get("modified"), Some(&"yes".to_string()));
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn transform_array_payload() {
            // Note: is_framed_batch=false means we treat the JSON array as a single document
            // The script receives the array and processes it directly
            let script = r#"
                fn transform(data, attributes) {
                    let results = [];
                    for doc in data {
                        doc.batch = true;
                        results.push(doc);
                    }
                    result(results, attributes)
                }
            "#;
            let payload = r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#;

            let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();
            let source_event = SourceEvent {
                cursor: None,
                attributes: None,
                encoding: Encoding::Json,
                is_framed_batch: false, // JSON array is parsed as a single value
                raw_bytes: payload.as_bytes().to_vec(),
            };

            let transformed = middleware.transform(source_event).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&transformed.raw_bytes).unwrap();

            assert!(json.is_array());
            let arr = json.as_array().unwrap();
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0]["batch"], true);
            assert_eq!(arr[1]["batch"], true);
            assert_eq!(arr[2]["batch"], true);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn transform_with_masking_functions() {
            let script = r#"
                fn transform(data, attributes) {
                    data.email = mask_email(data.email);
                    data.phone = mask_phone(data.phone);
                    result(data, attributes)
                }
            "#;
            let payload = r#"{"email": "john@example.com", "phone": "+1-555-123-4567"}"#;

            let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();
            let source_event = SourceEvent {
                cursor: None,
                attributes: None,
                encoding: Encoding::Json,
                is_framed_batch: false,
                raw_bytes: payload.as_bytes().to_vec(),
            };

            let transformed = middleware.transform(source_event).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&transformed.raw_bytes).unwrap();

            // mask_email: john@example.com -> j***@example.com
            assert_eq!(json["email"], "j***@example.com");
            // mask_phone: +1-555-123-4567 -> ***********4567
            assert_eq!(json["phone"], "***********4567");
        }

        #[test]
        fn invalid_script_returns_compile_error() {
            let script = r#"
                fn transform(data, attributes {
                    result(data, attributes)
                }
            "#;

            let result = RhaiMiddleware::with_script(script.to_string());

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(matches!(err, RhaiMiddlewareError::CompileError { .. }));
        }

        #[test]
        fn missing_transform_function_returns_error() {
            let script = r#"
                fn process(data, attributes) {
                    result(data, attributes)
                }
            "#;

            let result = RhaiMiddleware::with_script(script.to_string());

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(matches!(err, RhaiMiddlewareError::MissingTransformFunction));
        }
    }
}
