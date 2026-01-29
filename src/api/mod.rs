use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};

use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::info;

use crate::api::error::ApiError;
use crate::api::types::{
    CheckpointResponse, MaskedJson, Message, SchemaFillRequest, SchemaQuery, TransformTestRequest,
    TransformTestResponse,
};
use crate::config::system::LogsConfig;
use crate::config::{Connector, Encoding, Service};
use crate::encoding::json_schema::SchemaFiller;
use crate::job_manager::{JobManager, JobMetadata, error::JobManagerError};
use crate::logs::LogBuffer;
use crate::middleware::udf::rhai::{RhaiMiddleware, RhaiMiddlewareError};
use crate::schema::introspect::SchemaIntrospector;
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
        .route("/services/{name}/schema", get(get_resource_schema))
        .route("/schema/fill", post(fill_schema))
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
async fn list_jobs(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
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
async fn restart_job(
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
async fn create_start_job(
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

/// GET /services
async fn list_services(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
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
) -> Result<impl IntoResponse, ApiError> {
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
) -> Result<impl IntoResponse, ApiError> {
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

/// GET /services/{name}/schema
async fn get_resource_schema(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
    Query(query): Query<SchemaQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Get client and db_name while holding locks, then release before slow query
    let (client, db_name) = {
        let jm = state.job_manager.lock().await;
        let registry = jm.service_registry.read().await;

        let client = registry
            .mongodb_client(&service_name)
            .await
            .map_err(|e| ApiError::Internal(format!("failed to get MongoDB client: {}", e)))?;

        let service_definition = registry
            .service_definition(&service_name)
            .await
            .map_err(|err| ApiError::NotFound(err.to_string()))?;

        let db_name = match &service_definition {
            Service::MongoDb(cfg) => cfg.db_name.clone(),
            _ => {
                return Err(ApiError::Internal(format!(
                    "service {} is not a mongo service",
                    service_name
                )));
            }
        };

        (client, db_name)
    }; // locks released here

    let db = client.database(&db_name);
    let introspector = SchemaIntrospector::new(db, query.resource.clone());
    let variants = introspector.introspect(query.sample_size()).await?;

    Ok((StatusCode::OK, Json(variants)))
}

/// POST /schema/fill
async fn fill_schema(Json(req): Json<SchemaFillRequest>) -> Result<impl IntoResponse, ApiError> {
    let mut filler = SchemaFiller::new();
    let filled = filler.fill(&req.schema);
    Ok((StatusCode::OK, Json(filled)))
}

async fn transform_run(
    Json(transform_req): Json<TransformTestRequest>,
) -> Result<impl IntoResponse, ApiError> {
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
) -> Result<impl IntoResponse, ApiError> {
    let jm = state.job_manager.lock().await;
    let service = jm.get_service(&name).await?;
    Ok((StatusCode::OK, MaskedJson(service)))
}

/// GET /jobs/{name}/checkpoints
async fn list_checkpoints(
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

    mod schema_fill_tests {
        use crate::encoding::json_schema::SchemaFiller;
        use serde_json::json;

        #[test]
        fn fill_empty_schema() {
            let schema = json!({
                "type": "object",
                "properties": {}
            });

            let mut filler = SchemaFiller::with_seed(42);
            let result = filler.fill(&schema);

            assert!(result.is_object());
            assert!(result.as_object().unwrap().is_empty());
        }

        #[test]
        fn fill_simple_schema() {
            let schema = json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                    "active": {"type": "boolean"}
                }
            });

            let mut filler = SchemaFiller::with_seed(42);
            let result = filler.fill(&schema);

            assert!(result.is_object());
            assert!(result.get("name").unwrap().is_string());
            assert!(result.get("age").unwrap().is_number());
            assert!(result.get("active").unwrap().is_boolean());
        }

        #[test]
        fn fill_schema_with_format_hints() {
            let schema = json!({
                "type": "object",
                "properties": {
                    "email": {"type": "string", "format": "email"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "_id": {"type": "string", "format": "objectid"}
                }
            });

            let mut filler = SchemaFiller::with_seed(42);
            let result = filler.fill(&schema);

            let email = result.get("email").unwrap().as_str().unwrap();
            assert!(email.contains("@"), "Expected email format, got: {}", email);

            let created_at = result.get("created_at").unwrap().as_str().unwrap();
            assert!(
                created_at.contains("T") && created_at.ends_with("Z"),
                "Expected ISO datetime, got: {}",
                created_at
            );

            let id = result.get("_id").unwrap().as_str().unwrap();
            assert_eq!(id.len(), 24, "Expected 24-char ObjectId, got: {}", id);
        }

        #[test]
        fn fill_schema_with_enum() {
            let schema = json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["active", "pending", "inactive"]
                    }
                }
            });

            let mut filler = SchemaFiller::with_seed(42);
            let result = filler.fill(&schema);

            let status = result.get("status").unwrap().as_str().unwrap();
            assert!(
                ["active", "pending", "inactive"].contains(&status),
                "Expected enum value, got: {}",
                status
            );
        }

        #[test]
        fn fill_schema_with_min_max() {
            let schema = json!({
                "type": "object",
                "properties": {
                    "score": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 100
                    },
                    "gpa": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 4.0
                    }
                }
            });

            let mut filler = SchemaFiller::with_seed(42);
            let result = filler.fill(&schema);

            let score = result.get("score").unwrap().as_i64().unwrap();
            assert!((0..=100).contains(&score), "Expected 0-100, got: {}", score);

            let gpa = result.get("gpa").unwrap().as_f64().unwrap();
            assert!((0.0..=4.0).contains(&gpa), "Expected 0.0-4.0, got: {}", gpa);
        }

        #[test]
        fn fill_deeply_nested_schema() {
            let schema = json!({
                "type": "object",
                "properties": {
                    "level1": {
                        "type": "object",
                        "properties": {
                            "level2": {
                                "type": "object",
                                "properties": {
                                    "level3": {
                                        "type": "object",
                                        "properties": {
                                            "value": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });

            let mut filler = SchemaFiller::with_seed(42);
            let result = filler.fill(&schema);

            let value = result
                .get("level1")
                .and_then(|l1| l1.get("level2"))
                .and_then(|l2| l2.get("level3"))
                .and_then(|l3| l3.get("value"));

            assert!(value.is_some(), "Deeply nested value should exist");
            assert!(value.unwrap().is_string());
        }

        #[test]
        fn fill_schema_with_arrays() {
            let schema = json!({
                "type": "object",
                "properties": {
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "scores": {
                        "type": "array",
                        "items": {
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 100
                        }
                    }
                }
            });

            let mut filler = SchemaFiller::with_seed(42);
            let result = filler.fill(&schema);

            let tags = result.get("tags").unwrap().as_array().unwrap();
            assert!(!tags.is_empty(), "Tags array should not be empty");
            assert!(tags.iter().all(|t| t.is_string()));

            let scores = result.get("scores").unwrap().as_array().unwrap();
            assert!(!scores.is_empty(), "Scores array should not be empty");
            for score in scores {
                let n = score.as_i64().unwrap();
                assert!((0..=100).contains(&n), "Score out of range: {}", n);
            }
        }

        #[test]
        fn fill_schema_with_nullable_types() {
            let schema = json!({
                "type": "object",
                "properties": {
                    "nullable_string": {"type": ["string", "null"]},
                    "nullable_object": {
                        "oneOf": [
                            {"type": "object", "properties": {"name": {"type": "string"}}},
                            {"type": "null"}
                        ]
                    }
                }
            });

            let mut filler = SchemaFiller::with_seed(42);
            let result = filler.fill(&schema);

            // Nullable string should resolve to string (non-null)
            assert!(result.get("nullable_string").unwrap().is_string());

            // Nullable object should resolve to object (non-null)
            let obj = result.get("nullable_object").unwrap();
            assert!(obj.is_object());
            assert!(obj.get("name").unwrap().is_string());
        }

        #[test]
        fn fill_schema_deterministic_with_seed() {
            let schema = json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "value": {"type": "integer"}
                }
            });

            let mut filler1 = SchemaFiller::with_seed(123);
            let mut filler2 = SchemaFiller::with_seed(123);

            let result1 = filler1.fill(&schema);
            let result2 = filler2.fill(&schema);

            assert_eq!(result1, result2, "Same seed should produce same output");
        }

        #[test]
        fn fill_realistic_user_schema() {
            // Schema similar to what would be generated by SchemaIntrospector
            let schema = json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "_id": {"type": "string", "format": "objectid"},
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                    "email": {"type": "string", "format": "email"},
                    "phone": {"type": "string"},
                    "status": {"type": "string", "enum": ["active", "pending", "inactive"]},
                    "created_at": {"type": "string", "format": "date-time"},
                    "age": {"type": "integer", "minimum": 18, "maximum": 100},
                    "address": {
                        "type": "object",
                        "properties": {
                            "street": {"type": "string"},
                            "city": {"type": "string"},
                            "state": {"type": "string"},
                            "country": {"type": "string"}
                        }
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["_id", "email", "status"]
            });

            let mut filler = SchemaFiller::with_seed(42);
            let result = filler.fill(&schema);

            // Verify structure
            assert!(result.is_object());

            // Verify formats
            let id = result.get("_id").unwrap().as_str().unwrap();
            assert_eq!(id.len(), 24);

            let email = result.get("email").unwrap().as_str().unwrap();
            assert!(email.contains("@"));

            let status = result.get("status").unwrap().as_str().unwrap();
            assert!(["active", "pending", "inactive"].contains(&status));

            let created_at = result.get("created_at").unwrap().as_str().unwrap();
            assert!(created_at.contains("T"));

            let age = result.get("age").unwrap().as_i64().unwrap();
            assert!((18..=100).contains(&age));

            // Verify nested object
            let address = result.get("address").unwrap();
            assert!(address.get("city").unwrap().is_string());
            assert!(address.get("country").unwrap().is_string());

            // Verify array
            let tags = result.get("tags").unwrap().as_array().unwrap();
            assert!(!tags.is_empty());
        }
    }
}
