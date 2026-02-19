use std::collections::HashMap;

use axum::{
    Json,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::Checkpoint,
    config::{Encoding, Masked},
    encoding::json_schema::JsonSchema,
    kafka::KafkaOffset,
};

// =============================================================================
// Test Generation Types
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct TestGenerateRequest {
    pub script: String,
    pub sample_input: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct TestGenerateResponse {
    pub input_schema: JsonSchema,
    pub cases: Vec<GeneratedTestCase>,
}

#[derive(Debug, Serialize)]
pub struct GeneratedTestCase {
    pub name: String,
    pub input: serde_json::Value,
    pub expected: serde_json::Value,
    pub assertions: Vec<TestAssertion>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestAssertion {
    pub path: String,
    pub assert_type: AssertType,
    pub expected_value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AssertType {
    Equals,
    Contains,
    TypeMatches,
    Gt,  // Greater than (numbers only)
    Gte, // Greater than or equal (numbers only)
    Lt,  // Less than (numbers only)
    Lte, // Less than or equal (numbers only)
}

#[derive(Debug, Deserialize)]
pub struct TestRunRequest {
    pub script: String,
    pub cases: Vec<TestCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCase {
    pub name: String,
    pub input: serde_json::Value,
    #[serde(default)]
    pub expected: Option<serde_json::Value>,
    #[serde(default)]
    pub assertions: Vec<TestAssertion>,
    #[serde(default)]
    pub strict_mode: bool,
}

#[derive(Debug, Serialize)]
pub struct TestRunResponse {
    pub results: Vec<TestCaseResult>,
    pub summary: TestRunSummary,
}

#[derive(Debug, Serialize)]
pub struct TestCaseResult {
    pub name: String,
    pub passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<AssertionFailure>,
    pub duration_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct AssertionFailure {
    pub path: String,
    pub assert_type: AssertType,
    pub expected: serde_json::Value,
    pub actual: serde_json::Value,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct TestRunSummary {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
}

#[derive(Serialize)]
pub struct TransformTestResponse {
    pub document: serde_json::Value,
    pub encoding: Encoding,
    pub attributes: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
pub struct TransformTestRequest {
    pub payload: String,
    pub script: String,
    pub schema: Option<TransformTestSchema>,
    pub attributes: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
pub struct TransformTestSchema {
    pub schema_encoding: Encoding,
    pub body: String,
}

#[derive(Deserialize)]
pub struct SchemaConvertRequest {
    pub source: TransformTestSchema,
    pub target_encoding: Encoding,
    pub options: Option<HashMap<String, String>>,
}

#[derive(Serialize)]
pub struct SchemaConvertResponse {
    pub schema: String,
    pub encoding: Encoding,
}

#[derive(Serialize, Clone)]
pub struct Message<T> {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<T>,
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

pub struct MaskedJson<T>(pub T);

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
pub struct CheckpointResponse {
    pub job_name: String,
    pub updated_at: String,
    pub cursor: CursorInfo,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum CursorInfo {
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

pub(super) fn decode_cursor(cursor: &[u8]) -> CursorInfo {
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

/// Maximum allowed sample size to prevent memory exhaustion
const MAX_SAMPLE_SIZE: usize = 1000;

#[derive(Deserialize)]
pub(crate) struct SchemaQuery {
    pub(crate) resource: String,
    #[serde(default = "default_sample_size")]
    sample_size: usize,
}

impl SchemaQuery {
    /// Returns sample_size clamped to MAX_SAMPLE_SIZE
    pub(crate) fn sample_size(&self) -> usize {
        self.sample_size.min(MAX_SAMPLE_SIZE)
    }
}

fn default_sample_size() -> usize {
    100
}

#[derive(Deserialize)]
pub(crate) struct SchemaFillRequest {
    pub(crate) schema: JsonSchema,
}

#[derive(Serialize)]
pub(crate) struct ResourceInfo {
    pub name: String,
    pub resource_type: String,
}

#[derive(Serialize)]
pub(crate) struct ServiceResourcesResponse {
    pub service_name: String,
    pub resources: Vec<ResourceInfo>,
}

// =============================================================================
// System / Health / Metrics Types
// =============================================================================

#[derive(Serialize)]
pub struct HealthStatus {
    pub status: &'static str,
    pub version: &'static str,
    pub uptime_seconds: u64,
}

#[derive(Serialize)]
pub struct SystemStats {
    pub total_docs_processed: u64,
    pub total_bytes_transferred: u64,
    pub uptime_seconds: u64,
    pub running_jobs: usize,
    pub stopped_jobs: usize,
    pub error_jobs: usize,
}

#[derive(Serialize)]
pub struct JobMetrics {
    pub events_processed: u64,
    pub bytes_processed: u64,
    pub current_lag_seconds: f64,
    pub throughput_per_second: f64,
    pub total_errors: u64,
    pub last_processed_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;
    use crate::checkpoint::Checkpoint;
    use crate::config::Masked;
    use crate::kafka::KafkaOffset;

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
        use super::*;

        #[test]
        fn serialize_response_with_document() {
            let response = TransformTestResponse {
                document: serde_json::json!({"masked": true, "email": "j***@example.com"}),
                encoding: Encoding::Json,
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
                encoding: Encoding::Json,
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
                encoding: Encoding::Json,
                attributes: None,
            };

            let json = serde_json::to_string(&response).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

            assert!(parsed["document"].is_array());
            assert_eq!(parsed["document"].as_array().unwrap().len(), 2);
        }
    }

    mod system_types_tests {
        use super::super::{HealthStatus, JobMetrics, SystemStats};

        #[test]
        fn health_status_serializes_correctly() {
            let health = HealthStatus {
                status: "healthy",
                version: env!("CARGO_PKG_VERSION"),
                uptime_seconds: 120,
            };
            let json: serde_json::Value = serde_json::to_value(&health).unwrap();

            assert_eq!(json["status"], "healthy");
            assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
            assert_eq!(json["uptime_seconds"], 120);
        }

        #[test]
        fn health_status_degraded() {
            let health = HealthStatus {
                status: "degraded",
                version: env!("CARGO_PKG_VERSION"),
                uptime_seconds: 0,
            };
            let json: serde_json::Value = serde_json::to_value(&health).unwrap();

            assert_eq!(json["status"], "degraded");
            assert_eq!(json["uptime_seconds"], 0);
        }

        #[test]
        fn system_stats_serializes_correctly() {
            let stats = SystemStats {
                total_docs_processed: 1000,
                total_bytes_transferred: 50000,
                uptime_seconds: 3600,
                running_jobs: 3,
                stopped_jobs: 1,
                error_jobs: 2,
            };
            let json: serde_json::Value = serde_json::to_value(&stats).unwrap();

            assert_eq!(json["total_docs_processed"], 1000);
            assert_eq!(json["total_bytes_transferred"], 50000);
            assert_eq!(json["uptime_seconds"], 3600);
            assert_eq!(json["running_jobs"], 3);
            assert_eq!(json["stopped_jobs"], 1);
            assert_eq!(json["error_jobs"], 2);
        }

        #[test]
        fn system_stats_zeroed() {
            let stats = SystemStats {
                total_docs_processed: 0,
                total_bytes_transferred: 0,
                uptime_seconds: 0,
                running_jobs: 0,
                stopped_jobs: 0,
                error_jobs: 0,
            };
            let json: serde_json::Value = serde_json::to_value(&stats).unwrap();

            assert_eq!(json["running_jobs"], 0);
            assert_eq!(json["error_jobs"], 0);
        }

        #[test]
        fn job_metrics_serializes_with_nulls() {
            let metrics = JobMetrics {
                events_processed: 0,
                bytes_processed: 0,
                current_lag_seconds: 0.0,
                throughput_per_second: 0.0,
                total_errors: 0,
                last_processed_at: None,
            };
            let json: serde_json::Value = serde_json::to_value(&metrics).unwrap();

            assert_eq!(json["events_processed"], 0);
            assert_eq!(json["bytes_processed"], 0);
            assert_eq!(json["current_lag_seconds"], 0.0);
            assert_eq!(json["throughput_per_second"], 0.0);
            assert_eq!(json["total_errors"], 0);
            assert!(json["last_processed_at"].is_null());
        }

        #[test]
        fn job_metrics_serializes_with_values() {
            let metrics = JobMetrics {
                events_processed: 5000,
                bytes_processed: 1_200_000,
                current_lag_seconds: 2.5,
                throughput_per_second: 150.7,
                total_errors: 3,
                last_processed_at: Some("2025-01-15T10:30:00Z".to_string()),
            };
            let json: serde_json::Value = serde_json::to_value(&metrics).unwrap();

            assert_eq!(json["events_processed"], 5000);
            assert_eq!(json["bytes_processed"], 1_200_000);
            assert_eq!(json["current_lag_seconds"], 2.5);
            assert_eq!(json["throughput_per_second"], 150.7);
            assert_eq!(json["total_errors"], 3);
            assert_eq!(json["last_processed_at"], "2025-01-15T10:30:00Z");
        }
    }
}
