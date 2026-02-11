use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::api::types::TestAssertion;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestSuite {
    #[serde(default)]
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub context: TestContext,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<SourceRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<JsonValue>,
    pub cases: Vec<TestCase>,
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "Utc::now")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TestContext {
    Rhai { script: String },
    Http { endpoint: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRef {
    pub service_name: String,
    pub resource: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCase {
    pub name: String,
    pub input: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<JsonValue>,
    #[serde(default)]
    pub assertions: Vec<TestAssertion>,
    #[serde(default)]
    pub strict_mode: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TestSuiteSummary {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub case_count: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum TestSuiteStoreError {
    #[error("test suite not found: {0}")]
    NotFound(String),
    #[error("MongoDB error: {0}")]
    MongoDb(#[from] mongodb::error::Error),
    #[error("{0}")]
    Other(String),
}

pub type DynTestSuiteStore = Arc<dyn TestSuiteStore + Send + Sync>;

#[async_trait]
pub trait TestSuiteStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<TestSuite, TestSuiteStoreError>;
    async fn list(&self) -> Result<Vec<TestSuiteSummary>, TestSuiteStoreError>;
    async fn save(&self, suite: &TestSuite) -> Result<(), TestSuiteStoreError>;
    async fn delete(&self, id: &str) -> Result<(), TestSuiteStoreError>;
}

pub struct NoopTestSuiteStore;

#[async_trait]
impl TestSuiteStore for NoopTestSuiteStore {
    async fn get(&self, id: &str) -> Result<TestSuite, TestSuiteStoreError> {
        Err(TestSuiteStoreError::NotFound(id.to_string()))
    }

    async fn list(&self) -> Result<Vec<TestSuiteSummary>, TestSuiteStoreError> {
        Ok(vec![])
    }

    async fn save(&self, _suite: &TestSuite) -> Result<(), TestSuiteStoreError> {
        Ok(())
    }

    async fn delete(&self, _id: &str) -> Result<(), TestSuiteStoreError> {
        Ok(())
    }
}

impl TestSuite {
    pub fn new(id: String, name: String, context: TestContext) -> Self {
        let now = Utc::now();
        Self {
            id,
            name,
            description: None,
            context,
            source_ref: None,
            input_schema: None,
            cases: vec![],
            created_at: now,
            updated_at: now,
        }
    }
}

impl From<&TestSuite> for TestSuiteSummary {
    fn from(suite: &TestSuite) -> Self {
        Self {
            id: suite.id.clone(),
            name: suite.name.clone(),
            description: suite.description.clone(),
            case_count: suite.cases.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::AssertType;
    use serde_json::json;

    #[test]
    fn test_suite_serialization_roundtrip() {
        let suite = TestSuite {
            id: "test-suite-1".to_string(),
            name: "My Test Suite".to_string(),
            description: Some("A test suite".to_string()),
            context: TestContext::Rhai {
                script: "fn transform(data, attrs) { result(data, attrs) }".to_string(),
            },
            source_ref: Some(SourceRef {
                service_name: "udf-anonymizer".to_string(),
                resource: "mask_pii.rhai".to_string(),
            }),
            input_schema: Some(json!({"type": "object"})),
            cases: vec![TestCase {
                name: "basic test".to_string(),
                input: json!({"name": "John"}),
                expected: Some(json!({"name": "John"})),
                assertions: vec![TestAssertion {
                    path: "name".to_string(),
                    assert_type: AssertType::Equals,
                    expected_value: json!("John"),
                }],
                strict_mode: false,
            }],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let serialized = serde_json::to_string(&suite).expect("serialize");
        let deserialized: TestSuite = serde_json::from_str(&serialized).expect("deserialize");

        assert_eq!(deserialized.id, "test-suite-1");
        assert_eq!(deserialized.name, "My Test Suite");
        assert_eq!(deserialized.cases.len(), 1);
        assert_eq!(deserialized.cases[0].assertions.len(), 1);
        match &deserialized.context {
            TestContext::Rhai { script } => {
                assert!(script.contains("transform"));
            }
            _ => panic!("expected Rhai context"),
        }
    }

    #[test]
    fn test_suite_summary_from_suite() {
        let suite = TestSuite {
            id: "suite-1".to_string(),
            name: "Suite One".to_string(),
            description: Some("Description".to_string()),
            context: TestContext::Rhai {
                script: "fn transform(data, attrs) { result(data, attrs) }".to_string(),
            },
            source_ref: None,
            input_schema: None,
            cases: vec![
                TestCase {
                    name: "case 1".to_string(),
                    input: json!({}),
                    expected: None,
                    assertions: vec![],
                    strict_mode: false,
                },
                TestCase {
                    name: "case 2".to_string(),
                    input: json!({}),
                    expected: None,
                    assertions: vec![],
                    strict_mode: false,
                },
            ],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let summary = TestSuiteSummary::from(&suite);

        assert_eq!(summary.id, "suite-1");
        assert_eq!(summary.name, "Suite One");
        assert_eq!(summary.case_count, 2);
    }

    #[tokio::test]
    async fn noop_store_get_returns_not_found() {
        let store = NoopTestSuiteStore;
        let result = store.get("any-id").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            TestSuiteStoreError::NotFound(id) => assert_eq!(id, "any-id"),
            _ => panic!("expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn noop_store_list_returns_empty() {
        let store = NoopTestSuiteStore;
        let result = store.list().await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn noop_store_save_returns_ok() {
        let store = NoopTestSuiteStore;
        let context = TestContext::Rhai {
            script: "fn transform(data, attrs) { result(data, attrs) }".to_string(),
        };
        let suite = TestSuite::new("id".to_string(), "name".to_string(), context);
        let result = store.save(&suite).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn noop_store_delete_returns_ok() {
        let store = NoopTestSuiteStore;
        let result = store.delete("any-id").await;

        assert!(result.is_ok());
    }
}
