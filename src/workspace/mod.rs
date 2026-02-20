use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    #[serde(default)]
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script: Option<ResourceRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_suite_id: Option<String>,
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "Utc::now")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRef {
    pub service_name: String,
    pub resource: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRef {
    pub schema_id: String,
    pub service_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceSummary {
    pub id: String,
    pub name: String,
}

#[derive(thiserror::Error, Debug)]
pub enum WorkspaceStoreError {
    #[error("workspace not found: {0}")]
    NotFound(String),
    #[error("MongoDB error: {0}")]
    MongoDb(#[from] mongodb::error::Error),
    #[error("{0}")]
    Other(String),
}

pub type DynWorkspaceStore = Arc<dyn WorkspaceStore + Send + Sync>;

#[async_trait]
pub trait WorkspaceStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<Workspace, WorkspaceStoreError>;
    async fn list(&self) -> Result<Vec<WorkspaceSummary>, WorkspaceStoreError>;
    async fn save(&self, workspace: &Workspace) -> Result<(), WorkspaceStoreError>;
    async fn delete(&self, id: &str) -> Result<(), WorkspaceStoreError>;
}

pub struct NoopWorkspaceStore;

#[async_trait]
impl WorkspaceStore for NoopWorkspaceStore {
    async fn get(&self, id: &str) -> Result<Workspace, WorkspaceStoreError> {
        Err(WorkspaceStoreError::NotFound(id.to_string()))
    }

    async fn list(&self) -> Result<Vec<WorkspaceSummary>, WorkspaceStoreError> {
        Ok(vec![])
    }

    async fn save(&self, _workspace: &Workspace) -> Result<(), WorkspaceStoreError> {
        Ok(())
    }

    async fn delete(&self, _id: &str) -> Result<(), WorkspaceStoreError> {
        Ok(())
    }
}

impl From<&Workspace> for WorkspaceSummary {
    fn from(ws: &Workspace) -> Self {
        Self {
            id: ws.id.clone(),
            name: ws.name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn workspace_serialization_roundtrip() {
        let ws = Workspace {
            id: "ws-1".to_string(),
            name: "My Workspace".to_string(),
            script: Some(ResourceRef {
                service_name: "udf-anonymizer".to_string(),
                resource: "mask_pii.rhai".to_string(),
            }),
            input: Some(r#"{"name": "John"}"#.to_string()),
            schema: Some(SchemaRef {
                schema_id: "schema-1".to_string(),
                service_name: "etl-test-data".to_string(),
            }),
            test_suite_id: Some("suite-1".to_string()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let serialized = serde_json::to_string(&ws).expect("serialize");
        let deserialized: Workspace = serde_json::from_str(&serialized).expect("deserialize");

        assert_eq!(deserialized.id, "ws-1");
        assert_eq!(deserialized.name, "My Workspace");
        assert!(deserialized.script.is_some());
        assert_eq!(
            deserialized.script.as_ref().unwrap().service_name,
            "udf-anonymizer"
        );
        assert_eq!(deserialized.input.as_deref(), Some(r#"{"name": "John"}"#));
        assert_eq!(deserialized.schema.as_ref().unwrap().schema_id, "schema-1");
        assert_eq!(deserialized.test_suite_id.as_deref(), Some("suite-1"));
    }

    #[test]
    fn workspace_serialization_minimal() {
        let ws = Workspace {
            id: "ws-2".to_string(),
            name: "Empty Workspace".to_string(),
            script: None,
            input: None,
            schema: None,
            test_suite_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let serialized = serde_json::to_string(&ws).expect("serialize");
        assert!(!serialized.contains("script"));
        assert!(!serialized.contains("input"));
        assert!(!serialized.contains("schema"));
        assert!(!serialized.contains("test_suite_id"));

        let deserialized: Workspace = serde_json::from_str(&serialized).expect("deserialize");
        assert_eq!(deserialized.name, "Empty Workspace");
        assert!(deserialized.script.is_none());
    }

    #[test]
    fn workspace_summary_from_workspace() {
        let ws = Workspace {
            id: "ws-1".to_string(),
            name: "Test WS".to_string(),
            script: None,
            input: None,
            schema: None,
            test_suite_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let summary = WorkspaceSummary::from(&ws);
        assert_eq!(summary.id, "ws-1");
        assert_eq!(summary.name, "Test WS");
    }

    #[tokio::test]
    async fn noop_store_get_returns_not_found() {
        let store = NoopWorkspaceStore;
        let result = store.get("any-id").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            WorkspaceStoreError::NotFound(id) => assert_eq!(id, "any-id"),
            _ => panic!("expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn noop_store_list_returns_empty() {
        let store = NoopWorkspaceStore;
        let result = store.list().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn noop_store_save_returns_ok() {
        let store = NoopWorkspaceStore;
        let ws = Workspace {
            id: "id".to_string(),
            name: "name".to_string(),
            script: None,
            input: None,
            schema: None,
            test_suite_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(store.save(&ws).await.is_ok());
    }

    #[tokio::test]
    async fn noop_store_delete_returns_ok() {
        let store = NoopWorkspaceStore;
        assert!(store.delete("any-id").await.is_ok());
    }
}
