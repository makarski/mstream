use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use uuid::Uuid;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::Message;
use crate::workspace::{Workspace, WorkspaceStoreError};

/// GET /workspaces
pub async fn list_workspaces(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let summaries = state
        .stores
        .workspace_store
        .list()
        .await
        .map_err(workspace_error_to_api)?;

    Ok((StatusCode::OK, Json(summaries)))
}

/// GET /workspaces/{id}
pub async fn get_workspace(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let workspace = state
        .stores
        .workspace_store
        .get(&id)
        .await
        .map_err(workspace_error_to_api)?;

    Ok((StatusCode::OK, Json(workspace)))
}

/// POST /workspaces
pub async fn save_workspace(
    State(state): State<AppState>,
    Json(mut workspace): Json<Workspace>,
) -> Result<impl IntoResponse, ApiError> {
    if workspace.id.is_empty() {
        workspace.id = Uuid::new_v4().to_string();
    }
    workspace.updated_at = chrono::Utc::now();

    state
        .stores
        .workspace_store
        .save(&workspace)
        .await
        .map_err(workspace_error_to_api)?;

    Ok((
        StatusCode::CREATED,
        Json(Message {
            message: "workspace saved".to_string(),
            item: Some(workspace),
        }),
    ))
}

/// PUT /workspaces/{id}
pub async fn update_workspace(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(mut workspace): Json<Workspace>,
) -> Result<impl IntoResponse, ApiError> {
    workspace.id = id;
    workspace.updated_at = chrono::Utc::now();

    state
        .stores
        .workspace_store
        .save(&workspace)
        .await
        .map_err(workspace_error_to_api)?;

    Ok((
        StatusCode::OK,
        Json(Message {
            message: "workspace updated".to_string(),
            item: Some(workspace),
        }),
    ))
}

/// DELETE /workspaces/{id}
pub async fn delete_workspace(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .stores
        .workspace_store
        .delete(&id)
        .await
        .map_err(workspace_error_to_api)?;

    Ok((
        StatusCode::OK,
        Json(Message::<()> {
            message: format!("workspace '{}' deleted", id),
            item: None,
        }),
    ))
}

fn workspace_error_to_api(err: WorkspaceStoreError) -> ApiError {
    match err {
        WorkspaceStoreError::NotFound(id) => {
            ApiError::NotFound(format!("workspace '{}' not found", id))
        }
        WorkspaceStoreError::MongoDb(e) => ApiError::Internal(format!("database error: {}", e)),
        WorkspaceStoreError::Other(msg) => ApiError::Internal(msg),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode};
    use serde_json::{Value, json};
    use tokio::sync::{Mutex, RwLock, mpsc};
    use tower::util::ServiceExt;

    use crate::api::{AppState, Stores};
    use crate::config::system::LogsConfig;
    use crate::job_manager::{JobManager, in_memory::InMemoryJobStore};
    use crate::logs::LogBuffer;
    use crate::provision::registry::{ServiceRegistry, in_memory::InMemoryServiceStorage};
    use crate::testing::NoopTestSuiteStore;
    use crate::workspace::{Workspace, WorkspaceStore, WorkspaceStoreError, WorkspaceSummary};

    use super::*;

    struct InMemoryWorkspaceStore {
        workspaces: Mutex<Vec<Workspace>>,
    }

    impl InMemoryWorkspaceStore {
        fn new() -> Self {
            Self {
                workspaces: Mutex::new(Vec::new()),
            }
        }

        fn with_seed(workspaces: Vec<Workspace>) -> Self {
            Self {
                workspaces: Mutex::new(workspaces),
            }
        }
    }

    #[async_trait::async_trait]
    impl WorkspaceStore for InMemoryWorkspaceStore {
        async fn get(&self, id: &str) -> Result<Workspace, WorkspaceStoreError> {
            let lock = self.workspaces.lock().await;
            lock.iter()
                .find(|w| w.id == id)
                .cloned()
                .ok_or_else(|| WorkspaceStoreError::NotFound(id.to_string()))
        }

        async fn list(&self) -> Result<Vec<WorkspaceSummary>, WorkspaceStoreError> {
            let lock = self.workspaces.lock().await;
            Ok(lock.iter().map(WorkspaceSummary::from).collect())
        }

        async fn save(&self, workspace: &Workspace) -> Result<(), WorkspaceStoreError> {
            let mut lock = self.workspaces.lock().await;
            if let Some(pos) = lock.iter().position(|w| w.id == workspace.id) {
                lock[pos] = workspace.clone();
            } else {
                lock.push(workspace.clone());
            }
            Ok(())
        }

        async fn delete(&self, id: &str) -> Result<(), WorkspaceStoreError> {
            let mut lock = self.workspaces.lock().await;
            lock.retain(|w| w.id != id);
            Ok(())
        }
    }

    fn test_app_state(store: Arc<dyn WorkspaceStore + Send + Sync>) -> AppState {
        let storage = Box::new(InMemoryServiceStorage::new());
        let registry = ServiceRegistry::new(storage, None);
        let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
        let jm = JobManager::new(
            Arc::new(RwLock::new(registry)),
            Box::new(InMemoryJobStore::new()),
            exit_tx,
        );
        AppState::new(
            Arc::new(Mutex::new(jm)),
            LogBuffer::new(100),
            LogsConfig::default(),
            Stores {
                test_suite_store: Arc::new(NoopTestSuiteStore),
                workspace_store: store,
            },
        )
    }

    fn workspace_router(store: Arc<dyn WorkspaceStore + Send + Sync>) -> axum::Router {
        use axum::routing::get;

        axum::Router::new()
            .route("/workspaces", get(list_workspaces).post(save_workspace))
            .route(
                "/workspaces/{id}",
                get(get_workspace)
                    .put(update_workspace)
                    .delete(delete_workspace),
            )
            .with_state(test_app_state(store))
    }

    async fn response_json(resp: axum::response::Response) -> Value {
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn sample_workspace(id: &str, name: &str) -> Workspace {
        Workspace {
            id: id.to_string(),
            name: name.to_string(),
            script: None,
            input: None,
            schema: None,
            test_suite_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    async fn send_get(store: Arc<InMemoryWorkspaceStore>, path: &str) -> (StatusCode, Value) {
        let resp = workspace_router(store)
            .oneshot(Request::get(path).body(String::new()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        (status, response_json(resp).await)
    }

    async fn send_json(
        store: Arc<InMemoryWorkspaceStore>,
        method: &str,
        path: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .body(body.to_string())
            .unwrap();
        let resp = workspace_router(store).oneshot(req).await.unwrap();
        let status = resp.status();
        (status, response_json(resp).await)
    }

    #[tokio::test]
    async fn list_returns_empty_when_no_workspaces() {
        let store = Arc::new(InMemoryWorkspaceStore::new());
        let (status, json) = send_get(store, "/workspaces").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json, json!([]));
    }

    #[tokio::test]
    async fn list_returns_summaries() {
        let store = Arc::new(InMemoryWorkspaceStore::with_seed(vec![
            sample_workspace("ws-1", "Alpha"),
            sample_workspace("ws-2", "Beta"),
        ]));
        let (status, json) = send_get(store, "/workspaces").await;

        assert_eq!(status, StatusCode::OK);
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["name"], "Alpha");
        assert_eq!(arr[1]["name"], "Beta");
    }

    #[tokio::test]
    async fn get_returns_workspace() {
        let store = Arc::new(InMemoryWorkspaceStore::with_seed(vec![sample_workspace(
            "ws-1", "Alpha",
        )]));
        let (status, json) = send_get(store, "/workspaces/ws-1").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["id"], "ws-1");
        assert_eq!(json["name"], "Alpha");
    }

    #[tokio::test]
    async fn get_returns_404_when_not_found() {
        let store = Arc::new(InMemoryWorkspaceStore::new());
        let (status, _) = send_get(store, "/workspaces/missing").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn save_assigns_uuid_when_id_empty() {
        let store = Arc::new(InMemoryWorkspaceStore::new());
        let (status, json) = send_json(
            store.clone(),
            "POST",
            "/workspaces",
            json!({"name": "New Workspace"}),
        )
        .await;

        assert_eq!(status, StatusCode::CREATED);
        let id = json["item"]["id"].as_str().unwrap();
        assert!(!id.is_empty(), "should assign a UUID");
        assert_eq!(store.get(id).await.unwrap().name, "New Workspace");
    }

    #[tokio::test]
    async fn save_preserves_provided_id() {
        let store = Arc::new(InMemoryWorkspaceStore::new());
        let (status, json) = send_json(
            store,
            "POST",
            "/workspaces",
            json!({"id": "custom-id", "name": "Custom"}),
        )
        .await;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["item"]["id"], "custom-id");
    }

    #[tokio::test]
    async fn update_sets_id_from_path() {
        let store = Arc::new(InMemoryWorkspaceStore::with_seed(vec![sample_workspace(
            "ws-1", "Old Name",
        )]));
        let (status, json) = send_json(
            store.clone(),
            "PUT",
            "/workspaces/ws-1",
            json!({"name": "New Name"}),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["item"]["id"], "ws-1");
        assert_eq!(json["item"]["name"], "New Name");
        assert_eq!(store.get("ws-1").await.unwrap().name, "New Name");
    }

    #[tokio::test]
    async fn delete_removes_workspace() {
        let store = Arc::new(InMemoryWorkspaceStore::with_seed(vec![sample_workspace(
            "ws-1", "Alpha",
        )]));
        let (status, _) = send_get(store.clone(), "/workspaces/ws-1").await;
        assert_eq!(status, StatusCode::OK);

        let resp = workspace_router(store.clone())
            .oneshot(
                Request::delete("/workspaces/ws-1")
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(store.get("ws-1").await.is_err());
    }

    #[test]
    fn error_mapping_not_found() {
        let err = WorkspaceStoreError::NotFound("ws-99".to_string());
        let api_err = workspace_error_to_api(err);
        match api_err {
            ApiError::NotFound(msg) => assert!(msg.contains("ws-99")),
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[test]
    fn error_mapping_other() {
        let err = WorkspaceStoreError::Other("boom".to_string());
        let api_err = workspace_error_to_api(err);
        match api_err {
            ApiError::Internal(msg) => assert_eq!(msg, "boom"),
            other => panic!("expected Internal, got {:?}", other),
        }
    }
}
