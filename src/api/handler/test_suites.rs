use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use uuid::Uuid;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::Message;
use crate::testing::store::{TestSuite, TestSuiteStoreError};

/// GET /test-suites
pub async fn list_test_suites(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let summaries = state
        .test_suite_store
        .list()
        .await
        .map_err(test_suite_error_to_api)?;

    Ok((StatusCode::OK, Json(summaries)))
}

/// GET /test-suites/{id}
pub async fn get_test_suite(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let suite = state
        .test_suite_store
        .get(&id)
        .await
        .map_err(test_suite_error_to_api)?;

    Ok((StatusCode::OK, Json(suite)))
}

/// POST /test-suites
pub async fn save_test_suite(
    State(state): State<AppState>,
    Json(mut suite): Json<TestSuite>,
) -> Result<impl IntoResponse, ApiError> {
    if suite.id.is_empty() {
        suite.id = Uuid::new_v4().to_string();
    }
    suite.updated_at = chrono::Utc::now();

    state
        .test_suite_store
        .save(&suite)
        .await
        .map_err(test_suite_error_to_api)?;

    Ok((
        StatusCode::CREATED,
        Json(Message {
            message: "test suite saved".to_string(),
            item: Some(suite),
        }),
    ))
}

/// DELETE /test-suites/{id}
pub async fn delete_test_suite(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .test_suite_store
        .delete(&id)
        .await
        .map_err(test_suite_error_to_api)?;

    Ok((
        StatusCode::OK,
        Json(Message::<()> {
            message: format!("test suite '{}' deleted", id),
            item: None,
        }),
    ))
}

fn test_suite_error_to_api(err: TestSuiteStoreError) -> ApiError {
    match err {
        TestSuiteStoreError::NotFound(id) => {
            ApiError::NotFound(format!("test suite '{}' not found", id))
        }
        TestSuiteStoreError::MongoDb(e) => ApiError::Internal(format!("database error: {}", e)),
        TestSuiteStoreError::Other(msg) => ApiError::Internal(msg),
    }
}
