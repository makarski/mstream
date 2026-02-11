use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use uuid::Uuid;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::Message;
use crate::schema::{SchemaEntry, SchemaRegistryError};

/// GET /schemas
pub async fn list_schemas(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let summaries = state
        .schema_registry
        .list()
        .await
        .map_err(schema_error_to_api)?;

    Ok((StatusCode::OK, Json(summaries)))
}

/// GET /schemas/{id}
pub async fn get_schema(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let entry = state
        .schema_registry
        .get(&id)
        .await
        .map_err(schema_error_to_api)?;

    Ok((StatusCode::OK, Json(entry)))
}

/// POST /schemas
pub async fn save_schema(
    State(state): State<AppState>,
    Json(mut entry): Json<SchemaEntry>,
) -> Result<impl IntoResponse, ApiError> {
    if entry.id.is_empty() {
        entry.id = Uuid::new_v4().to_string();
    }
    entry.updated_at = chrono::Utc::now();

    state
        .schema_registry
        .save(&entry)
        .await
        .map_err(schema_error_to_api)?;

    Ok((
        StatusCode::CREATED,
        Json(Message {
            message: "schema saved".to_string(),
            item: Some(entry),
        }),
    ))
}

/// DELETE /schemas/{id}
pub async fn delete_schema(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .schema_registry
        .delete(&id)
        .await
        .map_err(schema_error_to_api)?;

    Ok((
        StatusCode::OK,
        Json(Message::<()> {
            message: format!("schema '{}' deleted", id),
            item: None,
        }),
    ))
}

fn schema_error_to_api(err: SchemaRegistryError) -> ApiError {
    match err {
        SchemaRegistryError::NotFound(id) => {
            ApiError::NotFound(format!("schema '{}' not found", id))
        }
        SchemaRegistryError::MongoDb(e) => ApiError::Internal(format!("database error: {}", e)),
        SchemaRegistryError::Parse(msg) => {
            ApiError::BadRequest(format!("schema parse error: {}", msg))
        }
        SchemaRegistryError::Other(msg) => ApiError::Internal(msg),
    }
}
