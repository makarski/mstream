use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use uuid::Uuid;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::Message;
use crate::schema::{SchemaEntry, SchemaRegistryError};

async fn resolve_schema_registry(
    state: &AppState,
    service_name: &str,
) -> Result<crate::schema::DynSchemaRegistry, ApiError> {
    let jm = state.job_manager.lock().await;
    let registry = jm.service_registry.read().await;
    registry
        .schema_registry_for(service_name)
        .await
        .map_err(|e| ApiError::NotFound(format!("service '{}': {}", service_name, e)))
}

/// GET /services/{name}/schemas
pub async fn list_schemas(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let registry = resolve_schema_registry(&state, &service_name).await?;
    let summaries = registry.list().await.map_err(schema_error_to_api)?;

    Ok((StatusCode::OK, Json(summaries)))
}

/// GET /services/{name}/schemas/{id}
pub async fn get_schema(
    State(state): State<AppState>,
    Path((service_name, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let registry = resolve_schema_registry(&state, &service_name).await?;
    let entry = registry.get(&id).await.map_err(schema_error_to_api)?;

    Ok((StatusCode::OK, Json(entry)))
}

/// POST /services/{name}/schemas
pub async fn save_schema(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
    Json(mut entry): Json<SchemaEntry>,
) -> Result<impl IntoResponse, ApiError> {
    if entry.id.is_empty() {
        entry.id = Uuid::new_v4().to_string();
    }
    entry.updated_at = chrono::Utc::now();

    let registry = resolve_schema_registry(&state, &service_name).await?;
    registry.save(&entry).await.map_err(schema_error_to_api)?;

    Ok((
        StatusCode::CREATED,
        Json(Message {
            message: "schema saved".to_string(),
            item: Some(entry),
        }),
    ))
}

/// DELETE /services/{name}/schemas/{id}
pub async fn delete_schema(
    State(state): State<AppState>,
    Path((service_name, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let registry = resolve_schema_registry(&state, &service_name).await?;
    registry.delete(&id).await.map_err(schema_error_to_api)?;

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
