use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use tracing::info;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::{MaskedJson, Message, ResourceInfo, ServiceResourcesResponse};
use crate::config::Service;
use crate::job_manager::error::JobManagerError;

#[derive(Deserialize, Default)]
pub struct ListServicesQuery {
    pub provider: Option<String>,
}

/// GET /services
pub async fn list_services(
    State(state): State<AppState>,
    Query(query): Query<ListServicesQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let jm = state.job_manager.lock().await;

    let services = jm
        .list_services()
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to list services: {}", e)))?;

    let filtered: Vec<_> = services
        .into_iter()
        .filter(|s| !s.is_system)
        .filter(|s| match &query.provider {
            Some(provider) => s.service.provider() == provider,
            None => true,
        })
        .collect();

    Ok((StatusCode::OK, MaskedJson(filtered)))
}

/// POST /services
pub async fn create_service(
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
pub async fn remove_service(
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

/// GET /services/{name}
pub async fn get_one_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let jm = state.job_manager.lock().await;
    let service = jm.get_service(&name).await?;
    Ok((StatusCode::OK, MaskedJson(service)))
}

/// GET /services/{name}/resources
pub async fn list_service_resources(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    // Extract what we need under locks, then drop them before async MongoDB call
    let (client, db_name) = {
        let jm = state.job_manager.lock().await;
        let registry = jm.service_registry.read().await;

        // Exclude system services from resource listing
        if registry.is_system_service(&service_name) {
            return Err(ApiError::BadRequest(
                "resource listing not available for system services".to_string(),
            ));
        }

        let service_definition = registry
            .service_definition(&service_name)
            .await
            .map_err(|err| ApiError::NotFound(err.to_string()))?;

        match &service_definition {
            Service::MongoDb(cfg) => {
                let client = registry.mongodb_client(&cfg.name).await.map_err(|e| {
                    ApiError::Internal(format!("failed to get MongoDB client: {}", e))
                })?;
                let db_name = cfg.db_name.clone();
                (client, db_name)
            }
            _ => {
                return Err(ApiError::BadRequest(
                    "resource listing not supported for this service type".to_string(),
                ));
            }
        }
    }; // locks dropped here

    // Perform async MongoDB operation without holding locks
    let db = client.database(&db_name);
    let collections = db
        .list_collection_names()
        .await
        .map_err(|e| ApiError::Internal(format!("failed to list collections: {}", e)))?;

    let resources: Vec<ResourceInfo> = collections
        .into_iter()
        .filter(|name| !name.starts_with("system."))
        .map(|name| ResourceInfo {
            name,
            resource_type: "collection".to_string(),
        })
        .collect();

    Ok((
        StatusCode::OK,
        Json(ServiceResourcesResponse {
            service_name,
            resources,
        }),
    ))
}
