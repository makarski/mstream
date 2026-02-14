use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::{MaskedJson, Message, ResourceInfo, ServiceResourcesResponse};
use crate::config::Service;
use crate::config::service_config::UdfScript;
use crate::job_manager::error::JobManagerError;

#[derive(Deserialize, Default)]
pub struct ListServicesQuery {
    pub provider: Option<String>,
}

#[derive(Serialize)]
pub struct ResourceContentResponse {
    pub filename: String,
    pub content: String,
}

#[derive(Deserialize)]
pub struct UpdateResourceRequest {
    pub content: String,
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

/// PUT /services/{name}/resources/{resource}
pub async fn update_resource_content(
    State(state): State<AppState>,
    Path((service_name, resource)): Path<(String, String)>,
    Json(body): Json<UpdateResourceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    info!(
        "updating resource '{}' for service '{}'",
        resource, service_name
    );

    let jm = state.job_manager.lock().await;
    jm.update_resource(&service_name, &resource, &body.content)
        .await?;

    Ok((
        StatusCode::OK,
        Json(ResourceContentResponse {
            filename: resource,
            content: body.content,
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

enum ServiceResourceQuery {
    MongoDb {
        client: mongodb::Client,
        db_name: String,
    },
    Udf {
        script_path: String,
        sources: Option<Vec<UdfScript>>,
    },
}

/// GET /services/{name}/resources
pub async fn list_service_resources(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let query = {
        let jm = state.job_manager.lock().await;
        let registry = jm.service_registry.read().await;

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
                ServiceResourceQuery::MongoDb {
                    client,
                    db_name: cfg.db_name.clone(),
                }
            }
            Service::Udf(cfg) => ServiceResourceQuery::Udf {
                script_path: cfg.script_path.clone(),
                sources: cfg.sources.clone(),
            },
            _ => {
                return Err(ApiError::BadRequest(
                    "resource listing not supported for this service type".to_string(),
                ));
            }
        }
    };

    let resources = match query {
        ServiceResourceQuery::MongoDb { client, db_name } => {
            let db = client.database(&db_name);
            let collections = db
                .list_collection_names()
                .await
                .map_err(|e| ApiError::Internal(format!("failed to list collections: {}", e)))?;

            collections
                .into_iter()
                .filter(|name| !name.starts_with("system."))
                .map(|name| ResourceInfo {
                    name,
                    resource_type: "collection".to_string(),
                })
                .collect()
        }
        ServiceResourceQuery::Udf {
            script_path,
            sources,
        } => udf_script_resources(&script_path, sources.as_deref()).await,
    };

    Ok((
        StatusCode::OK,
        Json(ServiceResourcesResponse {
            service_name,
            resources,
        }),
    ))
}

async fn udf_script_resources(
    script_path: &str,
    sources: Option<&[UdfScript]>,
) -> Vec<ResourceInfo> {
    if let Some(scripts) = sources {
        return scripts
            .iter()
            .map(|s| ResourceInfo {
                name: s.filename.clone(),
                resource_type: "script".to_string(),
            })
            .collect();
    }

    let path = std::path::Path::new(script_path);

    if path.is_file() {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| script_path.to_string());
        return vec![ResourceInfo {
            name,
            resource_type: "script".to_string(),
        }];
    }

    if !path.is_dir() {
        return Vec::new();
    }

    let Ok(mut entries) = tokio::fs::read_dir(path).await else {
        return Vec::new();
    };

    let mut resources = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let entry_path = entry.path();
        if entry_path.is_file() {
            if let Some(name) = entry_path.file_name() {
                resources.push(ResourceInfo {
                    name: name.to_string_lossy().into_owned(),
                    resource_type: "script".to_string(),
                });
            }
        }
    }
    resources
}

/// GET /services/{name}/resources/{resource}
pub async fn get_resource_content(
    State(state): State<AppState>,
    Path((service_name, resource)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let (script_path, sources) = {
        let jm = state.job_manager.lock().await;
        let registry = jm.service_registry.read().await;

        let service_definition = registry
            .service_definition(&service_name)
            .await
            .map_err(|err| ApiError::NotFound(err.to_string()))?;

        match &service_definition {
            Service::Udf(cfg) => (cfg.script_path.clone(), cfg.sources.clone()),
            _ => {
                return Err(ApiError::BadRequest(
                    "script content only available for UDF services".to_string(),
                ));
            }
        }
    };

    let content = resolve_resource_content(&resource, &script_path, sources.as_deref())
        .await
        .ok_or_else(|| ApiError::NotFound(format!("resource '{}' not found", resource)))?;

    Ok((
        StatusCode::OK,
        Json(ResourceContentResponse {
            filename: resource,
            content,
        }),
    ))
}

async fn resolve_resource_content(
    filename: &str,
    script_path: &str,
    sources: Option<&[UdfScript]>,
) -> Option<String> {
    if let Some(scripts) = sources {
        return scripts
            .iter()
            .find(|s| s.filename == filename)
            .map(|s| s.content.clone());
    }

    let path = std::path::Path::new(script_path);

    if path.is_file() {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if name == filename {
            return tokio::fs::read_to_string(path).await.ok();
        }
        return None;
    }

    if path.is_dir() {
        let candidate = path.join(filename);
        if candidate.is_file() {
            return tokio::fs::read_to_string(&candidate).await.ok();
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn script(filename: &str, content: &str) -> UdfScript {
        UdfScript {
            filename: filename.to_string(),
            content: content.to_string(),
        }
    }

    fn write_file(dir: &TempDir, name: &str, content: &str) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    #[tokio::test]
    async fn udf_resources_from_inline_sources() {
        let sources = vec![
            script("transform.rhai", "fn transform(d, a) { result(d, a) }"),
            script(
                "mask.rhai",
                "fn transform(d, a) { d.email = \"***\"; result(d, a) }",
            ),
        ];
        let result = udf_script_resources("unused", Some(&sources)).await;
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "transform.rhai");
        assert_eq!(result[0].resource_type, "script");
        assert_eq!(result[1].name, "mask.rhai");
    }

    #[tokio::test]
    async fn udf_resources_from_single_file() {
        let dir = TempDir::new().unwrap();
        let path = write_file(&dir, "transform.rhai", "// script");
        let result = udf_script_resources(path.to_str().unwrap(), None).await;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "transform.rhai");
        assert_eq!(result[0].resource_type, "script");
    }

    #[tokio::test]
    async fn udf_resources_from_directory() {
        let dir = TempDir::new().unwrap();
        write_file(&dir, "a.rhai", "// a");
        write_file(&dir, "b.rhai", "// b");
        let mut result = udf_script_resources(dir.path().to_str().unwrap(), None).await;
        result.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "a.rhai");
        assert_eq!(result[1].name, "b.rhai");
    }

    #[tokio::test]
    async fn udf_resources_nonexistent_path() {
        let result = udf_script_resources("/tmp/does-not-exist-12345", None).await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn resolve_content_from_inline_sources_match() {
        let sources = vec![
            script("mask.rhai", "// mask script"),
            script("transform.rhai", "// transform script"),
        ];
        let content = resolve_resource_content("transform.rhai", "unused", Some(&sources)).await;
        assert_eq!(content, Some("// transform script".to_string()));
    }

    #[tokio::test]
    async fn resolve_content_from_inline_sources_no_match() {
        let sources = vec![script("mask.rhai", "// mask")];
        let content = resolve_resource_content("other.rhai", "unused", Some(&sources)).await;
        assert_eq!(content, None);
    }

    #[tokio::test]
    async fn resolve_content_from_single_file_match() {
        let dir = TempDir::new().unwrap();
        let path = write_file(
            &dir,
            "transform.rhai",
            "fn transform(d, a) { result(d, a) }",
        );
        let content =
            resolve_resource_content("transform.rhai", path.to_str().unwrap(), None).await;
        assert_eq!(
            content,
            Some("fn transform(d, a) { result(d, a) }".to_string())
        );
    }

    #[tokio::test]
    async fn resolve_content_from_single_file_wrong_name() {
        let dir = TempDir::new().unwrap();
        let path = write_file(&dir, "transform.rhai", "// content");
        let content = resolve_resource_content("other.rhai", path.to_str().unwrap(), None).await;
        assert_eq!(content, None);
    }

    #[tokio::test]
    async fn resolve_content_from_directory_match() {
        let dir = TempDir::new().unwrap();
        write_file(&dir, "mask.rhai", "// mask content");
        write_file(&dir, "transform.rhai", "// transform content");
        let content =
            resolve_resource_content("mask.rhai", dir.path().to_str().unwrap(), None).await;
        assert_eq!(content, Some("// mask content".to_string()));
    }

    #[tokio::test]
    async fn resolve_content_from_directory_no_match() {
        let dir = TempDir::new().unwrap();
        write_file(&dir, "transform.rhai", "// content");
        let content =
            resolve_resource_content("missing.rhai", dir.path().to_str().unwrap(), None).await;
        assert_eq!(content, None);
    }
}
