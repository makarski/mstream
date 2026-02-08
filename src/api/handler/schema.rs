use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::api::AppState;
use crate::api::error::ApiError;
use crate::api::types::{SchemaFillRequest, SchemaQuery};
use crate::config::Service;
use crate::encoding::json_schema::SchemaFiller;
use crate::schema::introspect::SchemaIntrospector;

/// GET /services/{name}/schema
pub async fn get_resource_schema(
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
pub async fn fill_schema(
    Json(req): Json<SchemaFillRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut filler = SchemaFiller::new();
    let filled = filler.fill(&req.schema);
    Ok((StatusCode::OK, Json(filled)))
}

#[cfg(test)]
mod tests {
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
