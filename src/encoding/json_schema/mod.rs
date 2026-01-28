use mongodb::bson::Document;
use serde_json::Value as JsonValue;

mod bson;
mod builder;
mod json;

pub use builder::build_schema;

/// Type alias for JSON Schema representation
pub type JsonSchema = serde_json::Value;

#[derive(thiserror::Error, Debug)]
pub enum JsonSchemaError {
    #[error("JSON Schema validation error: {0}")]
    ValidationError(String),

    #[error("data is not an object")]
    NotObject,

    #[error("data is not an array")]
    NotArray,

    #[error("schema is missing 'properties' field")]
    MissingPropertiesField,

    #[error("schema is missing 'items' field")]
    MissingItemsField,

    #[error("schema 'properties' field is not an object")]
    PropertiesNotObject,

    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("BSON projection error: {0}")]
    BsonError(String),
}

enum SchemaType {
    Object,
    Array,
    String,
    Number,
    Integer,
    Boolean,
    Null,

    // Internal unknown / unsupported schema type
    Unknown,
}

impl From<&JsonSchema> for SchemaType {
    fn from(schema: &JsonSchema) -> Self {
        match schema.get("type").and_then(|t| t.as_str()) {
            Some("object") => SchemaType::Object,
            Some("array") => SchemaType::Array,
            Some("string") => SchemaType::String,
            Some("number") => SchemaType::Number,
            Some("integer") => SchemaType::Integer,
            Some("boolean") => SchemaType::Boolean,
            Some("null") => SchemaType::Null,
            _ => SchemaType::Unknown,
        }
    }
}

/// Applies the given JSON Schema to the provided JSON bytes
pub fn apply_to_vec(json_bytes: &[u8], schema: &JsonSchema) -> Result<Vec<u8>, JsonSchemaError> {
    let parsed: JsonValue = serde_json::from_slice(json_bytes)?;
    apply_to_value(&parsed, schema)
}

/// Applies the given JSON Schema to the provided JSON value
pub fn apply_to_value(
    json_value: &JsonValue,
    schema: &JsonSchema,
) -> Result<Vec<u8>, JsonSchemaError> {
    json::validate(json_value, schema)?;
    let projected = json::project_json(json_value, schema)?;
    Ok(serde_json::to_vec(&projected)?)
}

pub fn apply_to_doc(doc: &Document, schema: &JsonSchema) -> Result<Document, JsonSchemaError> {
    bson::project_bson(doc, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;
    use serde_json::json;

    // =========================================================================
    // Test helpers
    // =========================================================================

    /// Helper to assert a BSON document has expected string fields
    fn assert_doc_str_fields(doc: &mongodb::bson::Document, expected: &[(&str, &str)]) {
        for (key, value) in expected {
            assert_eq!(
                doc.get_str(*key).unwrap(),
                *value,
                "field '{}' mismatch",
                key
            );
        }
    }

    /// Helper to assert a BSON document does not contain certain keys
    fn assert_doc_missing_keys(doc: &mongodb::bson::Document, keys: &[&str]) {
        for key in keys {
            assert!(!doc.contains_key(*key), "field '{}' should not exist", key);
        }
    }

    /// Helper to assert JSON value has expected fields
    fn assert_json_fields(json: &serde_json::Value, expected: &[(&str, serde_json::Value)]) {
        for (key, value) in expected {
            assert_eq!(json[*key], *value, "field '{}' mismatch", key);
        }
    }

    /// Helper to assert JSON value is missing certain keys
    fn assert_json_missing_keys(json: &serde_json::Value, keys: &[&str]) {
        for key in keys {
            assert!(json.get(*key).is_none(), "field '{}' should not exist", key);
        }
    }

    /// Macro for testing schema error cases
    macro_rules! assert_schema_error {
        ($doc:expr, $schema:expr, $expected_msg:expr) => {
            let result = apply_to_doc($doc, $schema);
            assert!(result.is_err(), "expected error but got Ok");
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains($expected_msg),
                "expected error containing '{}', got '{}'",
                $expected_msg,
                err_msg
            );
        };
    }

    fn sample_schema() -> JsonValue {
        json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "age": { "type": "integer" },
                "email": { "type": "string" }
            },
            "required": ["name", "age"]
        })
    }

    fn nested_schema() -> JsonValue {
        json!({
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "address": {
                            "type": "object",
                            "properties": {
                                "city": { "type": "string" }
                            }
                        }
                    }
                }
            }
        })
    }

    fn array_schema() -> JsonValue {
        json!({
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            }
        })
    }

    // =========================================================================
    // Group 1: apply_to_doc tests (BSON projection)
    // =========================================================================

    #[test]
    fn apply_to_doc_projects_defined_fields() {
        let doc = doc! {
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
            "extra_field": "should be dropped"
        };
        let schema = sample_schema();

        let result = apply_to_doc(&doc, &schema).unwrap();

        assert_doc_str_fields(
            &result,
            &[("name", "Alice"), ("email", "alice@example.com")],
        );
        assert_eq!(result.get_i32("age").unwrap(), 30);
        assert_doc_missing_keys(&result, &["extra_field"]);
    }

    #[test]
    fn apply_to_doc_drops_extra_fields() {
        let doc = doc! {
            "name": "Bob",
            "age": 25,
            "email": "bob@example.com",
            "password": "secret123",
            "internal_id": 42,
            "metadata": { "created": "2024-01-01" }
        };
        let schema = sample_schema();

        let result = apply_to_doc(&doc, &schema).unwrap();

        assert_eq!(result.len(), 3);
        assert_doc_missing_keys(&result, &["password", "internal_id", "metadata"]);
    }

    #[test]
    fn apply_to_doc_handles_missing_optional_fields() {
        let doc = doc! {
            "name": "Charlie",
            "age": 35
        };
        let schema = sample_schema();

        let result = apply_to_doc(&doc, &schema).unwrap();

        assert_eq!(result.len(), 2);
        assert_doc_str_fields(&result, &[("name", "Charlie")]);
        assert_doc_missing_keys(&result, &["email"]);
    }

    #[test]
    fn apply_to_doc_handles_nested_objects() {
        let doc = doc! {
            "user": {
                "name": "Dave",
                "age": 40,
                "address": {
                    "city": "London",
                    "zip": "12345",
                    "country": "UK"
                },
                "extra": "drop me"
            },
            "extra_top": "also drop"
        };
        let schema = nested_schema();

        let result = apply_to_doc(&doc, &schema).unwrap();
        let user = result.get_document("user").unwrap();
        let address = user.get_document("address").unwrap();

        assert_doc_str_fields(user, &[("name", "Dave")]);
        assert_doc_missing_keys(user, &["age", "extra"]);
        assert_doc_str_fields(address, &[("city", "London")]);
        assert_doc_missing_keys(address, &["zip", "country"]);
        assert_doc_missing_keys(&result, &["extra_top"]);
    }

    #[test]
    fn apply_to_doc_handles_arrays() {
        let doc = doc! {
            "tags": ["rust", "mongodb", "json", "avro"],
            "extra": "drop"
        };
        let schema = array_schema();

        let result = apply_to_doc(&doc, &schema).unwrap();

        let tags = result.get_array("tags").unwrap();
        assert_eq!(tags.len(), 4);
        assert!(!result.contains_key("extra"));
    }

    #[test]
    fn apply_to_doc_handles_empty_arrays() {
        let doc = doc! {
            "tags": []
        };
        let schema = array_schema();

        let result = apply_to_doc(&doc, &schema).unwrap();

        let tags = result.get_array("tags").unwrap();
        assert!(tags.is_empty());
    }

    #[test]
    fn apply_to_doc_error_missing_properties_field() {
        let doc = doc! { "name": "Test" };
        let schema = json!({ "type": "object" });
        assert_schema_error!(&doc, &schema, "missing 'properties'");
    }

    #[test]
    fn apply_to_doc_error_properties_not_object() {
        let doc = doc! { "name": "Test" };
        let schema = json!({
            "type": "object",
            "properties": "not an object"
        });
        assert_schema_error!(&doc, &schema, "'properties' field is not an object");
    }

    #[test]
    fn apply_to_doc_error_top_level_not_object() {
        let doc = doc! { "name": "Test" };
        let schema = json!({ "type": "array" });
        assert_schema_error!(&doc, &schema, "top level bson schema must be an Object");
    }

    // =========================================================================
    // Group 2: apply_to_vec tests (JSON validation + projection)
    // =========================================================================

    #[test]
    fn apply_to_vec_validates_and_projects() {
        let json = json!({
            "name": "Eve",
            "age": 28,
            "email": "eve@example.com",
            "extra": "drop"
        });
        let json_bytes = serde_json::to_vec(&json).unwrap();
        let schema = sample_schema();

        let result = apply_to_vec(&json_bytes, &schema).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_json_fields(
            &parsed,
            &[
                ("name", json!("Eve")),
                ("age", json!(28)),
                ("email", json!("eve@example.com")),
            ],
        );
        assert_json_missing_keys(&parsed, &["extra"]);
    }

    #[test]
    fn apply_to_vec_validation_error_wrong_type() {
        let json = json!({
            "name": "Frank",
            "age": "not a number",
            "email": "frank@example.com"
        });
        let json_bytes = serde_json::to_vec(&json).unwrap();
        let schema = sample_schema();

        let result = apply_to_vec(&json_bytes, &schema);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("validation"));
    }

    #[test]
    fn apply_to_vec_validation_error_missing_required() {
        let json = json!({
            "name": "Grace"
        });
        let json_bytes = serde_json::to_vec(&json).unwrap();
        let schema = sample_schema();

        let result = apply_to_vec(&json_bytes, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn apply_to_vec_error_invalid_json() {
        let invalid_json = b"not valid json".to_vec();
        let schema = sample_schema();

        let result = apply_to_vec(&invalid_json, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn apply_to_vec_handles_nested_structures() {
        let json = json!({
            "user": {
                "name": "Helen",
                "address": {
                    "city": "Paris",
                    "extra": "drop"
                }
            }
        });
        let json_bytes = serde_json::to_vec(&json).unwrap();
        let schema = nested_schema();

        let result = apply_to_vec(&json_bytes, &schema).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["user"]["name"], "Helen");
        assert_eq!(parsed["user"]["address"]["city"], "Paris");
        assert!(parsed["user"]["address"].get("extra").is_none());
    }

    // =========================================================================
    // Group 3: apply_to_value tests
    // =========================================================================

    #[test]
    fn apply_to_value_projects_correctly() {
        let json_value = json!({
            "name": "Ian",
            "age": 45,
            "email": "ian@example.com",
            "sensitive": "drop this"
        });
        let schema = sample_schema();

        let result = apply_to_value(&json_value, &schema).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["name"], "Ian");
        assert_eq!(parsed["age"], 45);
        assert!(parsed.get("sensitive").is_none());
    }

    #[test]
    fn apply_to_value_validates_before_projecting() {
        let json_value = json!({
            "name": "Jack",
            "age": "invalid"
        });
        let schema = sample_schema();

        let result = apply_to_value(&json_value, &schema);

        assert!(result.is_err());
    }

    // =========================================================================
    // Group 4: Edge cases and complex scenarios
    // =========================================================================

    #[test]
    fn apply_to_doc_empty_document() {
        let doc = doc! {};
        let schema = sample_schema();

        let result = apply_to_doc(&doc, &schema).unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn apply_to_doc_deeply_nested() {
        let doc = doc! {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deep",
                        "extra": "drop"
                    }
                }
            }
        };
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
                                        "value": { "type": "string" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let result = apply_to_doc(&doc, &schema).unwrap();

        let level3 = result
            .get_document("level1")
            .unwrap()
            .get_document("level2")
            .unwrap()
            .get_document("level3")
            .unwrap();

        assert_eq!(level3.get_str("value").unwrap(), "deep");
        assert!(!level3.contains_key("extra"));
    }

    #[test]
    fn apply_to_doc_array_of_objects() {
        let doc = doc! {
            "items": [
                { "id": 1, "name": "Item1", "extra": "drop" },
                { "id": 2, "name": "Item2", "extra": "drop" }
            ]
        };
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "integer" },
                            "name": { "type": "string" }
                        }
                    }
                }
            }
        });

        let result = apply_to_doc(&doc, &schema).unwrap();

        let items = result.get_array("items").unwrap();
        assert_eq!(items.len(), 2);

        let item0 = items[0].as_document().unwrap();
        assert_eq!(item0.get_i32("id").unwrap(), 1);
        assert_eq!(item0.get_str("name").unwrap(), "Item1");
        assert!(!item0.contains_key("extra"));
    }

    #[test]
    fn apply_to_doc_mixed_types_preserved() {
        let doc = doc! {
            "string_field": "text",
            "int_field": 42,
            "bool_field": true,
            "float_field": 3.14,
            "null_field": null
        };
        let schema = json!({
            "type": "object",
            "properties": {
                "string_field": { "type": "string" },
                "int_field": { "type": "integer" },
                "bool_field": { "type": "boolean" },
                "float_field": { "type": "number" },
                "null_field": { "type": "null" }
            }
        });

        let result = apply_to_doc(&doc, &schema).unwrap();

        assert_doc_str_fields(&result, &[("string_field", "text")]);
        assert_eq!(result.get_i32("int_field").unwrap(), 42);
        assert!(result.get_bool("bool_field").unwrap());
        assert!((result.get_f64("float_field").unwrap() - 3.14).abs() < 0.01);
        assert!(result.get("null_field").unwrap().as_null().is_some());
    }

    #[test]
    fn apply_to_vec_roundtrip_with_projection() {
        let original = json!({
            "keep1": "value1",
            "keep2": 123,
            "drop1": "should not appear",
            "drop2": { "nested": "also gone" }
        });
        let schema = json!({
            "type": "object",
            "properties": {
                "keep1": { "type": "string" },
                "keep2": { "type": "integer" }
            }
        });

        let json_bytes = serde_json::to_vec(&original).unwrap();
        let result = apply_to_vec(&json_bytes, &schema).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_json_fields(
            &parsed,
            &[("keep1", json!("value1")), ("keep2", json!(123))],
        );
        assert_json_missing_keys(&parsed, &["drop1", "drop2"]);
    }
}
