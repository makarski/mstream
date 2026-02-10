//! Schema derivation for test case generation.
//!
//! This module provides functions to infer JSON schemas from values
//! and derive assertions from expected output.

use mongodb::bson::Document;
use serde_json::{Value as JsonValue, json};

use crate::api::types::{AssertType, TestAssertion};
use crate::encoding::json_schema::build_schema;

// =============================================================================
// Schema Derivation
// =============================================================================

/// Derive a JSON schema from a sample value.
pub fn derive_schema_from_value(value: &JsonValue) -> JsonValue {
    // Convert to BSON Document for schema builder compatibility
    if let Ok(doc) = serde_json::from_value::<Document>(value.clone()) {
        build_schema(&[doc])
    } else {
        // Fallback for non-object values
        infer_type_schema(value)
    }
}

/// Infer a simple type schema from a JSON value.
pub fn infer_type_schema(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Null => json!({"type": "null"}),
        JsonValue::Bool(_) => json!({"type": "boolean"}),
        JsonValue::Number(n) => {
            if n.is_i64() || n.is_u64() {
                json!({"type": "integer"})
            } else {
                json!({"type": "number"})
            }
        }
        JsonValue::String(_) => json!({"type": "string"}),
        JsonValue::Array(arr) => {
            let items_schema = if let Some(first) = arr.first() {
                infer_type_schema(first)
            } else {
                json!({})
            };
            json!({"type": "array", "items": items_schema})
        }
        JsonValue::Object(_) => json!({"type": "object"}),
    }
}

// =============================================================================
// Assertion Derivation
// =============================================================================

/// Derive assertions from an expected output value.
///
/// Walks the value structure and creates equality assertions for all leaf values.
pub fn derive_assertions(expected: &JsonValue) -> Vec<TestAssertion> {
    let mut assertions = Vec::new();
    collect_assertions(expected, "", &mut assertions);
    assertions
}

fn collect_assertions(value: &JsonValue, path: &str, assertions: &mut Vec<TestAssertion>) {
    match value {
        JsonValue::Object(obj) => collect_object_assertions(obj, path, assertions),
        JsonValue::Array(arr) => collect_array_assertions(arr, path, value, assertions),
        _ => add_leaf_assertion(path, value, assertions),
    }
}

fn collect_object_assertions(
    obj: &serde_json::Map<String, JsonValue>,
    path: &str,
    assertions: &mut Vec<TestAssertion>,
) {
    for (key, val) in obj {
        let field_path = build_field_path(path, key);

        if is_non_empty_object(val) {
            collect_assertions(val, &field_path, assertions);
        } else {
            add_leaf_assertion(&field_path, val, assertions);
        }
    }
}

fn collect_array_assertions(
    arr: &[JsonValue],
    path: &str,
    value: &JsonValue,
    assertions: &mut Vec<TestAssertion>,
) {
    if path.is_empty() {
        // Top-level array - add assertions for each indexed element
        for (i, item) in arr.iter().enumerate() {
            collect_assertions(item, &format!("[{}]", i), assertions);
        }
    } else {
        add_leaf_assertion(path, value, assertions);
    }
}

fn add_leaf_assertion(path: &str, value: &JsonValue, assertions: &mut Vec<TestAssertion>) {
    if !path.is_empty() {
        assertions.push(TestAssertion {
            path: path.to_string(),
            assert_type: AssertType::Equals,
            expected_value: value.clone(),
        });
    }
}

fn build_field_path(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{}.{}", prefix, key)
    }
}

fn is_non_empty_object(val: &JsonValue) -> bool {
    val.as_object().is_some_and(|obj| !obj.is_empty())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn find_assertion<'a>(
        assertions: &'a [TestAssertion],
        path: &str,
    ) -> Option<&'a TestAssertion> {
        assertions.iter().find(|a| a.path == path)
    }

    // =========================================================================
    // derive_assertions tests
    // =========================================================================

    #[test]
    fn test_derive_assertions_flat_object() {
        let assertions = derive_assertions(&json!({"name": "John", "age": 30}));
        assert_eq!(assertions.len(), 2);
        assert_eq!(
            find_assertion(&assertions, "name").unwrap().expected_value,
            json!("John")
        );
        assert_eq!(
            find_assertion(&assertions, "age").unwrap().expected_value,
            json!(30)
        );
    }

    #[test]
    fn test_derive_assertions_nested_object() {
        let assertions = derive_assertions(&json!({"user": {"name": "John"}}));
        assert_eq!(assertions.len(), 1);
        assert_eq!(
            find_assertion(&assertions, "user.name")
                .unwrap()
                .expected_value,
            json!("John")
        );
    }

    #[test]
    fn test_derive_assertions_leaf_values() {
        let cases = [
            (json!({"data": {}}), "data", json!({})),
            (json!({"tags": ["a"]}), "tags", json!(["a"])),
            (json!({"opt": null}), "opt", JsonValue::Null),
        ];
        for (input, path, expected) in cases {
            let assertions = derive_assertions(&input);
            assert_eq!(
                find_assertion(&assertions, path).unwrap().expected_value,
                expected
            );
        }
    }

    // =========================================================================
    // derive_schema_from_value tests
    // =========================================================================

    #[test]
    fn test_derive_schema_from_object() {
        let schema = derive_schema_from_value(&json!({"name": "test", "count": 42}));
        let props = schema["properties"].as_object().unwrap();

        assert!(props.contains_key("name"));
        assert!(props.contains_key("count"));
    }

    #[test]
    fn test_derive_schema_primitives() {
        let cases = [
            (json!(null), "null"),
            (json!(true), "boolean"),
            (json!(42), "integer"),
            (json!(3.14), "number"),
            (json!("text"), "string"),
        ];
        for (value, expected_type) in cases {
            assert_eq!(derive_schema_from_value(&value)["type"], expected_type);
        }
    }

    #[test]
    fn test_derive_schema_from_array() {
        let schema = derive_schema_from_value(&json!([1, 2, 3]));
        assert_eq!(schema["type"], "array");
        assert!(schema.get("items").is_some());
    }

    // =========================================================================
    // infer_type_schema tests
    // =========================================================================

    #[test]
    fn test_infer_type_schema_i64() {
        let schema = infer_type_schema(&json!(i64::MAX));
        assert_eq!(schema["type"], "integer");
    }

    #[test]
    fn test_infer_type_schema_f64() {
        let schema = infer_type_schema(&json!(1.5));
        assert_eq!(schema["type"], "number");
    }

    #[test]
    fn test_infer_type_schema_empty_array() {
        let schema = infer_type_schema(&json!([]));
        assert_eq!(schema["type"], "array");
        assert_eq!(schema["items"], json!({}));
    }
}
