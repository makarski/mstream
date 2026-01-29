use mongodb::bson::{Bson, Document};
use serde_json::{Map as JsonMap, json};
use std::collections::{HashMap, HashSet};

use super::{JSON_SCHEMA_VERSION, JsonSchema};

/// Builds a JSON Schema from a collection of BSON documents
pub fn build_schema(docs: &[Document]) -> JsonSchema {
    if docs.is_empty() {
        return json!({
            "$schema": JSON_SCHEMA_VERSION,
            "type": "object",
            "properties": {}
        });
    }

    let stats = collect_field_stats(docs);
    let mut schema = build_object_schema(&stats, "", docs.len());

    // Add $schema to top-level
    if let Some(obj) = schema.as_object_mut() {
        obj.insert("$schema".to_string(), json!(JSON_SCHEMA_VERSION));
    }

    schema
}

/// Collects statistics about field presence and types
fn collect_field_stats(docs: &[Document]) -> FieldStats {
    let mut stats = FieldStats::new();

    for doc in docs {
        analyze_document(doc, "", &mut stats, 0);
    }

    stats
}

/// Maximum nesting depth to prevent stack overflow from malicious documents
const MAX_DEPTH: usize = 100;

/// Recursively analyzes a document and collects field info
fn analyze_document(doc: &Document, prefix: &str, stats: &mut FieldStats, depth: usize) {
    if depth >= MAX_DEPTH {
        return;
    }

    for (key, value) in doc {
        let field_path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };

        stats.record_field(&field_path);

        let type_name = bson_to_json_type(value);
        stats.record_type(&field_path, type_name);

        match value {
            Bson::Document(nested) => {
                analyze_document(nested, &field_path, stats, depth + 1);
            }
            Bson::Array(arr) => {
                analyze_array_items(arr, &field_path, stats, depth + 1);
            }
            _ => {}
        }
    }
}

/// Analyzes array items to determine item schema
fn analyze_array_items(arr: &[Bson], field_path: &str, stats: &mut FieldStats, depth: usize) {
    if depth >= MAX_DEPTH {
        return;
    }

    let item_path = format!("{}[]", field_path);

    for item in arr {
        let type_name = bson_to_json_type(item);
        stats.record_type(&item_path, type_name);

        if let Bson::Document(doc) = item {
            analyze_document(doc, &item_path, stats, depth + 1);
        }
    }
}

/// Builds a JSON Schema for an object based on field statistics
fn build_object_schema(stats: &FieldStats, prefix: &str, total_docs: usize) -> JsonSchema {
    let mut properties = JsonMap::new();
    let mut required = Vec::new();

    let fields = get_fields_at_level(stats, prefix);

    for field_name in fields {
        let field_path = if prefix.is_empty() {
            field_name.clone()
        } else {
            format!("{}.{}", prefix, field_name)
        };

        let field_schema = build_field_schema(stats, &field_path, total_docs);
        properties.insert(field_name.clone(), field_schema);

        if is_required(stats, &field_path, total_docs) {
            required.push(field_name);
        }
    }

    let mut schema = json!({
        "type": "object",
        "properties": properties
    });

    if !required.is_empty() {
        schema
            .as_object_mut()
            .unwrap()
            .insert("required".to_string(), json!(required));
    }

    schema
}

/// Builds schema for a specific field
fn build_field_schema(stats: &FieldStats, field_path: &str, total_docs: usize) -> JsonSchema {
    let types = stats.get_types(field_path);

    // Find primary type, excluding null
    let primary_type = types
        .iter()
        .filter(|(type_name, _)| *type_name != "null")
        .max_by_key(|(_, count)| *count)
        .map(|(t, _)| t.as_str())
        .unwrap_or("string");

    let is_nullable = types.contains_key("null");

    match primary_type {
        "object" => {
            let obj_schema = build_object_schema(stats, field_path, total_docs);
            if is_nullable {
                // For complex types, wrap in oneOf
                json!({
                    "oneOf": [obj_schema, {"type": "null"}]
                })
            } else {
                obj_schema
            }
        }
        "array" => {
            let arr_schema = build_array_schema(stats, field_path, total_docs);
            if is_nullable {
                json!({
                    "oneOf": [arr_schema, {"type": "null"}]
                })
            } else {
                arr_schema
            }
        }
        _ => {
            if is_nullable && primary_type != "null" {
                json!({
                    "type": [primary_type, "null"]
                })
            } else {
                json!({ "type": primary_type })
            }
        }
    }
}

/// Builds schema for an array field
fn build_array_schema(stats: &FieldStats, field_path: &str, total_docs: usize) -> JsonSchema {
    let item_path = format!("{}[]", field_path);
    let item_types = stats.get_types(&item_path);

    if item_types.is_empty() {
        return json!({ "type": "array" });
    }

    let primary_item_type = item_types
        .iter()
        .max_by_key(|(_, count)| *count)
        .map(|(t, _)| t.as_str())
        .unwrap_or("string");

    let items_schema = if primary_item_type == "object" {
        build_object_schema(stats, &item_path, total_docs)
    } else {
        json!({ "type": primary_item_type })
    };

    json!({
        "type": "array",
        "items": items_schema
    })
}

fn get_fields_at_level(stats: &FieldStats, prefix: &str) -> Vec<String> {
    let prefix_with_dot = if prefix.is_empty() {
        String::new()
    } else {
        format!("{}.", prefix)
    };

    let prefix_len = prefix_with_dot.len();
    let mut fields = HashSet::new();

    let is_field_at_level = |field_path: &str| -> bool {
        let is_nested_under_prefix = field_path.starts_with(&prefix_with_dot);
        let is_top_level_field = prefix.is_empty() && !field_path.contains('.');
        is_nested_under_prefix || is_top_level_field
    };

    for field_path in stats.presence.keys() {
        if is_field_at_level(field_path) {
            let remainder = if prefix.is_empty() {
                field_path.as_str()
            } else {
                &field_path[prefix_len..]
            };

            if let Some(field_name) = remainder.split('.').next() {
                let field_name = field_name.trim_end_matches("[]");
                fields.insert(field_name.to_string());
            }
        }
    }

    fields.into_iter().collect()
}

fn is_required(stats: &FieldStats, field_path: &str, total_docs: usize) -> bool {
    stats
        .presence
        .get(field_path)
        .map_or(false, |&count| count == total_docs)
}

/// Helper struct to track field statistics
struct FieldStats {
    presence: HashMap<String, usize>,
    types: HashMap<String, HashMap<String, usize>>,
}

impl FieldStats {
    fn new() -> Self {
        Self {
            presence: HashMap::new(),
            types: HashMap::new(),
        }
    }

    fn record_field(&mut self, field_path: &str) {
        *self.presence.entry(field_path.to_string()).or_insert(0) += 1;
    }

    fn record_type(&mut self, field_path: &str, type_name: String) {
        self.types
            .entry(field_path.to_string())
            .or_insert_with(HashMap::new)
            .entry(type_name)
            .and_modify(|c| *c += 1)
            .or_insert(1);
    }

    fn get_types(&self, field_path: &str) -> HashMap<String, usize> {
        self.types.get(field_path).cloned().unwrap_or_default()
    }
}

/// Maps BSON type to JSON Schema type
fn bson_to_json_type(value: &Bson) -> String {
    match value {
        Bson::Double(_) => "number",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Boolean(_) => "boolean",
        Bson::Null => "null",
        Bson::Int32(_) | Bson::Int64(_) => "integer",
        Bson::ObjectId(_) | Bson::DateTime(_) | Bson::Timestamp(_) => "string",
        _ => "string",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    #[test]
    fn build_schema_empty_docs() {
        let docs = vec![];
        let schema = build_schema(&docs);

        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"].as_object().unwrap().len(), 0);
    }

    #[test]
    fn build_schema_single_flat_doc() {
        let docs = vec![doc! { "name": "Alice", "age": 30, "active": true }];

        let schema = build_schema(&docs);

        assert_eq!(schema["type"], "object");
        let props = schema["properties"].as_object().unwrap();
        assert_eq!(props["name"]["type"], "string");
        assert_eq!(props["age"]["type"], "integer");
        assert_eq!(props["active"]["type"], "boolean");

        let required = schema["required"].as_array().unwrap();
        assert_eq!(required.len(), 3);
    }

    #[test]
    fn build_schema_optional_fields() {
        let docs = vec![doc! { "name": "Alice", "age": 30 }, doc! { "name": "Bob" }];

        let schema = build_schema(&docs);

        let required = schema["required"].as_array().unwrap();
        assert_eq!(required.len(), 1);
        assert!(required.contains(&json!("name")));
        assert!(!required.contains(&json!("age")));
    }

    #[test]
    fn build_schema_nested_object() {
        let docs = vec![doc! {
            "user": {
                "name": "Charlie",
                "address": {
                    "city": "London"
                }
            }
        }];

        let schema = build_schema(&docs);

        let user = &schema["properties"]["user"];
        assert_eq!(user["type"], "object");

        let address = &user["properties"]["address"];
        assert_eq!(address["type"], "object");
        assert_eq!(address["properties"]["city"]["type"], "string");
    }

    #[test]
    fn build_schema_array_of_strings() {
        let docs = vec![doc! { "tags": ["rust", "mongodb"] }];

        let schema = build_schema(&docs);

        let tags = &schema["properties"]["tags"];
        assert_eq!(tags["type"], "array");
        assert_eq!(tags["items"]["type"], "string");
    }

    #[test]
    fn build_schema_array_of_objects() {
        let docs = vec![doc! {
            "items": [
                { "id": 1, "name": "Item1" },
                { "id": 2, "name": "Item2" }
            ]
        }];

        let schema = build_schema(&docs);

        let items = &schema["properties"]["items"];
        assert_eq!(items["type"], "array");
        assert_eq!(items["items"]["type"], "object");
        assert_eq!(items["items"]["properties"]["id"]["type"], "integer");
        assert_eq!(items["items"]["properties"]["name"]["type"], "string");
    }

    #[test]
    fn build_schema_nullable_field() {
        let docs = vec![
            doc! { "name": "Dave", "email": "dave@example.com" },
            doc! { "name": "Eve", "email": null },
        ];

        let schema = build_schema(&docs);

        let email_type = &schema["properties"]["email"]["type"];
        assert!(email_type.is_array());
        let types = email_type.as_array().unwrap();
        assert!(types.contains(&json!("string")));
        assert!(types.contains(&json!("null")));
    }
}
