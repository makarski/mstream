use mongodb::bson::{Bson, Document};
use regex::Regex;
use serde_json::{Map as JsonMap, json};
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use super::{JSON_SCHEMA_VERSION, JsonSchema};

/// Maximum number of values to collect per field for format/enum detection
const MAX_VALUES_PER_FIELD: usize = 1000;

/// Maximum number of distinct values to consider for enum inference
const MAX_ENUM_CARDINALITY: usize = 10;

/// Minimum number of times a value must appear to be included in enum (diversity check)
const MIN_ENUM_VALUE_OCCURRENCES: usize = 2;

/// Field name patterns that indicate PII - never emit enum for these
const PII_FIELD_PATTERNS: &[&str] = &[
    "email",
    "phone",
    "mobile",
    "name",
    "first_name",
    "last_name",
    "firstname",
    "lastname",
    "full_name",
    "fullname",
    "address",
    "street",
    "ssn",
    "social",
    "passport",
    "license",
    "credit",
    "card",
    "account",
    "password",
    "secret",
    "token",
    "key",
    "ip",
    "user_id",
    "userid",
    "username",
    "login",
];

/// Regex for validating email format
static EMAIL_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap());

/// Regex for validating ISO 8601 datetime format
static ISO_DATETIME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}").unwrap());

/// Regex for validating MongoDB ObjectId format (24 hex characters)
static OBJECTID_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-fA-F0-9]{24}$").unwrap());

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
        stats.record_value(&field_path, value.clone());

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
        stats.record_value(&item_path, item.clone());

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
        "string" => build_string_schema(stats, field_path, is_nullable),
        "integer" | "number" => build_numeric_schema(stats, field_path, primary_type, is_nullable),
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

/// Builds schema for a string field with format and enum detection
fn build_string_schema(stats: &FieldStats, field_path: &str, is_nullable: bool) -> JsonSchema {
    let values = stats.get_values(field_path);

    // Detect format
    let format = detect_string_format(&values, field_path);

    // Detect enum only if:
    // 1. No special format detected (format fields shouldn't leak actual values)
    // 2. Field name doesn't indicate PII
    let enum_values = if format.is_none() && !is_pii_field(field_path) {
        detect_enum_values(&values)
    } else {
        None
    };

    let mut schema = JsonMap::new();

    if is_nullable {
        schema.insert("type".to_string(), json!(["string", "null"]));
    } else {
        schema.insert("type".to_string(), json!("string"));
    }

    if let Some(fmt) = format {
        schema.insert("format".to_string(), json!(fmt));
    }

    if let Some(enums) = enum_values {
        let mut enum_list: Vec<serde_json::Value> = enums.into_iter().map(|s| json!(s)).collect();
        if is_nullable {
            enum_list.push(json!(null));
        }
        schema.insert("enum".to_string(), json!(enum_list));
    }

    serde_json::Value::Object(schema)
}

/// Builds schema for a numeric field with min/max detection
fn build_numeric_schema(
    stats: &FieldStats,
    field_path: &str,
    primary_type: &str,
    is_nullable: bool,
) -> JsonSchema {
    let values = stats.get_values(field_path);

    // Detect min/max
    let (minimum, maximum) = detect_numeric_range(&values);

    let mut schema = JsonMap::new();

    if is_nullable {
        schema.insert("type".to_string(), json!([primary_type, "null"]));
    } else {
        schema.insert("type".to_string(), json!(primary_type));
    }

    if let Some(min) = minimum {
        schema.insert("minimum".to_string(), json!(min));
    }

    if let Some(max) = maximum {
        schema.insert("maximum".to_string(), json!(max));
    }

    serde_json::Value::Object(schema)
}

/// Detects string format based on field name and values
fn detect_string_format(values: &[Bson], field_path: &str) -> Option<&'static str> {
    if values.is_empty() {
        return None;
    }

    if let Some(format) = try_detect_objectid_format(values, field_path) {
        return Some(format);
    }

    if is_all_datetime(values) {
        return Some("date-time");
    }

    if is_all_email(values) {
        return Some("email");
    }

    None
}

fn try_detect_objectid_format(values: &[Bson], field_path: &str) -> Option<&'static str> {
    let field_name = field_path.split('.').last().unwrap_or(field_path);
    let is_id_field = field_name == "_id" || field_name.ends_with("_id");

    if !is_id_field {
        return None;
    }

    let all_objectid = values.iter().all(|v| is_objectid_value(v));
    if all_objectid { Some("objectid") } else { None }
}

fn is_objectid_value(v: &Bson) -> bool {
    match v {
        Bson::ObjectId(_) => true,
        Bson::String(s) => OBJECTID_REGEX.is_match(s),
        _ => false,
    }
}

fn is_all_datetime(values: &[Bson]) -> bool {
    let non_null: Vec<_> = values.iter().filter(|v| !matches!(v, Bson::Null)).collect();

    if non_null.is_empty() {
        return false;
    }

    non_null.iter().all(|v| is_datetime_value(v))
}

fn is_datetime_value(v: &Bson) -> bool {
    match v {
        Bson::DateTime(_) => true,
        Bson::String(s) => ISO_DATETIME_REGEX.is_match(s),
        _ => false,
    }
}

fn is_all_email(values: &[Bson]) -> bool {
    let string_values: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();
    !string_values.is_empty() && string_values.iter().all(|s| EMAIL_REGEX.is_match(s))
}

/// Detects if a field should be an enum based on low cardinality
fn detect_enum_values(values: &[Bson]) -> Option<Vec<String>> {
    if values.is_empty() {
        return None;
    }

    let value_counts = count_string_values(values);

    if !is_valid_enum_cardinality(&value_counts) {
        return None;
    }

    let frequent_values = filter_frequent_values(value_counts);

    if frequent_values.len() < 2 {
        return None;
    }

    if contains_id_like_values(&frequent_values) {
        return None;
    }

    let mut sorted = frequent_values;
    sorted.sort();
    Some(sorted)
}

fn count_string_values(values: &[Bson]) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for v in values {
        if let Some(s) = v.as_str() {
            *counts.entry(s.to_string()).or_insert(0) += 1;
        }
    }
    counts
}

fn is_valid_enum_cardinality(counts: &HashMap<String, usize>) -> bool {
    !counts.is_empty() && counts.len() <= MAX_ENUM_CARDINALITY
}

fn filter_frequent_values(counts: HashMap<String, usize>) -> Vec<String> {
    counts
        .into_iter()
        .filter(|(_, count)| *count >= MIN_ENUM_VALUE_OCCURRENCES)
        .map(|(value, _)| value)
        .collect()
}

fn contains_id_like_values(values: &[String]) -> bool {
    values.iter().any(|s| is_id_like_string(s))
}

fn is_id_like_string(s: &str) -> bool {
    OBJECTID_REGEX.is_match(s) || s.len() > 50 || is_likely_hex_id(s)
}

/// Checks if a string looks like a hex-encoded ID (common lengths: 24, 32, 36, 40)
fn is_likely_hex_id(s: &str) -> bool {
    // Common ID lengths: MongoDB ObjectId (24), UUID hex (32), UUID with dashes (36), SHA1 (40)
    let id_lengths = [24, 32, 36, 40];
    id_lengths.contains(&s.len()) && s.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}

/// Checks if a field path indicates PII based on field name patterns
fn is_pii_field(field_path: &str) -> bool {
    let field_name = field_path
        .split('.')
        .last()
        .unwrap_or(field_path)
        .to_lowercase();

    // Remove array notation if present
    let field_name = field_name.trim_end_matches("[]");

    for pattern in PII_FIELD_PATTERNS {
        if field_name.contains(pattern) {
            return true;
        }
    }

    false
}

/// Detects minimum and maximum values for numeric fields
fn detect_numeric_range(values: &[Bson]) -> (Option<f64>, Option<f64>) {
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;

    for value in values {
        let num = match value {
            Bson::Int32(n) => Some(*n as f64),
            Bson::Int64(n) => Some(*n as f64),
            Bson::Double(n) => Some(*n),
            _ => None,
        };

        if let Some(n) = num {
            min = Some(min.map_or(n, |m: f64| m.min(n)));
            max = Some(max.map_or(n, |m: f64| m.max(n)));
        }
    }

    (min, max)
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

    let items_schema = match primary_item_type {
        "object" => build_object_schema(stats, &item_path, total_docs),
        "string" => build_string_schema(stats, &item_path, false),
        "integer" | "number" => build_numeric_schema(stats, &item_path, primary_item_type, false),
        _ => json!({ "type": primary_item_type }),
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
    values: HashMap<String, Vec<Bson>>,
}

impl FieldStats {
    fn new() -> Self {
        Self {
            presence: HashMap::new(),
            types: HashMap::new(),
            values: HashMap::new(),
        }
    }

    fn record_field(&mut self, field_path: &str) {
        *self.presence.entry(field_path.to_string()).or_insert(0) += 1;
    }

    fn record_type(&mut self, field_path: &str, type_name: String) {
        self.types
            .entry(field_path.to_string())
            .or_default()
            .entry(type_name)
            .and_modify(|c| *c += 1)
            .or_insert(1);
    }

    fn record_value(&mut self, field_path: &str, value: Bson) {
        let values = self.values.entry(field_path.to_string()).or_default();
        if values.len() < MAX_VALUES_PER_FIELD {
            values.push(value);
        }
    }

    fn get_types(&self, field_path: &str) -> HashMap<String, usize> {
        self.types.get(field_path).cloned().unwrap_or_default()
    }

    fn get_values(&self, field_path: &str) -> Vec<Bson> {
        self.values.get(field_path).cloned().unwrap_or_default()
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
    use mongodb::bson::{DateTime, doc, oid::ObjectId};

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

    #[test]
    fn build_schema_detects_email_format() {
        let docs = vec![
            doc! { "email": "alice@example.com" },
            doc! { "email": "bob@test.org" },
        ];

        let schema = build_schema(&docs);

        assert_eq!(schema["properties"]["email"]["type"], "string");
        assert_eq!(schema["properties"]["email"]["format"], "email");
    }

    #[test]
    fn build_schema_detects_datetime_format_from_bson() {
        let docs = vec![
            doc! { "created_at": DateTime::now() },
            doc! { "created_at": DateTime::now() },
        ];

        let schema = build_schema(&docs);

        assert_eq!(schema["properties"]["created_at"]["type"], "string");
        assert_eq!(schema["properties"]["created_at"]["format"], "date-time");
    }

    #[test]
    fn build_schema_detects_datetime_format_from_string() {
        let docs = vec![
            doc! { "created_at": "2024-01-15T10:30:00Z" },
            doc! { "created_at": "2024-02-20T14:45:30.123+0000" },
        ];

        let schema = build_schema(&docs);

        assert_eq!(schema["properties"]["created_at"]["type"], "string");
        assert_eq!(schema["properties"]["created_at"]["format"], "date-time");
    }

    #[test]
    fn build_schema_detects_objectid_format() {
        let docs = vec![
            doc! { "_id": ObjectId::new() },
            doc! { "_id": ObjectId::new() },
        ];

        let schema = build_schema(&docs);

        assert_eq!(schema["properties"]["_id"]["type"], "string");
        assert_eq!(schema["properties"]["_id"]["format"], "objectid");
    }

    #[test]
    fn build_schema_detects_objectid_format_from_string() {
        let docs = vec![
            doc! { "_id": "507f1f77bcf86cd799439011" },
            doc! { "_id": "507f1f77bcf86cd799439012" },
        ];

        let schema = build_schema(&docs);

        assert_eq!(schema["properties"]["_id"]["type"], "string");
        assert_eq!(schema["properties"]["_id"]["format"], "objectid");
    }

    /// Helper to build schema and extract a field's schema
    fn get_field_schema(docs: Vec<Document>, field: &str) -> serde_json::Value {
        let schema = build_schema(&docs);
        schema["properties"][field].clone()
    }

    /// Helper to assert a field has the expected type and min/max range
    fn assert_field_range(field: &serde_json::Value, expected_type: &str, min: f64, max: f64) {
        assert_eq!(field["type"], expected_type);
        assert_eq!(field["minimum"], min);
        assert_eq!(field["maximum"], max);
    }

    /// Helper to assert enum contains expected values
    fn assert_enum_contains(field: &serde_json::Value, expected: &[serde_json::Value]) {
        let enum_values = field["enum"].as_array().unwrap();
        for val in expected {
            assert!(enum_values.contains(val), "enum should contain {:?}", val);
        }
    }

    #[test]
    fn build_schema_detects_enum_low_cardinality() {
        // Each value must appear at least MIN_ENUM_VALUE_OCCURRENCES times
        let docs = vec![
            doc! { "status": "active" },
            doc! { "status": "active" },
            doc! { "status": "pending" },
            doc! { "status": "pending" },
            doc! { "status": "inactive" },
            doc! { "status": "inactive" },
        ];

        let field = get_field_schema(docs, "status");

        assert_eq!(field["type"], "string");
        assert_enum_contains(
            &field,
            &[json!("active"), json!("pending"), json!("inactive")],
        );
    }

    #[test]
    fn build_schema_no_enum_for_high_cardinality() {
        let docs: Vec<Document> = (0..20)
            .map(|i| doc! { "name": format!("User{}", i) })
            .collect();

        let schema = build_schema(&docs);

        assert_eq!(schema["properties"]["name"]["type"], "string");
        assert!(schema["properties"]["name"].get("enum").is_none());
    }

    #[test]
    fn build_schema_detects_numeric_range() {
        let docs = vec![
            doc! { "age": 25 },
            doc! { "age": 30 },
            doc! { "age": 18 },
            doc! { "age": 65 },
        ];

        let field = get_field_schema(docs, "age");

        assert_field_range(&field, "integer", 18.0, 65.0);
    }

    #[test]
    fn build_schema_detects_float_range() {
        let docs = vec![
            doc! { "gpa": 2.5 },
            doc! { "gpa": 3.8 },
            doc! { "gpa": 1.9 },
        ];

        let field = get_field_schema(docs, "gpa");

        assert_field_range(&field, "number", 1.9, 3.8);
    }

    #[test]
    fn build_schema_nullable_enum() {
        // Each value must appear at least MIN_ENUM_VALUE_OCCURRENCES times
        let docs = vec![
            doc! { "status": "active" },
            doc! { "status": "active" },
            doc! { "status": null },
            doc! { "status": "inactive" },
            doc! { "status": "inactive" },
        ];

        let field = get_field_schema(docs, "status");

        assert_enum_contains(&field, &[json!("active"), json!("inactive"), json!(null)]);
    }

    #[test]
    fn build_schema_array_items_with_enum() {
        // Each value must appear at least MIN_ENUM_VALUE_OCCURRENCES times
        let docs = vec![
            doc! { "tags": ["admin", "user"] },
            doc! { "tags": ["guest", "admin"] },
            doc! { "tags": ["user", "guest"] },
        ];

        let schema = build_schema(&docs);

        let items = &schema["properties"]["tags"]["items"];
        assert_eq!(items["type"], "string");
        let enum_values = items["enum"].as_array().unwrap();
        assert!(enum_values.contains(&json!("admin")));
        assert!(enum_values.contains(&json!("user")));
        assert!(enum_values.contains(&json!("guest")));
    }

    #[test]
    fn build_schema_no_enum_for_objectid_strings() {
        let docs = vec![
            doc! { "ref_id": "507f1f77bcf86cd799439011" },
            doc! { "ref_id": "507f1f77bcf86cd799439012" },
        ];

        let schema = build_schema(&docs);

        // Should detect as objectid format, not enum
        assert!(schema["properties"]["ref_id"].get("enum").is_none());
    }

    #[test]
    fn build_schema_no_enum_for_pii_fields() {
        // Even with low cardinality, PII fields should not get enums
        let docs = vec![
            doc! { "email": "alice@example.com" },
            doc! { "email": "alice@example.com" },
            doc! { "email": "bob@example.com" },
            doc! { "email": "bob@example.com" },
        ];

        let schema = build_schema(&docs);

        // Should have email format but NO enum (PII protection)
        assert_eq!(schema["properties"]["email"]["format"], "email");
        assert!(schema["properties"]["email"].get("enum").is_none());
    }

    #[test]
    fn build_schema_no_enum_for_pii_name_fields() {
        let docs = vec![
            doc! { "first_name": "Alice" },
            doc! { "first_name": "Alice" },
            doc! { "first_name": "Bob" },
            doc! { "first_name": "Bob" },
        ];

        let schema = build_schema(&docs);

        // Should NOT have enum (PII field name)
        assert!(schema["properties"]["first_name"].get("enum").is_none());
    }

    #[test]
    fn build_schema_no_enum_for_rare_values() {
        // Values that only appear once should not be in enum
        let docs = vec![
            doc! { "category": "A" },
            doc! { "category": "A" },
            doc! { "category": "B" },
            doc! { "category": "B" },
            doc! { "category": "rare_value" }, // Only appears once - should be excluded
        ];

        let schema = build_schema(&docs);

        // Enum should exist with only the frequent values
        if let Some(enum_values) = schema["properties"]["category"]["enum"].as_array() {
            assert!(enum_values.contains(&json!("A")));
            assert!(enum_values.contains(&json!("B")));
            assert!(
                !enum_values.contains(&json!("rare_value")),
                "Rare value should not be in enum"
            );
        } else {
            // It's also acceptable to have no enum if implementation is more conservative
        }
    }

    #[test]
    fn build_schema_no_enum_when_all_values_rare() {
        // If all values are unique, no enum should be created
        let docs = vec![
            doc! { "code": "ABC123" },
            doc! { "code": "DEF456" },
            doc! { "code": "GHI789" },
        ];

        let schema = build_schema(&docs);

        assert!(schema["properties"]["code"].get("enum").is_none());
    }
}
