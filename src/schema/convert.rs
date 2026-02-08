//! Schema conversion between JSON Schema and Avro.
//!
//! Supports bidirectional conversion with the following type mappings:
//!
//! | JSON Schema                      | Avro                                    |
//! |----------------------------------|-----------------------------------------|
//! | `"string"`                       | `"string"`                              |
//! | `"string"` + `format: "email"`   | `"string"`                              |
//! | `"string"` + `format: "date-time"` | `{"type": "long", "logicalType": "timestamp-millis"}` |
//! | `"string"` + `format: "objectid"` | `{"type": "string", "logicalType": "objectid"}` |
//! | `"integer"`                      | `"int"` or `"long"` (based on x-range)  |
//! | `"number"`                       | `"double"`                              |
//! | `"boolean"`                      | `"boolean"`                             |
//! | `"null"`                         | `"null"`                                |
//! | `["string", "null"]`             | `["null", "string"]` (union)            |
//! | `"object"` + `properties`        | nested `record`                         |
//! | `"array"` + `items`              | `{"type": "array", "items": ...}`       |
//! | `x-enum: [...]`                  | `{"type": "enum", "symbols": [...]}`    |

use anyhow::{Context, Result, anyhow, bail};
use apache_avro::Schema as AvroSchema;
use serde_json::{Map, Value as JsonValue, json};

use crate::encoding::json_schema::JSON_SCHEMA_VERSION;

/// Context for JSON Schema to Avro conversion, reducing argument count.
struct ConversionContext<'a> {
    namespace: Option<&'a str>,
    counter: &'a mut u32,
}

/// Converts a JSON Schema to an Avro schema.
pub fn json_schema_to_avro(
    json_schema: &JsonValue,
    options: &Option<std::collections::HashMap<String, String>>,
) -> Result<String> {
    let name = options
        .as_ref()
        .and_then(|o| o.get("name"))
        .map(|s| s.as_str())
        .unwrap_or("Record");

    let namespace = options
        .as_ref()
        .and_then(|o| o.get("namespace"))
        .map(|s| s.as_str());

    let mut counter = 0;
    let mut ctx = ConversionContext {
        namespace,
        counter: &mut counter,
    };

    let avro_value = convert_json_schema_to_avro(json_schema, name, &mut ctx)?;

    let avro_str = serde_json::to_string_pretty(&avro_value)?;
    AvroSchema::parse_str(&avro_str).context("generated invalid Avro schema")?;

    Ok(avro_str)
}

/// Converts an Avro schema to a JSON Schema.
pub fn avro_to_json_schema(avro_schema: &AvroSchema) -> Result<String> {
    let json_schema = convert_avro_to_json_schema(avro_schema)?;

    let mut schema_with_meta = Map::new();
    schema_with_meta.insert("$schema".to_string(), json!(JSON_SCHEMA_VERSION));

    if let JsonValue::Object(obj) = json_schema {
        for (k, v) in obj {
            schema_with_meta.insert(k, v);
        }
    }

    Ok(serde_json::to_string_pretty(&JsonValue::Object(
        schema_with_meta,
    ))?)
}

// =============================================================================
// JSON Schema -> Avro conversion
// =============================================================================

fn convert_json_schema_to_avro(
    schema: &JsonValue,
    name: &str,
    ctx: &mut ConversionContext,
) -> Result<JsonValue> {
    let schema_type = schema.get("type").and_then(|t| t.as_str());

    match schema_type {
        Some("object") => convert_object_to_avro(schema, name, ctx),
        Some("array") => convert_array_to_avro(schema, ctx),
        Some("string") => convert_string_to_avro(schema, name, ctx),
        Some("integer") => Ok(convert_integer_to_avro(schema)),
        Some("number") => Ok(json!("double")),
        Some("boolean") => Ok(json!("boolean")),
        Some("null") => Ok(json!("null")),
        None => convert_complex_type(schema, name, ctx),
        Some(other) => bail!("unsupported JSON Schema type: {}", other),
    }
}

fn convert_complex_type(
    schema: &JsonValue,
    name: &str,
    ctx: &mut ConversionContext,
) -> Result<JsonValue> {
    // Check for nullable type: {"type": ["string", "null"]}
    if let Some(types) = schema.get("type").and_then(|t| t.as_array()) {
        return convert_union_to_avro(types, schema, name, ctx);
    }

    // Handle oneOf/anyOf as union
    if schema.get("oneOf").is_some() || schema.get("anyOf").is_some() {
        return convert_oneof_to_avro(schema, name, ctx);
    }

    bail!("unsupported JSON Schema type: {:?}", schema)
}

fn convert_object_to_avro(
    schema: &JsonValue,
    name: &str,
    ctx: &mut ConversionContext,
) -> Result<JsonValue> {
    let properties = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .cloned()
        .unwrap_or_default();

    let required = collect_required_fields(schema);
    let fields = build_avro_fields(&properties, &required, ctx)?;

    let mut record = Map::new();
    record.insert("type".to_string(), json!("record"));
    record.insert("name".to_string(), json!(name));
    if let Some(ns) = ctx.namespace {
        record.insert("namespace".to_string(), json!(ns));
    }
    record.insert("fields".to_string(), JsonValue::Array(fields));

    Ok(JsonValue::Object(record))
}

fn collect_required_fields(schema: &JsonValue) -> Vec<String> {
    schema
        .get("required")
        .and_then(|r| r.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn build_avro_fields(
    properties: &Map<String, JsonValue>,
    required: &[String],
    ctx: &mut ConversionContext,
) -> Result<Vec<JsonValue>> {
    let mut fields = Vec::new();

    for (field_name, field_schema) in properties {
        let nested_name = capitalize(field_name);
        let field_avro = convert_json_schema_to_avro(field_schema, &nested_name, ctx)?;
        let is_required = required.contains(field_name);

        let field = build_single_field(field_name, field_avro, is_required);
        fields.push(field);
    }

    Ok(fields)
}

fn build_single_field(name: &str, avro_type: JsonValue, is_required: bool) -> JsonValue {
    let mut field = Map::new();
    field.insert("name".to_string(), json!(name));

    if is_required {
        field.insert("type".to_string(), avro_type);
    } else {
        field.insert("type".to_string(), make_nullable(avro_type));
        field.insert("default".to_string(), json!(null));
    }

    JsonValue::Object(field)
}

fn convert_array_to_avro(schema: &JsonValue, ctx: &mut ConversionContext) -> Result<JsonValue> {
    let items = schema
        .get("items")
        .ok_or_else(|| anyhow!("array schema missing 'items'"))?;

    *ctx.counter += 1;
    let item_name = format!("ArrayItem{}", ctx.counter);
    let items_avro = convert_json_schema_to_avro(items, &item_name, ctx)?;

    Ok(json!({
        "type": "array",
        "items": items_avro
    }))
}

fn convert_string_to_avro(
    schema: &JsonValue,
    name: &str,
    ctx: &mut ConversionContext,
) -> Result<JsonValue> {
    // Check for enum values (x-enum or standard enum)
    if let Some(avro_enum) = try_build_enum(schema, name, ctx) {
        return Ok(avro_enum);
    }

    // Check for format hints
    Ok(convert_string_format(schema))
}

fn try_build_enum(
    schema: &JsonValue,
    name: &str,
    ctx: &mut ConversionContext,
) -> Option<JsonValue> {
    let enum_values = schema
        .get("x-enum")
        .or_else(|| schema.get("enum"))
        .and_then(|e| e.as_array())?;

    let symbols: Vec<String> = enum_values
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();

    if symbols.is_empty() {
        return None;
    }

    *ctx.counter += 1;
    Some(json!({
        "type": "enum",
        "name": format!("{}Enum{}", name, ctx.counter),
        "symbols": symbols
    }))
}

fn convert_string_format(schema: &JsonValue) -> JsonValue {
    match schema.get("format").and_then(|f| f.as_str()) {
        Some("date-time") => json!({"type": "long", "logicalType": "timestamp-millis"}),
        Some("objectid") => json!({"type": "string", "logicalType": "objectid"}),
        _ => json!("string"),
    }
}

fn convert_integer_to_avro(schema: &JsonValue) -> JsonValue {
    if fits_in_i32(schema) {
        json!("int")
    } else {
        json!("long")
    }
}

fn fits_in_i32(schema: &JsonValue) -> bool {
    let i32_min = i32::MIN as i64;
    let i32_max = i32::MAX as i64;

    // Check x-range extension
    if let Some(range) = schema.get("x-range").and_then(|r| r.as_object()) {
        let min = range
            .get("min")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MIN);
        let max = range
            .get("max")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MAX);
        return min >= i32_min && max <= i32_max;
    }

    // Check standard minimum/maximum
    let min = schema.get("minimum").and_then(|v| v.as_i64());
    let max = schema.get("maximum").and_then(|v| v.as_i64());

    matches!((min, max), (Some(min_val), Some(max_val)) if min_val >= i32_min && max_val <= i32_max)
}

fn convert_union_to_avro(
    types: &[JsonValue],
    schema: &JsonValue,
    name: &str,
    ctx: &mut ConversionContext,
) -> Result<JsonValue> {
    let (has_null, non_null_types) = partition_union_types(types);
    let mut avro_types = convert_non_null_types(&non_null_types, schema, name, ctx)?;

    if has_null {
        avro_types.insert(0, json!("null"));
    }

    Ok(JsonValue::Array(avro_types))
}

fn partition_union_types(types: &[JsonValue]) -> (bool, Vec<&str>) {
    let mut has_null = false;
    let mut non_null = Vec::new();

    for t in types {
        if let Some(type_str) = t.as_str() {
            if type_str == "null" {
                has_null = true;
            } else {
                non_null.push(type_str);
            }
        }
    }

    (has_null, non_null)
}

fn convert_non_null_types(
    types: &[&str],
    schema: &JsonValue,
    name: &str,
    ctx: &mut ConversionContext,
) -> Result<Vec<JsonValue>> {
    let mut result = Vec::new();

    for type_str in types {
        let mini_schema = build_mini_schema(type_str, schema);
        let converted = convert_json_schema_to_avro(&mini_schema, name, ctx)?;
        result.push(converted);
    }

    Ok(result)
}

fn build_mini_schema(type_str: &str, parent: &JsonValue) -> JsonValue {
    let mut mini = Map::new();
    mini.insert("type".to_string(), json!(type_str));

    for key in ["format", "enum", "x-enum", "x-range", "minimum", "maximum"] {
        if let Some(val) = parent.get(key) {
            mini.insert(key.to_string(), val.clone());
        }
    }

    JsonValue::Object(mini)
}

fn convert_oneof_to_avro(
    schema: &JsonValue,
    name: &str,
    ctx: &mut ConversionContext,
) -> Result<JsonValue> {
    let variants = schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("expected oneOf/anyOf array"))?;

    let mut avro_types = Vec::new();
    let mut has_null = false;

    for variant in variants {
        let is_null = variant.get("type").and_then(|t| t.as_str()) == Some("null");
        if is_null {
            has_null = true;
        } else {
            *ctx.counter += 1;
            let variant_name = format!("{}Variant{}", name, ctx.counter);
            let converted = convert_json_schema_to_avro(variant, &variant_name, ctx)?;
            avro_types.push(converted);
        }
    }

    if has_null {
        avro_types.insert(0, json!("null"));
    }

    Ok(JsonValue::Array(avro_types))
}

fn make_nullable(avro_type: JsonValue) -> JsonValue {
    if let JsonValue::Array(arr) = &avro_type {
        if arr.iter().any(|v| v == &json!("null")) {
            return avro_type;
        }
        let mut new_arr = vec![json!("null")];
        new_arr.extend(arr.clone());
        return JsonValue::Array(new_arr);
    }

    json!(["null", avro_type])
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().chain(chars).collect(),
    }
}

// =============================================================================
// Avro -> JSON Schema conversion
// =============================================================================

fn convert_avro_to_json_schema(schema: &AvroSchema) -> Result<JsonValue> {
    match schema {
        AvroSchema::Null => Ok(json!({"type": "null"})),
        AvroSchema::Boolean => Ok(json!({"type": "boolean"})),
        AvroSchema::Int => Ok(json!({"type": "integer"})),
        AvroSchema::Long => Ok(json!({"type": "integer"})),
        AvroSchema::Float => Ok(json!({"type": "number"})),
        AvroSchema::Double => Ok(json!({"type": "number"})),
        AvroSchema::Bytes => Ok(json!({"type": "string", "contentEncoding": "base64"})),
        AvroSchema::String => Ok(json!({"type": "string"})),
        AvroSchema::Array(inner) => convert_avro_array(inner),
        AvroSchema::Map(inner) => convert_avro_map(inner),
        AvroSchema::Union(union_schema) => convert_avro_union(union_schema.variants()),
        AvroSchema::Record { fields, .. } => convert_avro_record(fields),
        AvroSchema::Enum { symbols, .. } => Ok(json!({"type": "string", "enum": symbols})),
        AvroSchema::Fixed { size, .. } => {
            Ok(json!({"type": "string", "minLength": size, "maxLength": size}))
        }
        _ => convert_avro_logical_type(schema),
    }
}

fn convert_avro_array(inner: &AvroSchema) -> Result<JsonValue> {
    let items = convert_avro_to_json_schema(inner)?;
    Ok(json!({"type": "array", "items": items}))
}

fn convert_avro_map(inner: &AvroSchema) -> Result<JsonValue> {
    let values = convert_avro_to_json_schema(inner)?;
    Ok(json!({"type": "object", "additionalProperties": values}))
}

fn convert_avro_logical_type(schema: &AvroSchema) -> Result<JsonValue> {
    match schema {
        AvroSchema::TimestampMillis | AvroSchema::TimestampMicros => {
            Ok(json!({"type": "string", "format": "date-time"}))
        }
        AvroSchema::Date => Ok(json!({"type": "string", "format": "date"})),
        AvroSchema::TimeMillis | AvroSchema::TimeMicros => {
            Ok(json!({"type": "string", "format": "time"}))
        }
        AvroSchema::Uuid => Ok(json!({"type": "string", "format": "uuid"})),
        AvroSchema::Decimal {
            precision, scale, ..
        } => Ok(json!({
            "type": "number",
            "x-avro-decimal": {"precision": precision, "scale": scale}
        })),
        AvroSchema::Duration => Ok(json!({"type": "string", "format": "duration"})),
        AvroSchema::Ref { name } => Ok(json!({"$ref": format!("#/$defs/{}", name.fullname(None))})),
        // Fallback for any types not explicitly handled
        _ => Ok(json!({"type": "string"})),
    }
}

fn convert_avro_union(variants: &[AvroSchema]) -> Result<JsonValue> {
    if let Some(nullable) = try_convert_nullable_union(variants)? {
        return Ok(nullable);
    }

    // General union: use oneOf
    let schemas: Result<Vec<_>> = variants.iter().map(convert_avro_to_json_schema).collect();
    Ok(json!({"oneOf": schemas?}))
}

fn try_convert_nullable_union(variants: &[AvroSchema]) -> Result<Option<JsonValue>> {
    if variants.len() != 2 {
        return Ok(None);
    }

    let has_null = variants.iter().any(|v| matches!(v, AvroSchema::Null));
    if !has_null {
        return Ok(None);
    }

    let non_null = variants.iter().find(|v| !matches!(v, AvroSchema::Null));
    let inner = match non_null {
        Some(schema) => schema,
        None => return Ok(None),
    };

    let inner_schema = convert_avro_to_json_schema(inner)?;

    // Express as type array if possible: ["string", "null"]
    if let Some(type_str) = inner_schema.get("type").and_then(|t| t.as_str()) {
        let mut result = inner_schema.as_object().cloned().unwrap_or_default();
        result.insert("type".to_string(), json!([type_str, "null"]));
        return Ok(Some(JsonValue::Object(result)));
    }

    // For complex types, use oneOf
    Ok(Some(json!({"oneOf": [inner_schema, {"type": "null"}]})))
}

fn convert_avro_record(fields: &[apache_avro::schema::RecordField]) -> Result<JsonValue> {
    let mut properties = Map::new();
    let mut required = Vec::new();

    for field in fields {
        let field_schema = convert_avro_to_json_schema(&field.schema)?;
        properties.insert(field.name.clone(), field_schema);

        if is_required_avro_field(field) {
            required.push(json!(field.name));
        }
    }

    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), JsonValue::Object(properties));

    if !required.is_empty() {
        schema.insert("required".to_string(), JsonValue::Array(required));
    }

    Ok(JsonValue::Object(schema))
}

fn is_required_avro_field(field: &apache_avro::schema::RecordField) -> bool {
    if field.default.is_some() {
        return false;
    }

    !matches!(&field.schema, AvroSchema::Union(u)
        if u.variants().iter().any(|v| matches!(v, AvroSchema::Null)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    // =========================================================================
    // Test helpers
    // =========================================================================

    fn parse_avro_and_convert(avro_str: &str) -> JsonValue {
        let avro = AvroSchema::parse_str(avro_str).unwrap();
        let result = avro_to_json_schema(&avro).unwrap();
        serde_json::from_str(&result).unwrap()
    }

    fn convert_with_options(schema: JsonValue, name: &str) -> JsonValue {
        let options = Some(HashMap::from([(String::from("name"), String::from(name))]));
        let result = json_schema_to_avro(&schema, &options).unwrap();
        serde_json::from_str(&result).unwrap()
    }

    fn convert_default(schema: JsonValue) -> JsonValue {
        let result = json_schema_to_avro(&schema, &None).unwrap();
        serde_json::from_str(&result).unwrap()
    }

    // =========================================================================
    // JSON Schema -> Avro tests
    // =========================================================================

    #[test]
    fn json_to_avro_simple_types() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "score": {"type": "number"},
                "active": {"type": "boolean"}
            },
            "required": ["name"]
        });

        let avro = convert_with_options(schema, "Person");

        assert_eq!(avro["type"], "record");
        assert_eq!(avro["name"], "Person");

        let fields = avro["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 4);

        let name_field = fields.iter().find(|f| f["name"] == "name").unwrap();
        assert_eq!(name_field["type"], "string");

        let age_field = fields.iter().find(|f| f["name"] == "age").unwrap();
        assert!(age_field["type"].is_array());
    }

    #[test]
    fn json_to_avro_nested_object() {
        let schema = json!({
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "email": {"type": "string"}
                    }
                }
            }
        });

        let avro = convert_default(schema);
        let fields = avro["fields"].as_array().unwrap();
        let user_field = fields.iter().find(|f| f["name"] == "user").unwrap();

        assert!(user_field["type"].is_array());
    }

    #[test]
    fn json_to_avro_array() {
        let schema = json!({
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        });

        let avro = convert_default(schema);
        let fields = avro["fields"].as_array().unwrap();
        let tags_field = fields.iter().find(|f| f["name"] == "tags").unwrap();

        let tags_type = &tags_field["type"];
        assert!(tags_type.is_array());
        let array_type = tags_type
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t.is_object())
            .unwrap();
        assert_eq!(array_type["type"], "array");
        assert_eq!(array_type["items"], "string");
    }

    #[test]
    fn json_to_avro_enum() {
        let schema = json!({
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["active", "pending", "inactive"]
                }
            }
        });

        let avro = convert_default(schema);
        let fields = avro["fields"].as_array().unwrap();
        let status_field = fields.iter().find(|f| f["name"] == "status").unwrap();

        let enum_type = status_field["type"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t.is_object() && t["type"] == "enum")
            .unwrap();

        assert_eq!(enum_type["type"], "enum");
        let symbols = enum_type["symbols"].as_array().unwrap();
        assert!(symbols.contains(&json!("active")));
        assert!(symbols.contains(&json!("pending")));
        assert!(symbols.contains(&json!("inactive")));
    }

    #[test]
    fn json_to_avro_x_enum() {
        let schema = json!({
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "x-enum": ["ACTIVE", "INACTIVE"]
                }
            }
        });

        let avro = convert_default(schema);
        let fields = avro["fields"].as_array().unwrap();
        let status_field = fields.iter().find(|f| f["name"] == "status").unwrap();
        let enum_type = status_field["type"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t.is_object() && t["type"] == "enum")
            .unwrap();

        assert_eq!(enum_type["symbols"], json!(["ACTIVE", "INACTIVE"]));
    }

    #[test]
    fn json_to_avro_datetime_format() {
        let schema = json!({
            "type": "object",
            "properties": {
                "created_at": {
                    "type": "string",
                    "format": "date-time"
                }
            },
            "required": ["created_at"]
        });

        let avro = convert_default(schema);
        let fields = avro["fields"].as_array().unwrap();
        let created_field = fields.iter().find(|f| f["name"] == "created_at").unwrap();

        assert_eq!(created_field["type"]["type"], "long");
        assert_eq!(created_field["type"]["logicalType"], "timestamp-millis");
    }

    #[test]
    fn json_to_avro_nullable_type() {
        let schema = json!({
            "type": "object",
            "properties": {
                "nickname": {
                    "type": ["string", "null"]
                }
            }
        });

        let avro = convert_default(schema);
        let fields = avro["fields"].as_array().unwrap();
        let nickname_field = fields.iter().find(|f| f["name"] == "nickname").unwrap();

        let types = nickname_field["type"].as_array().unwrap();
        assert_eq!(types[0], "null");
    }

    #[test]
    fn json_to_avro_with_namespace() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": {"type": "string"}
            }
        });

        let options = Some(HashMap::from([
            (String::from("name"), String::from("User")),
            (String::from("namespace"), String::from("com.example")),
        ]));

        let result = json_schema_to_avro(&schema, &options).unwrap();
        let avro: JsonValue = serde_json::from_str(&result).unwrap();

        assert_eq!(avro["name"], "User");
        assert_eq!(avro["namespace"], "com.example");
    }

    #[test]
    fn json_to_avro_integer_with_range() {
        let schema = json!({
            "type": "object",
            "properties": {
                "small": {
                    "type": "integer",
                    "x-range": {"min": 0, "max": 100}
                },
                "large": {
                    "type": "integer",
                    "x-range": {"min": 0, "max": 10000000000_i64}
                }
            },
            "required": ["small", "large"]
        });

        let avro = convert_default(schema);
        let fields = avro["fields"].as_array().unwrap();

        let small_field = fields.iter().find(|f| f["name"] == "small").unwrap();
        assert_eq!(small_field["type"], "int");

        let large_field = fields.iter().find(|f| f["name"] == "large").unwrap();
        assert_eq!(large_field["type"], "long");
    }

    // =========================================================================
    // Avro -> JSON Schema tests
    // =========================================================================

    #[test]
    fn avro_to_json_simple_record() {
        let json = parse_avro_and_convert(
            r#"{
            "type": "record",
            "name": "Person",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }"#,
        );

        assert_eq!(json["type"], "object");
        assert_eq!(json["properties"]["name"]["type"], "string");
        assert_eq!(json["properties"]["age"]["type"], "integer");
    }

    #[test]
    fn avro_to_json_nullable_field() {
        let json = parse_avro_and_convert(
            r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "optional", "type": ["null", "string"], "default": null}
            ]
        }"#,
        );

        let optional_type = &json["properties"]["optional"]["type"];
        assert!(optional_type.is_array());
        let types = optional_type.as_array().unwrap();
        assert!(types.contains(&json!("string")));
        assert!(types.contains(&json!("null")));
    }

    #[test]
    fn avro_to_json_enum() {
        let json = parse_avro_and_convert(
            r#"{
            "type": "record",
            "name": "Test",
            "fields": [{
                "name": "status",
                "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE"]}
            }]
        }"#,
        );

        assert_eq!(json["properties"]["status"]["type"], "string");
        let enum_vals = json["properties"]["status"]["enum"].as_array().unwrap();
        assert!(enum_vals.contains(&json!("ACTIVE")));
        assert!(enum_vals.contains(&json!("INACTIVE")));
    }

    #[test]
    fn avro_to_json_array() {
        let json = parse_avro_and_convert(
            r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}}
            ]
        }"#,
        );

        assert_eq!(json["properties"]["tags"]["type"], "array");
        assert_eq!(json["properties"]["tags"]["items"]["type"], "string");
    }

    #[test]
    fn avro_to_json_nested_record() {
        let json = parse_avro_and_convert(
            r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "address", "type": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {"name": "city", "type": "string"},
                        {"name": "zip", "type": "string"}
                    ]
                }}
            ]
        }"#,
        );

        assert_eq!(json["properties"]["address"]["type"], "object");
        assert_eq!(
            json["properties"]["address"]["properties"]["city"]["type"],
            "string"
        );
        assert_eq!(
            json["properties"]["address"]["properties"]["zip"]["type"],
            "string"
        );
    }

    #[test]
    fn avro_to_json_timestamp() {
        let json = parse_avro_and_convert(
            r#"{
            "type": "record",
            "name": "Event",
            "fields": [
                {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }"#,
        );

        assert_eq!(json["properties"]["created_at"]["type"], "string");
        assert_eq!(json["properties"]["created_at"]["format"], "date-time");
    }

    #[test]
    fn avro_to_json_required_fields() {
        let json = parse_avro_and_convert(
            r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "required_field", "type": "string"},
                {"name": "optional_field", "type": ["null", "string"], "default": null}
            ]
        }"#,
        );

        let required = json["required"].as_array().unwrap();
        assert!(required.contains(&json!("required_field")));
        assert!(!required.contains(&json!("optional_field")));
    }

    // =========================================================================
    // Roundtrip tests
    // =========================================================================

    #[test]
    fn roundtrip_json_to_avro_to_json() {
        let original = json!({
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "count": {"type": "integer"},
                "active": {"type": "boolean"}
            },
            "required": ["id"]
        });

        let options = Some(HashMap::from([(
            String::from("name"),
            String::from("Test"),
        )]));

        let avro_str = json_schema_to_avro(&original, &options).unwrap();
        let avro = AvroSchema::parse_str(&avro_str).unwrap();
        let result_str = avro_to_json_schema(&avro).unwrap();
        let result: JsonValue = serde_json::from_str(&result_str).unwrap();

        assert_eq!(result["type"], "object");
        assert!(
            result["properties"]["id"]["type"] == "string"
                || result["properties"]["id"]["type"]
                    .as_array()
                    .map(|a| a.contains(&json!("string")))
                    .unwrap_or(false)
        );
    }
}
