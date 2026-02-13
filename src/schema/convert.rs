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
use crate::schema::sanitize::{sanitize_avro_name, sanitize_avro_namespace};

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
    let raw_name = options
        .as_ref()
        .and_then(|o| o.get("name"))
        .map(|s| s.as_str())
        .unwrap_or("Record");
    let safe_name = sanitize_avro_name(raw_name);

    let raw_namespace = options
        .as_ref()
        .and_then(|o| o.get("namespace"))
        .map(|s| s.as_str());
    let safe_namespace = raw_namespace.map(|ns| sanitize_avro_namespace(ns));
    let ns_ref = safe_namespace.as_deref();

    let mut counter = 0;
    let mut ctx = ConversionContext {
        namespace: ns_ref,
        counter: &mut counter,
    };

    let avro_value = convert_json_schema_to_avro(json_schema, &safe_name, &mut ctx)?;

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
        let nested_name = sanitize_avro_name(&capitalize(field_name));
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
        .map(|s| sanitize_avro_name(s))
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
        // Ref types cannot be resolved without the full schema context
        AvroSchema::Ref { name } => bail!(
            "cannot convert Avro Ref type '{}' to JSON Schema: named references require full schema context",
            name.fullname(None)
        ),
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

/// A field is required in JSON Schema if it has no default value in Avro.
/// Nullability (union with null) is separate from requiredness — a field
/// can be required AND nullable (must be present, but can be null).
fn is_required_avro_field(field: &apache_avro::schema::RecordField) -> bool {
    field.default.is_none()
}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
