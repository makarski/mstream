use super::*;
use serde_json::json;
use std::collections::HashMap;

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

#[test]
fn avro_to_json_nullable_without_default_is_required() {
    let json = parse_avro_and_convert(
        r#"{
        "type": "record",
        "name": "Test",
        "fields": [
            {"name": "nullable_required", "type": ["null", "string"]}
        ]
    }"#,
    );

    let required = json["required"].as_array().unwrap();
    assert!(
        required.contains(&json!("nullable_required")),
        "nullable field without default should be required"
    );

    let field_type = &json["properties"]["nullable_required"]["type"];
    assert!(field_type.is_array(), "should be nullable type array");
    let types = field_type.as_array().unwrap();
    assert!(types.contains(&json!("null")));
}

#[test]
fn json_to_avro_with_hyphenated_name_and_namespace() {
    let schema = json!({
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        },
        "$schema": "https://json-schema.org/draft/2020-12/schema"
    });

    let options = Some(HashMap::from([
        (String::from("name"), String::from("demo-users")),
        (
            String::from("namespace"),
            String::from("com.mstream.etl-test-sinks"),
        ),
    ]));

    let result = json_schema_to_avro(&schema, &options);
    assert!(
        result.is_ok(),
        "conversion should not panic or fail: {:?}",
        result.err()
    );

    let avro_str = result.unwrap();
    let avro: JsonValue = serde_json::from_str(&avro_str).unwrap();
    assert_eq!(avro["name"], "demo_users");
    assert_eq!(avro["namespace"], "com.mstream.etl_test_sinks");
}

#[test]
fn json_to_avro_sanitizes_field_names_for_nested_records() {
    let schema = json!({
        "type": "object",
        "properties": {
            "user-info": {
                "type": "object",
                "properties": {
                    "full.name": {"type": "string"}
                }
            }
        }
    });

    let result = json_schema_to_avro(&schema, &None);
    assert!(result.is_ok(), "should sanitize invalid field names");
}

#[test]
fn json_to_avro_sanitizes_enum_symbols() {
    let schema = json!({
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "enum": ["in-progress", "on.hold", "123done"]
            }
        }
    });

    let result = json_schema_to_avro(&schema, &None);
    assert!(result.is_ok(), "should sanitize invalid enum symbols");

    let avro: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
    let fields = avro["fields"].as_array().unwrap();
    let status_field = fields.iter().find(|f| f["name"] == "status").unwrap();

    let enum_type = status_field["type"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t.is_object() && t["type"] == "enum")
        .unwrap();

    let symbols = enum_type["symbols"].as_array().unwrap();
    assert!(
        symbols.iter().all(|s| {
            let sym = s.as_str().unwrap();
            sym.chars()
                .next()
                .map(|c| c.is_ascii_alphabetic() || c == '_')
                .unwrap_or(false)
        }),
        "all symbols should start with letter or underscore"
    );
}

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
