//! Schema filling module for generating synthetic documents.
//!
//! This module provides `SchemaFiller`, which generates synthetic JSON documents
//! that conform to a given JSON Schema.

mod generators;
mod matchers;

use fake::rand::SeedableRng;
use fake::rand::rngs::StdRng;
use rand::Rng;
use serde_json::{Map, Value, json};

use super::JsonSchema;
use generators::{
    generate_by_format, generate_integer_by_name, generate_number_by_name, generate_string_by_name,
};

#[cfg(test)]
use generators::{is_date_field, is_person_name_field};

const MAX_DEPTH: usize = 100;

/// Generates synthetic documents from a JSON Schema.
pub struct SchemaFiller {
    rng: StdRng,
}

impl Default for SchemaFiller {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaFiller {
    pub fn new() -> Self {
        Self {
            rng: StdRng::from_os_rng(),
        }
    }

    pub fn with_seed(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Fill a JSON Schema with synthetic data.
    pub fn fill(&mut self, schema: &JsonSchema) -> Value {
        self.fill_value(schema, None, 0)
    }

    fn fill_value(&mut self, schema: &JsonSchema, field_name: Option<&str>, depth: usize) -> Value {
        if depth > MAX_DEPTH {
            return Value::Null;
        }

        // Handle oneOf (nullable types)
        if let Some(value) = self.try_fill_one_of(schema, field_name, depth) {
            return value;
        }

        // Handle type arrays (e.g., ["string", "null"])
        if let Some(value) = self.try_fill_type_array(schema, field_name, depth) {
            return value;
        }

        self.fill_by_type(schema, field_name, depth)
    }

    fn try_fill_one_of(
        &mut self,
        schema: &JsonSchema,
        field_name: Option<&str>,
        depth: usize,
    ) -> Option<Value> {
        let one_of = schema.get("oneOf")?.as_array()?;

        for variant in one_of {
            if variant.get("type") != Some(&json!("null")) {
                return Some(self.fill_value(variant, field_name, depth + 1));
            }
        }

        Some(Value::Null)
    }

    fn try_fill_type_array(
        &mut self,
        schema: &JsonSchema,
        field_name: Option<&str>,
        depth: usize,
    ) -> Option<Value> {
        let types = schema.get("type")?.as_array()?;

        for t in types {
            let type_str = t.as_str()?;
            if type_str != "null" {
                let mut single_schema = schema.clone();
                if let Some(obj) = single_schema.as_object_mut() {
                    obj.insert("type".to_string(), json!(type_str));
                }
                return Some(self.fill_value(&single_schema, field_name, depth + 1));
            }
        }

        Some(Value::Null)
    }

    fn fill_by_type(
        &mut self,
        schema: &JsonSchema,
        field_name: Option<&str>,
        depth: usize,
    ) -> Value {
        match schema.get("type").and_then(|t| t.as_str()) {
            Some("object") => self.fill_object(schema, depth),
            Some("array") => self.fill_array(schema, field_name, depth),
            Some("string") => self.fill_string(schema, field_name),
            Some("integer") => self.fill_integer(schema, field_name),
            Some("number") => self.fill_number(schema, field_name),
            Some("boolean") => Value::Bool(self.rng.random_bool(0.5)),
            _ => Value::Null,
        }
    }

    fn fill_object(&mut self, schema: &JsonSchema, depth: usize) -> Value {
        let mut obj = Map::new();

        if let Some(properties) = schema.get("properties").and_then(|p| p.as_object()) {
            for (key, prop_schema) in properties {
                obj.insert(
                    key.clone(),
                    self.fill_value(prop_schema, Some(key), depth + 1),
                );
            }
        }

        Value::Object(obj)
    }

    fn fill_array(&mut self, schema: &JsonSchema, field_name: Option<&str>, depth: usize) -> Value {
        let default_items = json!({"type": "string"});
        let items_schema = schema.get("items").unwrap_or(&default_items);
        let count = self.rng.random_range(1..=3);

        Value::Array(
            (0..count)
                .map(|_| self.fill_value(items_schema, field_name, depth + 1))
                .collect(),
        )
    }

    fn fill_string(&mut self, schema: &JsonSchema, field_name: Option<&str>) -> Value {
        // Priority 1: enum
        if let Some(value) = self.try_fill_enum(schema) {
            return value;
        }

        // Priority 2: format
        if let Some(format) = schema.get("format").and_then(|v| v.as_str()) {
            return Value::String(generate_by_format(&mut self.rng, format));
        }

        // Priority 3: field name heuristics
        let name = field_name.map(|n| n.to_lowercase()).unwrap_or_default();
        Value::String(generate_string_by_name(&mut self.rng, &name))
    }

    fn fill_integer(&mut self, schema: &JsonSchema, field_name: Option<&str>) -> Value {
        // Priority 1: schema-defined range
        if let Some(value) = self.try_fill_integer_range(schema) {
            return value;
        }

        // Priority 2: field name heuristics
        let name = field_name.map(|n| n.to_lowercase()).unwrap_or_default();
        Value::Number(generate_integer_by_name(&mut self.rng, &name).into())
    }

    fn fill_number(&mut self, schema: &JsonSchema, field_name: Option<&str>) -> Value {
        // Priority 1: schema-defined range
        if let Some(value) = self.try_fill_number_range(schema) {
            return value;
        }

        // Priority 2: field name heuristics
        let name = field_name.map(|n| n.to_lowercase()).unwrap_or_default();
        let value = generate_number_by_name(&mut self.rng, &name);

        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }

    fn try_fill_enum(&mut self, schema: &JsonSchema) -> Option<Value> {
        let enum_values = schema.get("enum")?.as_array()?;
        let non_null: Vec<_> = enum_values.iter().filter(|v| !v.is_null()).collect();

        if non_null.is_empty() {
            return None;
        }

        let idx = self.rng.random_range(0..non_null.len());
        non_null[idx].as_str().map(|s| Value::String(s.to_string()))
    }

    fn try_fill_integer_range(&mut self, schema: &JsonSchema) -> Option<Value> {
        let min = schema.get("minimum").and_then(|v| v.as_f64())? as i64;
        let max = schema.get("maximum").and_then(|v| v.as_f64())? as i64;
        Some(Value::Number(self.rng.random_range(min..=max).into()))
    }

    fn try_fill_number_range(&mut self, schema: &JsonSchema) -> Option<Value> {
        let min = schema.get("minimum").and_then(|v| v.as_f64())?;
        let max = schema.get("maximum").and_then(|v| v.as_f64())?;
        let value = self.rng.random_range(min..=max);
        let rounded = (value * 100.0).round() / 100.0;
        serde_json::Number::from_f64(rounded).map(Value::Number)
    }

    // Expose helper functions for testing
    #[cfg(test)]
    pub fn is_person_name_field(&self, name: &str) -> bool {
        is_person_name_field(name)
    }

    #[cfg(test)]
    pub fn is_date_field(&self, name: &str) -> bool {
        is_date_field(name)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(properties: Value) -> JsonSchema {
        json!({ "type": "object", "properties": properties })
    }

    // -------------------------------------------------------------------------
    // Basic type tests
    // -------------------------------------------------------------------------

    #[test]
    fn fill_simple_object() {
        let s = schema(json!({ "name": {"type": "string"}, "age": {"type": "integer"} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        assert!(result.get("name").unwrap().is_string());
        assert!(result.get("age").unwrap().is_number());
    }

    #[test]
    fn fill_all_primitive_types() {
        let s = schema(json!({
            "str": {"type": "string"},
            "int": {"type": "integer"},
            "num": {"type": "number"},
            "bool": {"type": "boolean"},
            "null": {"type": "null"}
        }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        assert!(result.get("str").unwrap().is_string());
        assert!(result.get("int").unwrap().is_i64());
        assert!(result.get("num").unwrap().is_f64());
        assert!(result.get("bool").unwrap().is_boolean());
        assert!(result.get("null").unwrap().is_null());
    }

    #[test]
    fn fill_array() {
        let s = schema(json!({ "tags": { "type": "array", "items": {"type": "string"} } }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert!(!tags.is_empty());
        assert!(tags.len() <= 3);
    }

    #[test]
    fn fill_nested_object() {
        let s = schema(json!({
            "user": {
                "type": "object",
                "properties": {
                    "profile": {
                        "type": "object",
                        "properties": { "email": {"type": "string"} }
                    }
                }
            }
        }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let email = result
            .get("user")
            .unwrap()
            .get("profile")
            .unwrap()
            .get("email")
            .unwrap();
        assert!(email.as_str().unwrap().contains("@"));
    }

    // -------------------------------------------------------------------------
    // Nullable type tests
    // -------------------------------------------------------------------------

    #[test]
    fn fill_nullable_type_array_syntax() {
        let s = schema(json!({ "nickname": {"type": ["string", "null"]} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        assert!(result.get("nickname").unwrap().is_string());
    }

    #[test]
    fn fill_nullable_type_oneof_syntax() {
        let s = schema(json!({
            "address": {
                "oneOf": [
                    {"type": "object", "properties": {"city": {"type": "string"}}},
                    {"type": "null"}
                ]
            }
        }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        assert!(result.get("address").unwrap().is_object());
    }

    // -------------------------------------------------------------------------
    // Format tests
    // -------------------------------------------------------------------------

    #[test]
    fn fill_format_email() {
        let s = schema(json!({ "contact": {"type": "string", "format": "email"} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        assert!(
            result
                .get("contact")
                .unwrap()
                .as_str()
                .unwrap()
                .contains("@")
        );
    }

    #[test]
    fn fill_format_datetime() {
        let s = schema(json!({ "created_at": {"type": "string", "format": "date-time"} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("created_at").unwrap().as_str().unwrap();
        assert!(value.contains("T") && value.ends_with("Z"));
    }

    #[test]
    fn fill_format_objectid() {
        let s = schema(json!({ "ref": {"type": "string", "format": "objectid"} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("ref").unwrap().as_str().unwrap();
        assert_eq!(value.len(), 24);
        assert!(value.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // -------------------------------------------------------------------------
    // Enum tests
    // -------------------------------------------------------------------------

    #[test]
    fn fill_enum() {
        let s = schema(json!({ "status": {"type": "string", "enum": ["a", "b", "c"]} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("status").unwrap().as_str().unwrap();
        assert!(["a", "b", "c"].contains(&value));
    }

    #[test]
    fn fill_enum_nullable() {
        let s = schema(json!({ "status": {"type": ["string", "null"], "enum": ["a", "b", null]} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("status").unwrap().as_str().unwrap();
        assert!(["a", "b"].contains(&value));
    }

    #[test]
    fn enum_takes_precedence_over_format() {
        let s =
            schema(json!({ "email": {"type": "string", "format": "email", "enum": ["x@y.com"]} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        assert_eq!(result.get("email").unwrap().as_str().unwrap(), "x@y.com");
    }

    // -------------------------------------------------------------------------
    // Min/max tests
    // -------------------------------------------------------------------------

    #[test]
    fn fill_integer_with_range() {
        let s = schema(json!({ "age": {"type": "integer", "minimum": 18, "maximum": 65} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("age").unwrap().as_i64().unwrap();
        assert!((18..=65).contains(&value));
    }

    #[test]
    fn fill_number_with_range() {
        let s = schema(json!({ "gpa": {"type": "number", "minimum": 0.0, "maximum": 4.0} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("gpa").unwrap().as_f64().unwrap();
        assert!((0.0..=4.0).contains(&value));
    }

    #[test]
    fn nullable_preserves_range() {
        let s =
            schema(json!({ "score": {"type": ["integer", "null"], "minimum": 0, "maximum": 100} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("score").unwrap().as_i64().unwrap();
        assert!((0..=100).contains(&value));
    }

    // -------------------------------------------------------------------------
    // Field name heuristic tests
    // -------------------------------------------------------------------------

    #[test]
    fn field_name_email() {
        let s = schema(json!({ "user_email": {"type": "string"} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        assert!(
            result
                .get("user_email")
                .unwrap()
                .as_str()
                .unwrap()
                .contains("@")
        );
    }

    #[test]
    fn field_name_object_id() {
        let s = schema(json!({ "_id": {"type": "string"} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("_id").unwrap().as_str().unwrap();
        assert_eq!(value.len(), 24);
    }

    #[test]
    fn field_name_date() {
        let s = schema(json!({ "created_at": {"type": "string"} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("created_at").unwrap().as_str().unwrap();
        assert!(value.contains("T"));
    }

    #[test]
    fn field_name_coordinates() {
        let s = schema(json!({ "lat": {"type": "number"}, "lng": {"type": "number"} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let lat = result.get("lat").unwrap().as_f64().unwrap();
        let lng = result.get("lng").unwrap().as_f64().unwrap();
        assert!((-90.0..=90.0).contains(&lat));
        assert!((-180.0..=180.0).contains(&lng));
    }

    #[test]
    fn field_name_ssn() {
        let s = schema(json!({ "ssn": {"type": "string"} }));
        let result = SchemaFiller::with_seed(42).fill(&s);

        let value = result.get("ssn").unwrap().as_str().unwrap();
        assert_eq!(value.len(), 11);
        assert!(value.chars().nth(3) == Some('-'));
    }

    // -------------------------------------------------------------------------
    // Person name detection tests
    // -------------------------------------------------------------------------

    #[test]
    fn person_name_exact_match() {
        let filler = SchemaFiller::with_seed(42);
        assert!(filler.is_person_name_field("name"));
        assert!(filler.is_person_name_field("contact_name"));
    }

    #[test]
    fn person_name_excludes_non_person() {
        let filler = SchemaFiller::with_seed(42);
        assert!(!filler.is_person_name_field("filename"));
        assert!(!filler.is_person_name_field("hostname"));
        assert!(!filler.is_person_name_field("nickname"));
    }

    // -------------------------------------------------------------------------
    // Date field detection tests
    // -------------------------------------------------------------------------

    #[test]
    fn date_field_by_suffix() {
        let filler = SchemaFiller::with_seed(42);
        assert!(filler.is_date_field("created_at"));
        assert!(filler.is_date_field("birth_date"));
    }

    #[test]
    fn date_field_by_prefix() {
        let filler = SchemaFiller::with_seed(42);
        assert!(filler.is_date_field("date_of_birth"));
    }

    #[test]
    fn date_field_by_keyword() {
        let filler = SchemaFiller::with_seed(42);
        assert!(filler.is_date_field("timestamp"));
        assert!(filler.is_date_field("last_login"));
    }

    // -------------------------------------------------------------------------
    // Determinism test
    // -------------------------------------------------------------------------

    #[test]
    fn deterministic_with_seed() {
        let s = schema(json!({ "name": {"type": "string"}, "age": {"type": "integer"} }));

        let r1 = SchemaFiller::with_seed(123).fill(&s);
        let r2 = SchemaFiller::with_seed(123).fill(&s);

        assert_eq!(r1, r2);
    }
}
