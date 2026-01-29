use fake::Fake;
use fake::faker::address::en::{CityName, PostCode, StreetName};
use fake::faker::internet::en::SafeEmail;
use fake::faker::lorem::en::Word;
use fake::faker::name::en::{FirstName, LastName, Name};
use fake::faker::phone_number::en::PhoneNumber;
use fake::rand::SeedableRng;
use fake::rand::rngs::StdRng;
use rand::Rng;
use serde_json::{Map, Value, json};

use super::JsonSchema;

/// Generates synthetic documents from a JSON Schema.
///
/// Field names are analyzed to produce realistic-looking data suitable
/// for testing masking pipelines.
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
        if depth > 100 {
            return Value::Null;
        }

        // Handle oneOf (nullable types from schema builder)
        if let Some(one_of) = schema.get("oneOf").and_then(|v| v.as_array()) {
            // Pick first non-null variant
            for variant in one_of {
                if variant.get("type") != Some(&json!("null")) {
                    return self.fill_value(variant, field_name, depth);
                }
            }
            return Value::Null;
        }

        // Handle type as array (e.g., ["string", "null"])
        if let Some(types) = schema.get("type").and_then(|t| t.as_array()) {
            // Pick first non-null type
            for t in types {
                if let Some(type_str) = t.as_str() {
                    if type_str != "null" {
                        let single_type_schema = json!({"type": type_str});
                        return self.fill_value(&single_type_schema, field_name, depth);
                    }
                }
            }
            return Value::Null;
        }

        match schema.get("type").and_then(|t| t.as_str()) {
            Some("object") => self.fill_object(schema, depth),
            Some("array") => self.fill_array(schema, field_name, depth),
            Some("string") => self.fill_string(field_name),
            Some("integer") => self.fill_integer(),
            Some("number") => self.fill_number(),
            Some("boolean") => self.fill_boolean(),
            Some("null") => Value::Null,
            _ => Value::Null,
        }
    }

    fn fill_object(&mut self, schema: &JsonSchema, depth: usize) -> Value {
        let mut obj = Map::new();

        if let Some(properties) = schema.get("properties").and_then(|p| p.as_object()) {
            for (key, prop_schema) in properties {
                let value = self.fill_value(prop_schema, Some(key), depth + 1);
                obj.insert(key.clone(), value);
            }
        }

        Value::Object(obj)
    }

    fn fill_array(&mut self, schema: &JsonSchema, field_name: Option<&str>, depth: usize) -> Value {
        let default_items = json!({"type": "string"});
        let items_schema = schema.get("items").unwrap_or(&default_items);
        let count = 2;

        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(self.fill_value(items_schema, field_name, depth + 1));
        }

        Value::Array(items)
    }

    fn fill_string(&mut self, field_name: Option<&str>) -> Value {
        let name_lower = field_name.map(|n| n.to_lowercase()).unwrap_or_default();

        let generated: String = if name_lower.contains("email") {
            SafeEmail().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("firstname") || name_lower.contains("first_name") {
            FirstName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("lastname") || name_lower.contains("last_name") {
            LastName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("name") {
            Name().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("phone") || name_lower.contains("mobile") {
            PhoneNumber().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("street") || name_lower.contains("address") {
            StreetName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("city") {
            CityName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("zip") || name_lower.contains("postal") {
            PostCode().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("ssn") || name_lower.contains("social") {
            format!(
                "{:03}-{:02}-{:04}",
                self.rng.random_range(100..999),
                self.rng.random_range(10..99),
                self.rng.random_range(1000..9999)
            )
        } else if name_lower.contains("creditcard")
            || name_lower.contains("credit_card")
            || name_lower.contains("cardnumber")
            || name_lower.contains("card_number")
        {
            // Visa test card number
            String::from("4111111111111111")
        } else if name_lower.contains("cvv") || name_lower.contains("cvc") {
            format!("{:03}", self.rng.random_range(100..999))
        } else {
            Word().fake_with_rng(&mut self.rng)
        };

        Value::String(generated)
    }

    fn fill_integer(&mut self) -> Value {
        Value::Number(self.rng.random_range(1..1000).into())
    }

    fn fill_number(&mut self) -> Value {
        let n: f64 = self.rng.random_range(1.0..1000.0);
        serde_json::Number::from_f64(n)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }

    fn fill_boolean(&mut self) -> Value {
        Value::Bool(self.rng.random_bool(0.5))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_simple_object() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        assert!(result.is_object());
        assert!(result.get("name").unwrap().is_string());
        assert!(result.get("age").unwrap().is_number());
    }

    #[test]
    fn fill_with_email_hint() {
        let schema = json!({
            "type": "object",
            "properties": {
                "email": {"type": "string"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let email = result.get("email").unwrap().as_str().unwrap();
        assert!(email.contains("@"), "Expected email format, got: {}", email);
    }

    #[test]
    fn fill_array() {
        let schema = json!({
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn fill_nullable_type_array_syntax() {
        let schema = json!({
            "type": "object",
            "properties": {
                "nickname": {"type": ["string", "null"]}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        assert!(result.get("nickname").unwrap().is_string());
    }

    #[test]
    fn fill_nullable_type_oneof_syntax() {
        let schema = json!({
            "type": "object",
            "properties": {
                "address": {
                    "oneOf": [
                        {"type": "object", "properties": {"city": {"type": "string"}}},
                        {"type": "null"}
                    ]
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let address = result.get("address").unwrap();
        assert!(address.is_object());
        assert!(address.get("city").unwrap().is_string());
    }

    #[test]
    fn fill_nested_object() {
        let schema = json!({
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "firstName": {"type": "string"},
                        "lastName": {"type": "string"},
                        "contact": {
                            "type": "object",
                            "properties": {
                                "email": {"type": "string"},
                                "phone": {"type": "string"}
                            }
                        }
                    }
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let user = result.get("user").unwrap();
        assert!(user.get("firstName").unwrap().is_string());
        assert!(user.get("lastName").unwrap().is_string());

        let contact = user.get("contact").unwrap();
        let email = contact.get("email").unwrap().as_str().unwrap();
        assert!(email.contains("@"));
    }

    #[test]
    fn fill_deterministic_with_seed() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            }
        });

        let mut filler1 = SchemaFiller::with_seed(123);
        let mut filler2 = SchemaFiller::with_seed(123);

        let result1 = filler1.fill(&schema);
        let result2 = filler2.fill(&schema);

        assert_eq!(result1, result2);
    }

    #[test]
    fn fill_ssn_format() {
        let schema = json!({
            "type": "object",
            "properties": {
                "ssn": {"type": "string"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let ssn = result.get("ssn").unwrap().as_str().unwrap();
        // Format: XXX-XX-XXXX
        assert!(
            ssn.len() == 11 && ssn.chars().nth(3) == Some('-') && ssn.chars().nth(6) == Some('-'),
            "Expected SSN format XXX-XX-XXXX, got: {}",
            ssn
        );
    }

    #[test]
    fn fill_all_primitive_types() {
        let schema = json!({
            "type": "object",
            "properties": {
                "str": {"type": "string"},
                "int": {"type": "integer"},
                "num": {"type": "number"},
                "bool": {"type": "boolean"},
                "null": {"type": "null"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        assert!(result.get("str").unwrap().is_string());
        assert!(result.get("int").unwrap().is_i64());
        assert!(result.get("num").unwrap().is_f64());
        assert!(result.get("bool").unwrap().is_boolean());
        assert!(result.get("null").unwrap().is_null());
    }
}
