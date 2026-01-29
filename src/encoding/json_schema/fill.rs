use fake::Fake;
use fake::faker::address::en::{CityName, CountryName, PostCode, StateName, StreetName};
use fake::faker::internet::en::SafeEmail;
use fake::faker::lorem::en::Word;
use fake::faker::name::en::{FirstName, LastName, Name};
use fake::faker::phone_number::en::PhoneNumber;
use fake::rand::SeedableRng;
use fake::rand::rngs::StdRng;
use rand::Rng;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::JsonSchema;

/// Generates synthetic documents from a JSON Schema.
///
/// Uses schema hints (format, enum, minimum/maximum) when available,
/// falls back to field name heuristics otherwise.
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

        if let Some(one_of) = schema.get("oneOf").and_then(|v| v.as_array()) {
            for variant in one_of {
                if variant.get("type") != Some(&json!("null")) {
                    return self.fill_value(variant, field_name, depth + 1);
                }
            }
            return Value::Null;
        }

        if let Some(types) = schema.get("type").and_then(|t| t.as_array()) {
            for t in types {
                if let Some(type_str) = t.as_str() {
                    if type_str != "null" {
                        // Create a new schema with single type but preserve other properties
                        let mut single_type_schema = schema.clone();
                        if let Some(obj) = single_type_schema.as_object_mut() {
                            obj.insert("type".to_string(), json!(type_str));
                        }
                        return self.fill_value(&single_type_schema, field_name, depth + 1);
                    }
                }
            }
            return Value::Null;
        }

        match schema.get("type").and_then(|t| t.as_str()) {
            Some("object") => self.fill_object(schema, depth),
            Some("array") => self.fill_array(schema, field_name, depth),
            Some("string") => self.fill_string(schema, field_name),
            Some("integer") => self.fill_integer(schema, field_name),
            Some("number") => self.fill_number(schema, field_name),
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
        let count = self.rng.random_range(1..=3);

        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(self.fill_value(items_schema, field_name, depth + 1));
        }

        Value::Array(items)
    }

    fn fill_string(&mut self, schema: &JsonSchema, field_name: Option<&str>) -> Value {
        // Check for enum first - highest priority
        if let Some(enum_values) = schema.get("enum").and_then(|v| v.as_array()) {
            let non_null: Vec<&Value> = enum_values.iter().filter(|v| !v.is_null()).collect();
            if !non_null.is_empty() {
                let idx = self.rng.random_range(0..non_null.len());
                if let Some(s) = non_null[idx].as_str() {
                    return Value::String(s.to_string());
                }
            }
        }

        // Check for format hint
        if let Some(format) = schema.get("format").and_then(|v| v.as_str()) {
            return Value::String(self.generate_by_format(format));
        }

        // Fall back to field name heuristics
        let name_lower = field_name.map(|n| n.to_lowercase()).unwrap_or_default();
        Value::String(self.generate_by_field_name(&name_lower))
    }

    fn fill_integer(&mut self, schema: &JsonSchema, field_name: Option<&str>) -> Value {
        // Check for min/max from schema
        let min = schema
            .get("minimum")
            .and_then(|v| v.as_f64())
            .map(|n| n as i64);
        let max = schema
            .get("maximum")
            .and_then(|v| v.as_f64())
            .map(|n| n as i64);

        if let (Some(min_val), Some(max_val)) = (min, max) {
            // Use schema-defined range
            let value = self.rng.random_range(min_val..=max_val);
            return Value::Number(value.into());
        }

        // Fall back to field name heuristics
        let name_lower = field_name.map(|n| n.to_lowercase()).unwrap_or_default();
        let value = self.generate_integer_by_field_name(&name_lower);
        Value::Number(value.into())
    }

    fn fill_number(&mut self, schema: &JsonSchema, field_name: Option<&str>) -> Value {
        // Check for min/max from schema
        let min = schema.get("minimum").and_then(|v| v.as_f64());
        let max = schema.get("maximum").and_then(|v| v.as_f64());

        if let (Some(min_val), Some(max_val)) = (min, max) {
            // Use schema-defined range
            let value: f64 = self.rng.random_range(min_val..=max_val);
            // Round to 2 decimal places for cleaner output
            let rounded = (value * 100.0).round() / 100.0;
            return serde_json::Number::from_f64(rounded)
                .map(Value::Number)
                .unwrap_or(Value::Null);
        }

        // Fall back to field name heuristics
        let name_lower = field_name.map(|n| n.to_lowercase()).unwrap_or_default();
        let value = self.generate_number_by_field_name(&name_lower);
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }

    fn fill_boolean(&mut self) -> Value {
        Value::Bool(self.rng.random_bool(0.5))
    }

    /// Generate a string value based on format hint
    fn generate_by_format(&mut self, format: &str) -> String {
        match format {
            "date-time" => self.generate_iso_datetime(),
            "email" => SafeEmail().fake_with_rng(&mut self.rng),
            "objectid" => self.generate_object_id(),
            "date" => self.generate_iso_date(),
            "time" => self.generate_iso_time(),
            "uri" | "url" => format!(
                "https://example.com/{}",
                Word().fake_with_rng::<String, _>(&mut self.rng)
            ),
            "uuid" => self.generate_uuid(),
            "ipv4" => format!(
                "{}.{}.{}.{}",
                self.rng.random_range(1..255),
                self.rng.random_range(0..255),
                self.rng.random_range(0..255),
                self.rng.random_range(1..255)
            ),
            "ipv6" => format!(
                "{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}",
                self.rng.random_range(0..65535u16),
                self.rng.random_range(0..65535u16),
                self.rng.random_range(0..65535u16),
                self.rng.random_range(0..65535u16),
                self.rng.random_range(0..65535u16),
                self.rng.random_range(0..65535u16),
                self.rng.random_range(0..65535u16),
                self.rng.random_range(0..65535u16)
            ),
            "hostname" => format!(
                "{}.example.com",
                Word().fake_with_rng::<String, _>(&mut self.rng)
            ),
            _ => Word().fake_with_rng(&mut self.rng),
        }
    }

    /// Generate a string value based on field name heuristics
    fn generate_by_field_name(&mut self, name_lower: &str) -> String {
        if name_lower == "_id" || name_lower == "id" || name_lower.ends_with("_id") {
            self.generate_object_id()
        } else if name_lower.contains("email") {
            SafeEmail().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("firstname") || name_lower.contains("first_name") {
            FirstName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("lastname") || name_lower.contains("last_name") {
            LastName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("full_name") || name_lower.contains("fullname") {
            Name().fake_with_rng(&mut self.rng)
        } else if self.is_person_name_field(name_lower) {
            Name().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("phone") || name_lower.contains("mobile") {
            PhoneNumber().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("street") {
            StreetName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("city") {
            CityName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("state") || name_lower.contains("province") {
            StateName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("country") {
            CountryName().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("zip")
            || name_lower.contains("postal")
            || name_lower.contains("postcode")
        {
            PostCode().fake_with_rng(&mut self.rng)
        } else if name_lower.contains("address") {
            format!(
                "{} {}",
                self.rng.random_range(1..=9999),
                StreetName().fake_with_rng::<String, _>(&mut self.rng)
            )
        } else if self.is_date_field(name_lower) {
            self.generate_iso_datetime()
        } else if name_lower.contains("ssn") || name_lower.contains("social") {
            format!(
                "{:03}-{:02}-{:04}",
                self.rng.random_range(100..=999),
                self.rng.random_range(10..=99),
                self.rng.random_range(1000..=9999)
            )
        } else if name_lower.contains("creditcard")
            || name_lower.contains("credit_card")
            || name_lower.contains("cardnumber")
            || name_lower.contains("card_number")
        {
            String::from("4111111111111111")
        } else if name_lower.contains("cvv") || name_lower.contains("cvc") {
            format!("{:03}", self.rng.random_range(100..=999))
        } else {
            Word().fake_with_rng(&mut self.rng)
        }
    }

    /// Generate an integer value based on field name heuristics
    fn generate_integer_by_field_name(&mut self, name_lower: &str) -> i64 {
        if name_lower.contains("credits") {
            self.rng.random_range(0..=180)
        } else if self.is_academic_year_field(name_lower) {
            self.rng.random_range(1..=8)
        } else if name_lower.contains("year") {
            // Calendar year (e.g., graduation_year, birth_year, publication_year)
            self.rng.random_range(2020..=2025)
        } else if name_lower.contains("salary") {
            self.rng.random_range(30000..=200000)
        } else if name_lower.contains("age") {
            self.rng.random_range(18..=80)
        } else if name_lower.contains("count") || name_lower.contains("total") {
            self.rng.random_range(0..=100)
        } else if name_lower.contains("h_index") {
            self.rng.random_range(0..=100)
        } else {
            self.rng.random_range(1..=1000)
        }
    }

    /// Generate a float value based on field name heuristics
    fn generate_number_by_field_name(&mut self, name_lower: &str) -> f64 {
        if name_lower == "lat" || name_lower.contains("latitude") {
            self.rng.random_range(-90.0..90.0)
        } else if name_lower == "lng" || name_lower == "lon" || name_lower.contains("longitude") {
            self.rng.random_range(-180.0..180.0)
        } else if name_lower.contains("gpa") {
            let gpa: f64 = self.rng.random_range(0.0..4.0);
            (gpa * 100.0).round() / 100.0
        } else if name_lower.contains("percent") || name_lower.contains("ratio") {
            self.rng.random_range(0.0..100.0)
        } else if name_lower.contains("price") || name_lower.contains("cost") {
            let price: f64 = self.rng.random_range(1.0..1000.0);
            (price * 100.0).round() / 100.0
        } else if name_lower.contains("salary") {
            self.rng.random_range(30000.0..200000.0)
        } else if name_lower.contains("h_index") {
            self.rng.random_range(0.0..100.0)
        } else {
            self.rng.random_range(1.0..1000.0)
        }
    }

    /// Check if a field name indicates a person's name (not filename, hostname, etc.)
    fn is_person_name_field(&self, name_lower: &str) -> bool {
        // Exact matches that are clearly person names
        if name_lower == "name"
            || name_lower == "person_name"
            || name_lower == "contact_name"
            || name_lower == "author_name"
            || name_lower == "customer_name"
            || name_lower == "employee_name"
            || name_lower == "student_name"
            || name_lower == "user_name"
            || name_lower == "display_name"
            || name_lower == "legal_name"
        {
            return true;
        }

        // If it contains "name" but is NOT one of these non-person patterns, treat as person name
        if name_lower.contains("name") {
            let non_person_patterns = [
                "filename",
                "file_name",
                "hostname",
                "host_name",
                "pathname",
                "path_name",
                "dirname",
                "dir_name",
                "typename",
                "type_name",
                "classname",
                "class_name",
                "tablename",
                "table_name",
                "colname",
                "col_name",
                "column_name",
                "fieldname",
                "field_name",
                "varname",
                "var_name",
                "keyname",
                "key_name",
                "tagname",
                "tag_name",
                "nickname",
                "codename",
                "code_name",
                "servicename",
                "service_name",
                "dbname",
                "db_name",
                "schemaname",
                "schema_name",
            ];

            return !non_person_patterns
                .iter()
                .any(|pattern| name_lower.contains(pattern));
        }

        false
    }

    /// Check if a field name indicates an academic year level (1-8) vs calendar year
    fn is_academic_year_field(&self, name_lower: &str) -> bool {
        name_lower == "year_of_study"
            || name_lower == "study_year"
            || name_lower == "academic_year"
            || name_lower == "year_level"
            || name_lower == "school_year"
            || name_lower.contains("year_of_study")
    }

    /// Check if a field name indicates a date/datetime field
    fn is_date_field(&self, name_lower: &str) -> bool {
        if name_lower.ends_with("_at")
            || name_lower.ends_with("_date")
            || name_lower.ends_with("_time")
            || name_lower.ends_with("_on")
        {
            return true;
        }

        if name_lower.starts_with("date_of_")
            || name_lower.starts_with("time_of_")
            || name_lower.starts_with("date_")
        {
            return true;
        }

        let date_keywords = [
            "created",
            "updated",
            "modified",
            "deleted",
            "timestamp",
            "datetime",
            "birthdate",
            "birthday",
            "dob",
            "enrolled",
            "started",
            "finished",
            "completed",
            "expired",
            "expiry",
            "login",
            "last_seen",
            "registered",
        ];

        for keyword in &date_keywords {
            if name_lower.contains(keyword) {
                return true;
            }
        }

        false
    }

    /// Generate an ISO 8601 datetime string
    fn generate_iso_datetime(&mut self) -> String {
        let year = self.rng.random_range(2020..=2025);
        let month = self.rng.random_range(1..=12);
        let day = self.rng.random_range(1..=28);
        let hour = self.rng.random_range(0..=23);
        let minute = self.rng.random_range(0..=59);
        let second = self.rng.random_range(0..=59);
        let millis = self.rng.random_range(0..=999);

        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            year, month, day, hour, minute, second, millis
        )
    }

    /// Generate an ISO 8601 date string (no time component)
    fn generate_iso_date(&mut self) -> String {
        let year = self.rng.random_range(2020..=2025);
        let month = self.rng.random_range(1..=12);
        let day = self.rng.random_range(1..=28);
        format!("{:04}-{:02}-{:02}", year, month, day)
    }

    /// Generate an ISO 8601 time string
    fn generate_iso_time(&mut self) -> String {
        let hour = self.rng.random_range(0..=23);
        let minute = self.rng.random_range(0..=59);
        let second = self.rng.random_range(0..=59);
        format!("{:02}:{:02}:{:02}", hour, minute, second)
    }

    /// Generate a MongoDB ObjectId-like hex string (24 characters)
    fn generate_object_id(&mut self) -> String {
        let bytes: [u8; 12] = self.rng.random();
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Generate a UUID v4 string
    fn generate_uuid(&mut self) -> String {
        let bytes: [u8; 16] = self.rng.random();
        Uuid::from_bytes(bytes).to_string()
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
        assert!(!tags.is_empty());
        assert!(tags.len() <= 3);
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

    #[test]
    fn fill_with_format_datetime() {
        let schema = json!({
            "type": "object",
            "properties": {
                "created_at": {"type": "string", "format": "date-time"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let value = result.get("created_at").unwrap().as_str().unwrap();
        assert!(
            value.contains("T") && value.contains("-") && value.contains(":"),
            "Expected ISO datetime, got: {}",
            value
        );
    }

    #[test]
    fn fill_with_format_email() {
        let schema = json!({
            "type": "object",
            "properties": {
                "contact": {"type": "string", "format": "email"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let value = result.get("contact").unwrap().as_str().unwrap();
        assert!(value.contains("@"), "Expected email, got: {}", value);
    }

    #[test]
    fn fill_with_format_objectid() {
        let schema = json!({
            "type": "object",
            "properties": {
                "ref": {"type": "string", "format": "objectid"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let value = result.get("ref").unwrap().as_str().unwrap();
        assert_eq!(value.len(), 24, "Expected 24-char hex, got: {}", value);
        assert!(
            value.chars().all(|c| c.is_ascii_hexdigit()),
            "Expected hex string, got: {}",
            value
        );
    }

    #[test]
    fn fill_with_enum() {
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

        let value = result.get("status").unwrap().as_str().unwrap();
        assert!(
            ["active", "pending", "inactive"].contains(&value),
            "Expected enum value, got: {}",
            value
        );
    }

    #[test]
    fn fill_with_enum_nullable() {
        let schema = json!({
            "type": "object",
            "properties": {
                "status": {
                    "type": ["string", "null"],
                    "enum": ["active", "pending", null]
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let value = result.get("status").unwrap().as_str().unwrap();
        assert!(
            ["active", "pending"].contains(&value),
            "Expected enum value (non-null), got: {}",
            value
        );
    }

    #[test]
    fn fill_with_min_max_integer() {
        let schema = json!({
            "type": "object",
            "properties": {
                "age": {
                    "type": "integer",
                    "minimum": 18,
                    "maximum": 65
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let value = result.get("age").unwrap().as_i64().unwrap();
        assert!((18..=65).contains(&value), "Expected 18-65, got: {}", value);
    }

    #[test]
    fn fill_with_min_max_number() {
        let schema = json!({
            "type": "object",
            "properties": {
                "gpa": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 4.0
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let value = result.get("gpa").unwrap().as_f64().unwrap();
        assert!(
            (0.0..=4.0).contains(&value),
            "Expected 0.0-4.0, got: {}",
            value
        );
    }

    #[test]
    fn fill_object_id_field_name() {
        let schema = json!({
            "type": "object",
            "properties": {
                "_id": {"type": "string"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let id = result.get("_id").unwrap().as_str().unwrap();
        assert_eq!(id.len(), 24, "Expected 24-char hex string, got: {}", id);
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()),
            "Expected hex string, got: {}",
            id
        );
    }

    #[test]
    fn fill_date_fields_by_name() {
        let schema = json!({
            "type": "object",
            "properties": {
                "created_at": {"type": "string"},
                "updated_at": {"type": "string"},
                "date_of_birth": {"type": "string"},
                "enrolled_at": {"type": "string"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        for field in &["created_at", "updated_at", "date_of_birth", "enrolled_at"] {
            let value = result.get(*field).unwrap().as_str().unwrap();
            assert!(
                value.contains("T") && value.contains("-") && value.contains(":"),
                "Expected ISO datetime for {}, got: {}",
                field,
                value
            );
        }
    }

    #[test]
    fn fill_coordinates_by_name() {
        let schema = json!({
            "type": "object",
            "properties": {
                "geo": {
                    "type": "object",
                    "properties": {
                        "lat": {"type": "number"},
                        "lng": {"type": "number"}
                    }
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let geo = result.get("geo").unwrap();
        let lat = geo.get("lat").unwrap().as_f64().unwrap();
        let lng = geo.get("lng").unwrap().as_f64().unwrap();

        assert!(
            (-90.0..=90.0).contains(&lat),
            "Latitude should be -90 to 90, got: {}",
            lat
        );
        assert!(
            (-180.0..=180.0).contains(&lng),
            "Longitude should be -180 to 180, got: {}",
            lng
        );
    }

    #[test]
    fn fill_state_and_country() {
        let schema = json!({
            "type": "object",
            "properties": {
                "state": {"type": "string"},
                "country": {"type": "string"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let state = result.get("state").unwrap().as_str().unwrap();
        let country = result.get("country").unwrap().as_str().unwrap();

        assert!(
            !state.chars().all(|c| c.is_lowercase()),
            "State should be capitalized: {}",
            state
        );
        assert!(
            !country.chars().all(|c| c.is_lowercase()),
            "Country should be capitalized: {}",
            country
        );
    }

    #[test]
    fn fill_array_items_with_enum() {
        let schema = json!({
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["admin", "user", "guest"]
                    }
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let tags = result.get("tags").unwrap().as_array().unwrap();
        for tag in tags {
            let value = tag.as_str().unwrap();
            assert!(
                ["admin", "user", "guest"].contains(&value),
                "Expected enum value, got: {}",
                value
            );
        }
    }

    #[test]
    fn format_takes_precedence_over_field_name() {
        // Even though field is named "status", format should win
        let schema = json!({
            "type": "object",
            "properties": {
                "status": {"type": "string", "format": "email"}
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let value = result.get("status").unwrap().as_str().unwrap();
        assert!(
            value.contains("@"),
            "Format should take precedence, got: {}",
            value
        );
    }

    #[test]
    fn enum_takes_precedence_over_format() {
        let schema = json!({
            "type": "object",
            "properties": {
                "email": {
                    "type": "string",
                    "format": "email",
                    "enum": ["test@example.com", "admin@example.com"]
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let value = result.get("email").unwrap().as_str().unwrap();
        assert!(
            ["test@example.com", "admin@example.com"].contains(&value),
            "Enum should take precedence, got: {}",
            value
        );
    }

    #[test]
    fn nullable_type_preserves_schema_hints() {
        let schema = json!({
            "type": "object",
            "properties": {
                "score": {
                    "type": ["integer", "null"],
                    "minimum": 0,
                    "maximum": 100
                }
            }
        });

        let mut filler = SchemaFiller::with_seed(42);
        let result = filler.fill(&schema);

        let value = result.get("score").unwrap().as_i64().unwrap();
        assert!(
            (0..=100).contains(&value),
            "Should respect min/max with nullable type, got: {}",
            value
        );
    }
}
