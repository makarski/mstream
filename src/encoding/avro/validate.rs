use anyhow::bail;
use apache_avro::{Schema, from_avro_datum};

pub fn validate(avro_b: Vec<u8>, schema: &Schema) -> anyhow::Result<Vec<u8>> {
    let mut reader = avro_b.as_slice();
    let avro_value = from_avro_datum(schema, &mut reader, None)?;
    if !avro_value.validate(schema) {
        bail!("failed to validate schema");
    }

    Ok(avro_b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::to_avro_datum;
    use apache_avro::types::Record;

    fn simple_schema() -> Schema {
        Schema::parse_str(
            r#"{
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                ]
            }"#,
        )
        .unwrap()
    }

    fn encode_record(schema: &Schema, name: &str, age: i32) -> Vec<u8> {
        let mut record = Record::new(schema).unwrap();
        record.put("name", name);
        record.put("age", age);
        to_avro_datum(schema, record).unwrap()
    }

    // =========================================================================
    // Group 1: Valid data passes through unchanged
    // =========================================================================

    #[test]
    fn validate_valid_data_returns_unchanged() {
        let schema = simple_schema();
        let avro_bytes = encode_record(&schema, "Alice", 30);
        let original = avro_bytes.clone();

        let result = validate(avro_bytes, &schema).unwrap();

        assert_eq!(result, original);
    }

    #[test]
    fn validate_multiple_valid_records() {
        let schema = simple_schema();

        // Validate several different valid records
        for (name, age) in [("Bob", 25), ("Charlie", 0), ("Dave", -5), ("", 999)] {
            let avro_bytes = encode_record(&schema, name, age);
            let result = validate(avro_bytes.clone(), &schema);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), avro_bytes);
        }
    }

    // =========================================================================
    // Group 2: Invalid data returns error
    // =========================================================================

    #[test]
    fn validate_error_malformed_bytes() {
        let schema = simple_schema();
        // Use bytes that will fail to parse as a valid Avro record
        // (string length prefix indicating impossibly long string)
        let invalid_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F];

        let result = validate(invalid_bytes, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn validate_error_empty_bytes() {
        let schema = simple_schema();
        let empty_bytes = vec![];

        let result = validate(empty_bytes, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn validate_error_truncated_data() {
        let schema = simple_schema();
        let mut avro_bytes = encode_record(&schema, "Alice", 30);
        // Truncate the data
        avro_bytes.truncate(avro_bytes.len() / 2);

        let result = validate(avro_bytes, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn validate_error_wrong_schema() {
        // Encode with one schema
        let schema1 = simple_schema();
        let avro_bytes = encode_record(&schema1, "Alice", 30);

        // Validate with a different schema
        let schema2 = Schema::parse_str(
            r#"{
                "type": "record",
                "name": "DifferentRecord",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "value", "type": "double"}
                ]
            }"#,
        )
        .unwrap();

        let result = validate(avro_bytes, &schema2);

        // May fail during parsing or validation depending on byte interpretation
        assert!(result.is_err());
    }

    // =========================================================================
    // Group 3: Edge cases
    // =========================================================================

    #[test]
    fn validate_record_with_optional_field_null() {
        let schema = Schema::parse_str(
            r#"{
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "nickname", "type": ["null", "string"], "default": null}
                ]
            }"#,
        )
        .unwrap();

        let mut record = Record::new(&schema).unwrap();
        record.put("name", "Alice");
        record.put(
            "nickname",
            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
        );
        let avro_bytes = to_avro_datum(&schema, record).unwrap();

        let result = validate(avro_bytes, &schema);

        assert!(result.is_ok());
    }

    #[test]
    fn validate_record_with_optional_field_present() {
        let schema = Schema::parse_str(
            r#"{
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "nickname", "type": ["null", "string"], "default": null}
                ]
            }"#,
        )
        .unwrap();

        let mut record = Record::new(&schema).unwrap();
        record.put("name", "Alice");
        record.put(
            "nickname",
            apache_avro::types::Value::Union(
                1,
                Box::new(apache_avro::types::Value::String("Ali".to_string())),
            ),
        );
        let avro_bytes = to_avro_datum(&schema, record).unwrap();

        let result = validate(avro_bytes, &schema);

        assert!(result.is_ok());
    }

    #[test]
    fn validate_nested_record() {
        let schema = Schema::parse_str(
            r#"{
                "type": "record",
                "name": "Outer",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "inner", "type": {
                        "type": "record",
                        "name": "Inner",
                        "fields": [
                            {"name": "value", "type": "string"}
                        ]
                    }}
                ]
            }"#,
        )
        .unwrap();

        // Build nested record
        let inner_schema = Schema::parse_str(
            r#"{
                "type": "record",
                "name": "Inner",
                "fields": [{"name": "value", "type": "string"}]
            }"#,
        )
        .unwrap();

        let mut inner = Record::new(&inner_schema).unwrap();
        inner.put("value", "nested data");

        let mut outer = Record::new(&schema).unwrap();
        outer.put("id", 42i32);
        outer.put("inner", inner);

        let avro_bytes = to_avro_datum(&schema, outer).unwrap();

        let result = validate(avro_bytes, &schema);

        assert!(result.is_ok());
    }

    #[test]
    fn validate_record_with_array() {
        let schema = Schema::parse_str(
            r#"{
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "tags", "type": {"type": "array", "items": "string"}}
                ]
            }"#,
        )
        .unwrap();

        let mut record = Record::new(&schema).unwrap();
        record.put(
            "tags",
            apache_avro::types::Value::Array(vec![
                apache_avro::types::Value::String("tag1".to_string()),
                apache_avro::types::Value::String("tag2".to_string()),
            ]),
        );
        let avro_bytes = to_avro_datum(&schema, record).unwrap();

        let result = validate(avro_bytes, &schema);

        assert!(result.is_ok());
    }

    #[test]
    fn validate_record_with_empty_array() {
        let schema = Schema::parse_str(
            r#"{
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "tags", "type": {"type": "array", "items": "string"}}
                ]
            }"#,
        )
        .unwrap();

        let mut record = Record::new(&schema).unwrap();
        record.put("tags", apache_avro::types::Value::Array(vec![]));
        let avro_bytes = to_avro_datum(&schema, record).unwrap();

        let result = validate(avro_bytes, &schema);

        assert!(result.is_ok());
    }
}
