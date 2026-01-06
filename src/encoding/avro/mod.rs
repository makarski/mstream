use anyhow::{Context, Ok, anyhow, bail};
use apache_avro::types::Value as AvroVal;
use apache_avro::{Schema, from_avro_datum, to_avro_datum, types::Record};
use mongodb::bson::Document;

use crate::encoding::avro::types::{AvroValue, BsonWithSchema, avro_to_bson};

pub(crate) mod types;
pub mod validate;

pub fn encode(mongo_doc: Document, schema: &Schema) -> anyhow::Result<Vec<u8>> {
    let record = transform_document_to_avro_record(mongo_doc, schema)?;
    Ok(to_avro_datum(&schema, record)?)
}

fn transform_document_to_avro_record(doc: Document, schema: &Schema) -> anyhow::Result<Record<'_>> {
    match schema {
        Schema::Record { fields, .. } => {
            let mut record = Record::new(&schema).context("failed to create record")?;
            for field in fields.iter() {
                let field_name = &field.name;

                let bson_val = doc.get(field_name).ok_or_else(|| {
                    anyhow!("failed to find bson property '{}' for schema", &field_name)
                })?;

                let avro_val =
                    AvroValue::try_from(BsonWithSchema(bson_val.clone(), field.schema.clone()))?.0;
                record.put(field_name, avro_val);
            }
            Ok(record)
        }
        _ => bail!(
            "expect a record raw schema. got: {}",
            schema.canonical_form()
        ),
    }
}

pub fn decode(avro_bytes: &[u8], schema: &Schema) -> anyhow::Result<Document> {
    let mut reader = avro_bytes;
    let avro_value =
        from_avro_datum(&schema, &mut reader, None).context("failed to parse avro datum")?;

    if let Schema::Record { .. } = &schema {
        let mut doc = Document::new();

        match &avro_value {
            AvroVal::Record(fields_with_values) => {
                for (field_name, avro_val) in fields_with_values {
                    let bson_val = avro_to_bson(avro_val, field_name)?;
                    doc.insert(field_name, bson_val);
                }
                Ok(doc)
            }
            _ => bail!("expect a record avro value. got: {:?}", avro_value),
        }
    } else {
        bail!("expect a record schema. got: {}", schema.canonical_form());
    }
}

#[cfg(test)]
mod tests {
    use crate::encoding::{
        avro::{decode, encode, validate::validate},
        xson::JsonBytes,
    };
    use anyhow::Context;
    use apache_avro::Schema;
    use mongodb::bson::{Decimal128, Document, doc};

    #[test]
    fn encode_with_valid_schema_and_valid_payload() -> anyhow::Result<()> {
        let raw_schema = r###"
        {
            "type" : "record",
            "name" : "Employee",
            "fields" : [
                    { "name": "nickname", "type": ["null", "string"], "default": null },
                    { "name": "nickname2", "type": ["null", "string"], "default": null },
                    { "name": "name" , "type" : "string" },
                    { "name": "age" , "type" : "int" },
                    { "name": "gender", "type": "enum", "symbols": ["MALE", "FEMALE", "OTHER"]},
                    { "name": "teams", "type": "array", "items": "string" },
                    { "name": "performance_grades", "type": "array", "items": "int" },
                    { "name": "project", "type": {
                        "type": "record",
                        "name": "EmployeeProject",
                        "fields": [
                            { "name": "title", "type": "string" },
                            { "name": "rating", "type": "double" }
                        ]
                    }},
                    { "name": "score", "type": "bytes", "logicalType": "decimal", "scale": 2, "precision": 4 },
                    { "name": "is_active", "type": "boolean" },
                    { "name": "long_number", "type": "long" }
                ]
            }
        "###;

        let employee_score: &[u8; 16] = b"12345678.9876543";

        let mongodb_document = doc! {
            "name": "Jon Doe",
            "age": 32,
            "gender": "OTHER",
            "teams": ["team A", "team B", "team C"],
            "performance_grades": [3, 3, 5],
            "project": doc! {
                "title": "Awesome Project",
                "rating": 92.5_f64
            },
            "score": Decimal128::from_bytes(*employee_score),
            "nickname": null,
            "nickname2": "ABC",
            "is_active": true,
            "long_number": 100500_i64,
            "additional_field": "foobar",  // will be omitted
        };

        let avro_schema = Schema::parse_str(raw_schema)?;
        let result = encode(mongodb_document, &avro_schema)?;
        validate_avro_encoded(result, raw_schema)
    }

    #[test]
    fn encode_with_invalid_schema() {
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "Employee"
            }
        "###;
        let result = Schema::parse_str(raw_schema);
        assert!(result.is_err());
    }

    #[test]
    fn encode_with_valid_schema_but_invalid_payload() {
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "Employee",
                "fields" : [
                    { "name" : "name" , "type" : "string" },
                    { "name" : "age" , "type" : "int" }
                ]
            }
        "###;
        let mongodb_document = doc! {"first_name": "Jon", "last_name": "Doe"};
        let avro_schema = Schema::parse_str(raw_schema).unwrap();
        let result = encode(mongodb_document, &avro_schema);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("failed to find bson property"));
    }

    #[test]
    fn encode_with_non_record_schema() {
        let string_schema = Schema::String;
        let mongodb_document = doc! {"name": "test"};
        let result = encode(mongodb_document, &string_schema);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("expect a record raw schema"));
    }

    #[test]
    fn encode_from_raw_json_format() {
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "Employee",
                "fields" : [
                    { "name" : "name" , "type" : "string" },
                    { "name" : "age" , "type" : "int" }
                ]
            }
        "###;

        let json_payload = br###"
            {
                "name": "Jon Doe",
                "age": 32
            }
        "###;

        let document: Document = JsonBytes(json_payload.to_vec()).try_into().unwrap();
        let avro_schema = Schema::parse_str(raw_schema).unwrap();

        let b = encode(document, &avro_schema).unwrap();
        validate_avro_encoded(b, raw_schema).unwrap();
    }

    fn validate_avro_encoded(avro_b: Vec<u8>, raw_schema: &str) -> anyhow::Result<()> {
        let compiled_schema = Schema::parse_str(raw_schema)
            .context("failed to compile schema from a raw definition")?;

        validate(avro_b, &compiled_schema)?;
        Ok(())
    }

    #[test]
    fn test_encode_decode_roundtrip() -> anyhow::Result<()> {
        let raw_schema = r###"
        {
            "type" : "record",
            "name" : "Employee",
            "fields" : [
                    { "name": "nickname", "type": ["null", "string"], "default": null },
                    { "name": "name" , "type" : "string" },
                    { "name": "age" , "type" : "int" },
                    { "name": "gender", "type": "enum", "symbols": ["MALE", "FEMALE", "OTHER"]},
                    { "name": "teams", "type": "array", "items": "string" },
                    { "name": "project", "type": {
                        "type": "record",
                        "name": "EmployeeProject",
                        "fields": [
                            { "name": "title", "type": "string" },
                            { "name": "rating", "type": "double" }
                        ]
                    }},
                    { "name": "is_active", "type": "boolean" },
                    { "name": "long_number", "type": "long" }
                ]
            }
        "###;

        let mongodb_document = doc! {
            "name": "Jon Doe",
            "age": 32,
            "gender": "OTHER",
            "teams": ["team A", "team B", "team C"],
            "project": doc! {
                "title": "Awesome Project",
                "rating": 92.5_f64
            },
            "nickname": null,
            "is_active": true,
            "long_number": 100500_i64,
        };

        let avro_schema = Schema::parse_str(raw_schema)?;

        // Encode BSON to Avro
        let avro_bytes = encode(mongodb_document.clone(), &avro_schema)?;

        // Decode Avro back to BSON
        let decoded_doc = decode(&avro_bytes, &avro_schema)?;

        // Compare original and decoded documents
        assert_eq!(mongodb_document.get("name"), decoded_doc.get("name"));
        assert_eq!(mongodb_document.get("age"), decoded_doc.get("age"));
        assert_eq!(mongodb_document.get("gender"), decoded_doc.get("gender"));
        assert_eq!(mongodb_document.get("teams"), decoded_doc.get("teams"));
        assert_eq!(
            mongodb_document.get("is_active"),
            decoded_doc.get("is_active")
        );
        assert_eq!(
            mongodb_document.get("long_number"),
            decoded_doc.get("long_number")
        );

        // For nested documents, compare fields
        let orig_project = mongodb_document.get_document("project")?;
        let decoded_project = decoded_doc.get_document("project")?;
        assert_eq!(orig_project.get("title"), decoded_project.get("title"));
        assert_eq!(orig_project.get("rating"), decoded_project.get("rating"));

        Ok(())
    }

    #[test]
    fn decode_with_invalid_bytes() {
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "Employee",
                "fields" : [
                    { "name" : "name" , "type" : "string" },
                    { "name" : "age" , "type" : "int" }
                ]
            }
        "###;
        let avro_schema = Schema::parse_str(raw_schema).unwrap();
        let invalid_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF];
        let result = decode(&invalid_bytes, &avro_schema);
        assert!(result.is_err());
    }

    #[test]
    fn decode_with_empty_bytes() {
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "Employee",
                "fields" : [
                    { "name" : "name" , "type" : "string" }
                ]
            }
        "###;
        let avro_schema = Schema::parse_str(raw_schema).unwrap();
        let result = decode(&[], &avro_schema);
        assert!(result.is_err());
    }

    #[test]
    fn decode_with_non_record_schema() {
        let string_schema = Schema::String;
        let bytes = vec![0x00];
        let result = decode(&bytes, &string_schema);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("expect a record schema"));
    }

    #[test]
    fn encode_decode_with_empty_arrays() -> anyhow::Result<()> {
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "TestRecord",
                "fields" : [
                    { "name": "items", "type": {"type": "array", "items": "string"} }
                ]
            }
        "###;
        let mongodb_document = doc! { "items": Vec::<String>::new() };
        let avro_schema = Schema::parse_str(raw_schema)?;

        let avro_bytes = encode(mongodb_document.clone(), &avro_schema)?;
        let decoded_doc = decode(&avro_bytes, &avro_schema)?;

        assert_eq!(decoded_doc.get_array("items")?.len(), 0);
        Ok(())
    }

    #[test]
    fn encode_decode_with_nullable_fields() -> anyhow::Result<()> {
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "TestRecord",
                "fields" : [
                    { "name": "required", "type": "string" },
                    { "name": "optional", "type": ["null", "string"], "default": null }
                ]
            }
        "###;
        let avro_schema = Schema::parse_str(raw_schema)?;

        // Test with null value
        let doc_with_null = doc! { "required": "test", "optional": null };
        let avro_bytes = encode(doc_with_null, &avro_schema)?;
        let decoded = decode(&avro_bytes, &avro_schema)?;
        assert!(decoded.get("optional").unwrap().as_null().is_some());

        // Test with actual value
        let doc_with_value = doc! { "required": "test", "optional": "value" };
        let avro_bytes = encode(doc_with_value, &avro_schema)?;
        let decoded = decode(&avro_bytes, &avro_schema)?;
        assert_eq!(decoded.get_str("optional")?, "value");

        Ok(())
    }
}
