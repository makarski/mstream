use anyhow::{anyhow, bail, Context, Ok};
use apache_avro::types::Value as AvroVal;
use apache_avro::{from_avro_datum, to_avro_datum, types::Record, Schema};
use mongodb::bson::Document;

use crate::encoding::avro::types::{avro_to_bson, AvroValue, BsonWithSchema};

pub(crate) mod types;
pub mod validate;

pub fn encode_many(docs: Vec<Document>, item_schema: &Schema) -> anyhow::Result<Vec<u8>> {
    let mut records = Vec::with_capacity(docs.len());
    let batch_schema = Schema::Array(Box::new(item_schema.clone()));

    for doc in docs.into_iter() {
        let record = transform_document_to_avro_record(doc, &item_schema)?;
        let avro_val: AvroVal = record.into();
        records.push(avro_val);
    }

    Ok(to_avro_datum(&batch_schema, AvroVal::Array(records))?)
}

pub fn encode(mongo_doc: Document, schema: &Schema) -> anyhow::Result<Vec<u8>> {
    let record = transform_document_to_avro_record(mongo_doc, schema)?;
    Ok(to_avro_datum(&schema, record)?)
}

fn transform_document_to_avro_record(doc: Document, schema: &Schema) -> anyhow::Result<Record> {
    match schema {
        Schema::Record { ref fields, .. } => {
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
    use mongodb::bson::{doc, Decimal128, Document};

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
    #[should_panic(expected = "Failed to parse schema from JSON")]
    fn encode_with_invalid_schema() {
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "Employee"
            }
        "###;
        Schema::parse_str(raw_schema).expect("Failed to parse schema from JSON");
    }

    #[test]
    #[should_panic(expected = "failed to find bson property 'name' for schema")]
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
        encode(mongodb_document, &avro_schema).unwrap();
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
}
