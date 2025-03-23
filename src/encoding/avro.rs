use std::str::{from_utf8, FromStr};

use anyhow::{anyhow, bail, Context, Ok};
use apache_avro::{
    from_avro_datum, schema::SchemaKind, to_avro_datum, types::Record, Decimal, Schema,
};
use mongodb::bson::{Decimal128, Document};

pub fn encode(mongo_doc: Document, schema: &Schema) -> anyhow::Result<Vec<u8>> {
    let mut record = Record::new(&schema).context("failed to create record")?;

    if let Schema::Record { ref fields, .. } = schema {
        for field in fields.iter() {
            let field_name = &field.name;

            let bson_val = mongo_doc.get(field_name).ok_or_else(|| {
                anyhow!("failed to find bson property '{}' for schema", &field_name)
            })?;

            let avro_val =
                Wrap::try_from(BsonWithSchema(bson_val.clone(), field.schema.clone()))?.0;
            record.put(field_name, avro_val);
        }
    } else {
        bail!(
            "expect a record raw schema. got: {}",
            schema.canonical_form()
        );
    }

    Ok(to_avro_datum(&schema, record)?)
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

fn avro_to_bson(avro_val: &AvroVal, field_name: &str) -> anyhow::Result<Bson> {
    match avro_val {
        AvroVal::Null => Ok(Bson::Null),
        AvroVal::Boolean(b) => Ok(Bson::Boolean(*b)),
        AvroVal::Int(i) => Ok(Bson::Int32(*i)),
        AvroVal::Long(l) => Ok(Bson::Int64(*l)),
        AvroVal::Float(f) => Ok(Bson::Double(f64::from(*f))),
        AvroVal::Double(d) => Ok(Bson::Double(*d)),
        AvroVal::String(s) => Ok(Bson::String(s.clone())),
        AvroVal::Decimal(d) => {
            let b = <Vec<u8>>::try_from(d)?;
            let dec_string = from_utf8(b.as_slice())?;
            Ok(Decimal128::from_str(dec_string)
                .context("failed to convert to decimal")?
                .into())
        }
        AvroVal::Array(arr) => {
            let mut bson_arr = Vec::new();
            for avro_v in arr.iter() {
                bson_arr.push(avro_to_bson(avro_v, field_name)?);
            }
            Ok(Bson::Array(bson_arr))
        }
        AvroVal::Record(fields) => {
            let mut doc = Document::new();
            for (field_name, avro_v) in fields.iter() {
                doc.insert(field_name.clone(), avro_to_bson(avro_v, field_name)?);
            }
            Ok(Bson::Document(doc))
        }
        AvroVal::Enum(_, symbol) => Ok(Bson::String(symbol.clone())),
        AvroVal::Union(_, inner) => avro_to_bson(inner, field_name),
        AvroVal::Bytes(_)
        | AvroVal::Fixed(_, _)
        | AvroVal::Date(_)
        | AvroVal::TimeMillis(_)
        | AvroVal::TimeMicros(_)
        | AvroVal::TimestampMillis(_)
        | AvroVal::TimestampMicros(_)
        | AvroVal::Duration(_)
        | AvroVal::Uuid(_)
        | AvroVal::Map(_) => bail!(
            "unsupported avro type for field '{}': {:?}",
            field_name,
            avro_val
        ),
    }
}

use apache_avro::types::Value as AvroVal;
use mongodb::bson::Bson;

struct BsonWithSchema(Bson, Schema);
struct Wrap(AvroVal);

impl TryFrom<BsonWithSchema> for Wrap {
    type Error = anyhow::Error;

    /// Converts Bson Value into Avro
    ///
    /// Mongo types reference: https://www.mongodb.com/docs/manual/reference/bson-types/
    /// Avro types reference: https://avro.apache.org/docs/1.11.1/specification/
    ///
    /// Supported bson->avro type conversion:
    ///     * bool       -> boolean
    ///     * double     -> double
    ///     * int32      -> int
    ///     * int64      -> long
    ///     * null       -> null
    ///     * string     -> string
    ///     * array      -> array
    ///     * object     -> record
    ///     * decimal128 -> bytes (logicalType: decimal)
    ///
    /// Additional supported avro types:
    ///     * union https://avro.apache.org/docs/1.11.1/specification/#unions
    ///     * enum  https://avro.apache.org/docs/1.11.1/specification/#enums
    ///
    fn try_from(val: BsonWithSchema) -> Result<Self, Self::Error> {
        let bson_val = val.0;
        let avro_schema = val.1;

        let get_string = |bson_val: Bson| -> Result<String, Self::Error> {
            Ok(bson_val
                .as_str()
                .ok_or_else(|| anyhow!("failed to convert bson to string: {}", bson_val))?
                .to_owned())
        };

        match avro_schema {
            Schema::Record {
                ref name,
                ref fields,
                ..
            } => {
                let bson_map = bson_val
                    .as_document()
                    .ok_or_else(|| anyhow!("failed to convert to document: {}", bson_val))?;

                let mut avro_rec = Vec::new();

                for field in fields {
                    let bson_v = bson_map.get(&field.name).ok_or_else(|| {
                        anyhow!(
                            "failed to obtain field '{}' from bson document. avro schema: {}",
                            field.name,
                            name.name,
                        )
                    })?;

                    let avro_v =
                        Self::try_from(BsonWithSchema(bson_v.clone(), field.schema.clone()))?.0;
                    avro_rec.push((field.name.clone(), avro_v));
                }

                Ok(Wrap(AvroVal::Record(avro_rec)))
            }
            Schema::Null => Ok(Wrap(AvroVal::Null)),
            Schema::Boolean => {
                let bool_val = bson_val
                    .as_bool()
                    .ok_or_else(|| anyhow!("failed to convert bson to boolean: {}", bson_val))?;

                Ok(Wrap(AvroVal::Boolean(bool_val)))
            }
            Schema::Int => {
                let res = bson_val
                    .as_i32()
                    .or_else(|| bson_val.as_i64().map(|v| v as i32))
                    .ok_or_else(|| anyhow!("failed to convert bson to int: {}", bson_val))?;

                Ok(Wrap(AvroVal::Int(res)))
            }
            Schema::Long => {
                Ok(Wrap(AvroVal::Long(bson_val.as_i64().ok_or_else(|| {
                    anyhow!("failed to convert bson to long: {}", bson_val)
                })?)))
            }
            Schema::Double => {
                Ok(Wrap(AvroVal::Double(bson_val.as_f64().ok_or_else(
                    || anyhow!("failed to convert bson to double: {}", bson_val),
                )?)))
            }
            Schema::String => Ok(Wrap(AvroVal::String(get_string(bson_val)?))),
            Schema::Array(array_schema) => {
                let bson_vec = bson_val
                    .as_array()
                    .ok_or_else(|| anyhow!("failed to convert bson to array: {}", bson_val))?;

                let mut avro_arr = Vec::new();
                for bson_v in bson_vec.iter().cloned() {
                    let avro_v = Self::try_from(BsonWithSchema(bson_v, *array_schema.clone()))?;
                    avro_arr.push(avro_v.0);
                }
                Ok(Wrap(AvroVal::Array(avro_arr)))
            }
            Schema::Decimal { .. } => {
                // https://www.mongodb.com/developer/products/mongodb/bson-data-types-decimal128/
                Ok(Wrap(AvroVal::Decimal(Decimal::from(bson_val.to_string()))))
            }
            Schema::Enum { name, symbols, .. } => {
                let item = get_string(bson_val)?;
                if let Some(i) = symbols.iter().position(|s| s.eq(&item)) {
                    Ok(Wrap(AvroVal::Enum(i as u32, item)))
                } else {
                    bail!(
                        "failed to convert bson to enum for avro field: '{}'",
                        name.name
                    );
                }
            }
            Schema::Union(union_schema) => {
                let union_variant_position = || -> anyhow::Result<u32> {
                    Ok(union_schema.variants().iter()
                    .position(|schema| schema.ne(&Schema::Null))
                    .ok_or_else(||
                        anyhow!(
                            "failed to find a non-null schema variant in avro definition while converting from bson: {}",
                            bson_val
                        )
                    )? as u32)
                };

                match bson_val {
                    Bson::Null if !union_schema.is_nullable() => {
                        bail!("got a null bson value for an non-nullable avro union schema");
                    }
                    Bson::Null => Ok(Wrap(AvroVal::Union(0, Box::new(AvroVal::Null)))),
                    _ => {
                        let pos = union_variant_position()?;
                        let union_variant =
                            unsafe { union_schema.variants().get_unchecked(pos as usize) };
                        let converted_avro =
                            Self::try_from(BsonWithSchema(bson_val, union_variant.clone()))?.0;
                        Ok(Wrap(AvroVal::Union(pos, Box::new(converted_avro))))
                    }
                }
            }
            Schema::Float => bail!(
                "avro float (32-bit) is not supported, use double (64-bit) instead. bson value: {}",
                bson_val
            ),
            Schema::Map(_)
            | Schema::Fixed { .. }
            | Schema::Uuid
            | Schema::Date
            | Schema::TimeMillis
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::Duration
            | Schema::Bytes
            | Schema::Ref { .. } => bail!(
                "avro type '{:?}' is not implemented",
                SchemaKind::from(avro_schema)
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::encoding::{
        avro::{decode, encode},
        json_to_bson_doc,
    };
    use anyhow::{bail, Context};
    use apache_avro::{from_avro_datum, Schema};
    use mongodb::bson::{doc, Decimal128};

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

        let document = json_to_bson_doc(json_payload).unwrap();
        let avro_schema = Schema::parse_str(raw_schema).unwrap();

        let b = encode(document, &avro_schema).unwrap();
        validate_avro_encoded(b, raw_schema).unwrap();
    }

    fn validate_avro_encoded(avro_b: Vec<u8>, raw_schema: &str) -> anyhow::Result<()> {
        let compiled_schema = Schema::parse_str(raw_schema)
            .context("failed to compile schema from a raw definition")?;

        let mut reader = avro_b.as_slice();
        let avro_value = from_avro_datum(&compiled_schema, &mut reader, None)?;
        if !avro_value.validate(&compiled_schema) {
            bail!("failed to validate schema");
        }

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
