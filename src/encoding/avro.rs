use anyhow::{anyhow, bail, Context, Ok};
use apache_avro::{schema::SchemaKind, to_avro_datum, types::Record, Decimal, Schema};
use mongodb::bson::Document;

pub fn encode2(mongo_doc: Document, raw_schema: &str) -> anyhow::Result<Vec<u8>> {
    let schema = Schema::parse_str(raw_schema)?;
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
        bail!("expect a record raw schema. got: {}", raw_schema);
    }

    Ok(to_avro_datum(&schema, record)?)
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
                Ok(Wrap(AvroVal::Int(bson_val.as_i32().ok_or_else(|| {
                    anyhow!("failed to convert bson to int: {}", bson_val)
                })?)))
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
    use crate::encoding::avro::encode2;
    use anyhow::{bail, Context};
    use apache_avro::{from_avro_datum, Schema};
    use mongodb::bson::{doc, Decimal128};

    #[test]
    fn encode2_with_valid_schema_and_valid_payload() -> anyhow::Result<()> {
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

        let result = encode2(mongodb_document, raw_schema)?;
        validate_avro_encoded(result, raw_schema)
    }

    #[test]
    #[should_panic(expected = "Failed to parse schema from JSON")]
    fn encode2_with_invalid_schema() {
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "Employee",
            }
        "###;
        let mongodb_document = doc! {"name": "Jon Doe", "age": 32};
        encode2(mongodb_document, raw_schema).unwrap();
    }

    #[test]
    #[should_panic(expected = "failed to find bson property 'name' for schema")]
    fn encode2_with_valid_schema_but_invalid_payload() {
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
        encode2(mongodb_document, raw_schema).unwrap();
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
}
