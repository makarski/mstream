use std::str::{FromStr, from_utf8};

use anyhow::{Context, Ok, anyhow, bail};
use apache_avro::{Decimal, Schema, schema::SchemaKind, types::Value as AvroTypeValue};
use mongodb::bson::{Bson, Decimal128, Document};

use crate::encoding::xson::{BsonBytes, BsonBytesWithSchema, JsonBytes, JsonBytesWithSchema};
use crate::schema::Schema as MstreamSchema;

// --- AvroBytes ---

pub(crate) struct AvroBytes(pub(crate) Vec<u8>);

impl TryFrom<JsonBytesWithSchema<'_>> for AvroBytes {
    type Error = anyhow::Error;

    /// Converts JSON document raw bytes with schema into Avro bytes.
    ///
    /// The JSON document is expected to be in a format that matches the provided Avro schema.
    fn try_from(value: JsonBytesWithSchema) -> Result<Self, Self::Error> {
        let avro_schema = value.schema.try_as_avro()?;

        JsonBytes(value.data)
            .try_into()
            .and_then(|doc: Document| super::encode(doc, avro_schema))
            .map(|avro_bytes| AvroBytes(avro_bytes))
    }
}

impl TryFrom<BsonBytesWithSchema<'_>> for AvroBytes {
    type Error = anyhow::Error;

    /// Converts BSON document raw bytes with schema into Avro bytes.
    ///
    /// The BSON document is expected to be in a format that matches the provided Avro schema.
    fn try_from(value: BsonBytesWithSchema) -> Result<Self, Self::Error> {
        let avro_schema = value.schema.try_as_avro()?;

        BsonBytes(value.data)
            .try_into()
            .and_then(|doc: Document| super::encode(doc, avro_schema))
            .map(|avro_bytes| AvroBytes(avro_bytes))
    }
}

// --- AvroBatchBytesWithSchema ---

pub struct AvroBatchBytesWithSchema<'a, I> {
    data: I,
    schema: &'a MstreamSchema,
}

impl<'a, I> AvroBatchBytesWithSchema<'a, I> {
    pub fn new(data: I, schema: &'a MstreamSchema) -> Self {
        AvroBatchBytesWithSchema { data, schema }
    }
}

impl<I> TryFrom<AvroBatchBytesWithSchema<'_, I>> for Vec<Document>
where
    I: IntoIterator<Item = Vec<u8>>,
{
    type Error = anyhow::Error;

    fn try_from(value: AvroBatchBytesWithSchema<I>) -> Result<Self, Self::Error> {
        let avro_schema = value.schema.try_as_avro()?;
        let iter = value.data.into_iter();
        let (lower, _) = iter.size_hint();
        let mut docs = Vec::with_capacity(lower);
        for avro_b in iter {
            let bson_doc = super::decode(&avro_b, avro_schema)
                .context("failed to decode avro bytes into bson document")?;

            docs.push(bson_doc);
        }

        Ok(docs)
    }
}

// --- BsonWithSchema and AvroValue Wrapper ---

pub(crate) struct BsonWithSchema(pub(crate) Bson, pub(crate) Schema);
pub(crate) struct AvroValue(pub(crate) AvroTypeValue);

impl TryFrom<BsonWithSchema> for AvroValue {
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

                Ok(AvroValue(AvroTypeValue::Record(avro_rec)))
            }
            Schema::Null => Ok(AvroValue(AvroTypeValue::Null)),
            Schema::Boolean => {
                let bool_val = bson_val
                    .as_bool()
                    .ok_or_else(|| anyhow!("failed to convert bson to boolean: {}", bson_val))?;

                Ok(AvroValue(AvroTypeValue::Boolean(bool_val)))
            }
            Schema::Int => {
                let res = bson_val
                    .as_i32()
                    .or_else(|| bson_val.as_i64().map(|v| v as i32))
                    .ok_or_else(|| anyhow!("failed to convert bson to int: {}", bson_val))?;

                Ok(AvroValue(AvroTypeValue::Int(res)))
            }
            Schema::Long => Ok(AvroValue(AvroTypeValue::Long(
                bson_val
                    .as_i64()
                    .ok_or_else(|| anyhow!("failed to convert bson to long: {}", bson_val))?,
            ))),
            Schema::Double => Ok(AvroValue(AvroTypeValue::Double(
                bson_val
                    .as_f64()
                    .ok_or_else(|| anyhow!("failed to convert bson to double: {}", bson_val))?,
            ))),
            Schema::String => Ok(AvroValue(AvroTypeValue::String(get_string(bson_val)?))),
            Schema::Array(array_schema) => {
                let bson_vec = bson_val
                    .as_array()
                    .ok_or_else(|| anyhow!("failed to convert bson to array: {}", bson_val))?;

                let mut avro_arr = Vec::new();
                for bson_v in bson_vec.iter().cloned() {
                    let avro_v = Self::try_from(BsonWithSchema(bson_v, *array_schema.clone()))?;
                    avro_arr.push(avro_v.0);
                }
                Ok(AvroValue(AvroTypeValue::Array(avro_arr)))
            }
            Schema::Decimal { .. } => {
                // https://www.mongodb.com/developer/products/mongodb/bson-data-types-decimal128/
                Ok(AvroValue(AvroTypeValue::Decimal(Decimal::from(
                    bson_val.to_string(),
                ))))
            }
            Schema::Enum { name, symbols, .. } => {
                let item = get_string(bson_val)?;
                if let Some(i) = symbols.iter().position(|s| s.eq(&item)) {
                    Ok(AvroValue(AvroTypeValue::Enum(i as u32, item)))
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
                    Bson::Null => Ok(AvroValue(AvroTypeValue::Union(
                        0,
                        Box::new(AvroTypeValue::Null),
                    ))),
                    _ => {
                        let pos = union_variant_position()?;
                        let union_variant =
                            unsafe { union_schema.variants().get_unchecked(pos as usize) };
                        let converted_avro =
                            Self::try_from(BsonWithSchema(bson_val, union_variant.clone()))?.0;
                        Ok(AvroValue(AvroTypeValue::Union(
                            pos,
                            Box::new(converted_avro),
                        )))
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

pub(crate) fn avro_to_bson(avro_val: &AvroTypeValue, field_name: &str) -> anyhow::Result<Bson> {
    match avro_val {
        AvroTypeValue::Null => Ok(Bson::Null),
        AvroTypeValue::Boolean(b) => Ok(Bson::Boolean(*b)),
        AvroTypeValue::Int(i) => Ok(Bson::Int32(*i)),
        AvroTypeValue::Long(l) => Ok(Bson::Int64(*l)),
        AvroTypeValue::Float(f) => Ok(Bson::Double(f64::from(*f))),
        AvroTypeValue::Double(d) => Ok(Bson::Double(*d)),
        AvroTypeValue::String(s) => Ok(Bson::String(s.clone())),
        AvroTypeValue::Decimal(d) => {
            let b = <Vec<u8>>::try_from(d)?;
            let dec_string = from_utf8(b.as_slice())?;
            Ok(Decimal128::from_str(dec_string)
                .context("failed to convert to decimal")?
                .into())
        }
        AvroTypeValue::Array(arr) => {
            let mut bson_arr = Vec::new();
            for avro_v in arr.iter() {
                bson_arr.push(avro_to_bson(avro_v, field_name)?);
            }
            Ok(Bson::Array(bson_arr))
        }
        AvroTypeValue::Record(fields) => {
            let mut doc = Document::new();
            for (field_name, avro_v) in fields.iter() {
                doc.insert(field_name.clone(), avro_to_bson(avro_v, field_name)?);
            }
            Ok(Bson::Document(doc))
        }
        AvroTypeValue::Enum(_, symbol) => Ok(Bson::String(symbol.clone())),
        AvroTypeValue::Union(_, inner) => avro_to_bson(inner, field_name),
        AvroTypeValue::Bytes(_)
        | AvroTypeValue::Fixed(_, _)
        | AvroTypeValue::Date(_)
        | AvroTypeValue::TimeMillis(_)
        | AvroTypeValue::TimeMicros(_)
        | AvroTypeValue::TimestampMillis(_)
        | AvroTypeValue::TimestampMicros(_)
        | AvroTypeValue::Duration(_)
        | AvroTypeValue::Uuid(_)
        | AvroTypeValue::Map(_) => bail!(
            "unsupported avro type for field '{}': {:?}",
            field_name,
            avro_val
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value as AvroTypeValue;
    use mongodb::bson::{Bson, doc};

    // =========================================================================
    // Helper functions
    // =========================================================================

    fn string_schema() -> Schema {
        Schema::String
    }

    fn int_schema() -> Schema {
        Schema::Int
    }

    fn long_schema() -> Schema {
        Schema::Long
    }

    fn double_schema() -> Schema {
        Schema::Double
    }

    fn boolean_schema() -> Schema {
        Schema::Boolean
    }

    fn null_schema() -> Schema {
        Schema::Null
    }

    fn array_of_strings_schema() -> Schema {
        Schema::Array(Box::new(Schema::String))
    }

    fn array_of_ints_schema() -> Schema {
        Schema::Array(Box::new(Schema::Int))
    }

    fn enum_schema() -> Schema {
        Schema::parse_str(
            r#"{"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}"#,
        )
        .unwrap()
    }

    fn nullable_string_schema() -> Schema {
        Schema::parse_str(r#"["null", "string"]"#).unwrap()
    }

    fn simple_record_schema() -> Schema {
        Schema::parse_str(
            r#"{
                "type": "record",
                "name": "Person",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                ]
            }"#,
        )
        .unwrap()
    }

    fn decimal_schema() -> Schema {
        Schema::parse_str(
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}"#,
        )
        .unwrap()
    }

    // =========================================================================
    // Group 1: BsonWithSchema -> AvroValue (primitive types)
    // =========================================================================

    #[test]
    fn bson_to_avro_null() {
        let result = AvroValue::try_from(BsonWithSchema(Bson::Null, null_schema())).unwrap();
        assert_eq!(result.0, AvroTypeValue::Null);
    }

    #[test]
    fn bson_to_avro_boolean_true() {
        let result =
            AvroValue::try_from(BsonWithSchema(Bson::Boolean(true), boolean_schema())).unwrap();
        assert_eq!(result.0, AvroTypeValue::Boolean(true));
    }

    #[test]
    fn bson_to_avro_boolean_false() {
        let result =
            AvroValue::try_from(BsonWithSchema(Bson::Boolean(false), boolean_schema())).unwrap();
        assert_eq!(result.0, AvroTypeValue::Boolean(false));
    }

    #[test]
    fn bson_to_avro_int() {
        let result = AvroValue::try_from(BsonWithSchema(Bson::Int32(42), int_schema())).unwrap();
        assert_eq!(result.0, AvroTypeValue::Int(42));
    }

    #[test]
    fn bson_to_avro_int_negative() {
        let result = AvroValue::try_from(BsonWithSchema(Bson::Int32(-100), int_schema())).unwrap();
        assert_eq!(result.0, AvroTypeValue::Int(-100));
    }

    #[test]
    fn bson_to_avro_int_from_i64() {
        // i64 that fits in i32 should convert
        let result = AvroValue::try_from(BsonWithSchema(Bson::Int64(100), int_schema())).unwrap();
        assert_eq!(result.0, AvroTypeValue::Int(100));
    }

    #[test]
    fn bson_to_avro_long() {
        let result =
            AvroValue::try_from(BsonWithSchema(Bson::Int64(9999999999i64), long_schema())).unwrap();
        assert_eq!(result.0, AvroTypeValue::Long(9999999999i64));
    }

    #[test]
    fn bson_to_avro_double() {
        let result =
            AvroValue::try_from(BsonWithSchema(Bson::Double(3.14159), double_schema())).unwrap();
        assert_eq!(result.0, AvroTypeValue::Double(3.14159));
    }

    #[test]
    fn bson_to_avro_string() {
        let result = AvroValue::try_from(BsonWithSchema(
            Bson::String("hello".to_string()),
            string_schema(),
        ))
        .unwrap();
        assert_eq!(result.0, AvroTypeValue::String("hello".to_string()));
    }

    #[test]
    fn bson_to_avro_string_empty() {
        let result = AvroValue::try_from(BsonWithSchema(
            Bson::String("".to_string()),
            string_schema(),
        ))
        .unwrap();
        assert_eq!(result.0, AvroTypeValue::String("".to_string()));
    }

    // =========================================================================
    // Group 2: BsonWithSchema -> AvroValue (complex types)
    // =========================================================================

    #[test]
    fn bson_to_avro_array_of_strings() {
        let bson_arr = Bson::Array(vec![
            Bson::String("a".to_string()),
            Bson::String("b".to_string()),
        ]);
        let result =
            AvroValue::try_from(BsonWithSchema(bson_arr, array_of_strings_schema())).unwrap();

        if let AvroTypeValue::Array(arr) = result.0 {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], AvroTypeValue::String("a".to_string()));
            assert_eq!(arr[1], AvroTypeValue::String("b".to_string()));
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn bson_to_avro_array_of_ints() {
        let bson_arr = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
        let result = AvroValue::try_from(BsonWithSchema(bson_arr, array_of_ints_schema())).unwrap();

        if let AvroTypeValue::Array(arr) = result.0 {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], AvroTypeValue::Int(1));
            assert_eq!(arr[1], AvroTypeValue::Int(2));
            assert_eq!(arr[2], AvroTypeValue::Int(3));
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn bson_to_avro_array_empty() {
        let bson_arr = Bson::Array(vec![]);
        let result =
            AvroValue::try_from(BsonWithSchema(bson_arr, array_of_strings_schema())).unwrap();

        if let AvroTypeValue::Array(arr) = result.0 {
            assert!(arr.is_empty());
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn bson_to_avro_enum() {
        let result = AvroValue::try_from(BsonWithSchema(
            Bson::String("GREEN".to_string()),
            enum_schema(),
        ))
        .unwrap();

        if let AvroTypeValue::Enum(idx, symbol) = result.0 {
            assert_eq!(idx, 1); // GREEN is at index 1
            assert_eq!(symbol, "GREEN");
        } else {
            panic!("expected Enum");
        }
    }

    #[test]
    fn bson_to_avro_enum_first() {
        let result = AvroValue::try_from(BsonWithSchema(
            Bson::String("RED".to_string()),
            enum_schema(),
        ))
        .unwrap();

        if let AvroTypeValue::Enum(idx, symbol) = result.0 {
            assert_eq!(idx, 0);
            assert_eq!(symbol, "RED");
        } else {
            panic!("expected Enum");
        }
    }

    #[test]
    fn bson_to_avro_union_null() {
        let result =
            AvroValue::try_from(BsonWithSchema(Bson::Null, nullable_string_schema())).unwrap();

        if let AvroTypeValue::Union(idx, inner) = result.0 {
            assert_eq!(idx, 0);
            assert_eq!(*inner, AvroTypeValue::Null);
        } else {
            panic!("expected Union");
        }
    }

    #[test]
    fn bson_to_avro_union_string() {
        let result = AvroValue::try_from(BsonWithSchema(
            Bson::String("value".to_string()),
            nullable_string_schema(),
        ))
        .unwrap();

        if let AvroTypeValue::Union(idx, inner) = result.0 {
            assert_eq!(idx, 1); // string is at index 1 in ["null", "string"]
            assert_eq!(*inner, AvroTypeValue::String("value".to_string()));
        } else {
            panic!("expected Union");
        }
    }

    #[test]
    fn bson_to_avro_record() {
        let bson_doc = Bson::Document(doc! {"name": "Alice", "age": 30});
        let result = AvroValue::try_from(BsonWithSchema(bson_doc, simple_record_schema())).unwrap();

        if let AvroTypeValue::Record(fields) = result.0 {
            assert_eq!(fields.len(), 2);
            assert_eq!(
                fields[0],
                (
                    "name".to_string(),
                    AvroTypeValue::String("Alice".to_string())
                )
            );
            assert_eq!(fields[1], ("age".to_string(), AvroTypeValue::Int(30)));
        } else {
            panic!("expected Record");
        }
    }

    #[test]
    fn bson_to_avro_decimal() {
        let dec = Decimal128::from_str("123.45").unwrap();
        let result =
            AvroValue::try_from(BsonWithSchema(Bson::Decimal128(dec), decimal_schema())).unwrap();

        if let AvroTypeValue::Decimal(_) = result.0 {
            // Decimal conversion succeeded
        } else {
            panic!("expected Decimal");
        }
    }

    // =========================================================================
    // Group 3: avro_to_bson (reverse conversion)
    // =========================================================================

    #[test]
    fn avro_to_bson_null() {
        let result = avro_to_bson(&AvroTypeValue::Null, "field").unwrap();
        assert_eq!(result, Bson::Null);
    }

    #[test]
    fn avro_to_bson_boolean() {
        let result = avro_to_bson(&AvroTypeValue::Boolean(true), "field").unwrap();
        assert_eq!(result, Bson::Boolean(true));
    }

    #[test]
    fn avro_to_bson_int() {
        let result = avro_to_bson(&AvroTypeValue::Int(42), "field").unwrap();
        assert_eq!(result, Bson::Int32(42));
    }

    #[test]
    fn avro_to_bson_long() {
        let result = avro_to_bson(&AvroTypeValue::Long(9999999999i64), "field").unwrap();
        assert_eq!(result, Bson::Int64(9999999999i64));
    }

    #[test]
    fn avro_to_bson_float() {
        let result = avro_to_bson(&AvroTypeValue::Float(3.14f32), "field").unwrap();
        // Float is converted to Double
        if let Bson::Double(d) = result {
            assert!((d - 3.14f64).abs() < 0.001);
        } else {
            panic!("expected Double");
        }
    }

    #[test]
    fn avro_to_bson_double() {
        let result = avro_to_bson(&AvroTypeValue::Double(3.14159), "field").unwrap();
        assert_eq!(result, Bson::Double(3.14159));
    }

    #[test]
    fn avro_to_bson_string() {
        let result = avro_to_bson(&AvroTypeValue::String("hello".to_string()), "field").unwrap();
        assert_eq!(result, Bson::String("hello".to_string()));
    }

    #[test]
    fn avro_to_bson_array() {
        let avro_arr = AvroTypeValue::Array(vec![AvroTypeValue::Int(1), AvroTypeValue::Int(2)]);
        let result = avro_to_bson(&avro_arr, "field").unwrap();

        if let Bson::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Bson::Int32(1));
            assert_eq!(arr[1], Bson::Int32(2));
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn avro_to_bson_record() {
        let avro_rec = AvroTypeValue::Record(vec![
            ("name".to_string(), AvroTypeValue::String("Bob".to_string())),
            ("age".to_string(), AvroTypeValue::Int(25)),
        ]);
        let result = avro_to_bson(&avro_rec, "field").unwrap();

        if let Bson::Document(doc) = result {
            assert_eq!(doc.get_str("name").unwrap(), "Bob");
            assert_eq!(doc.get_i32("age").unwrap(), 25);
        } else {
            panic!("expected Document");
        }
    }

    #[test]
    fn avro_to_bson_enum() {
        let avro_enum = AvroTypeValue::Enum(1, "GREEN".to_string());
        let result = avro_to_bson(&avro_enum, "field").unwrap();
        assert_eq!(result, Bson::String("GREEN".to_string()));
    }

    #[test]
    fn avro_to_bson_union_null() {
        let avro_union = AvroTypeValue::Union(0, Box::new(AvroTypeValue::Null));
        let result = avro_to_bson(&avro_union, "field").unwrap();
        assert_eq!(result, Bson::Null);
    }

    #[test]
    fn avro_to_bson_union_value() {
        let avro_union =
            AvroTypeValue::Union(1, Box::new(AvroTypeValue::String("test".to_string())));
        let result = avro_to_bson(&avro_union, "field").unwrap();
        assert_eq!(result, Bson::String("test".to_string()));
    }

    // =========================================================================
    // Group 4: Error cases
    // =========================================================================

    #[test]
    fn bson_to_avro_error_type_mismatch_string_to_int() {
        let result = AvroValue::try_from(BsonWithSchema(
            Bson::String("not a number".to_string()),
            int_schema(),
        ));
        assert!(result.is_err());
    }

    #[test]
    fn bson_to_avro_error_type_mismatch_int_to_string() {
        let result = AvroValue::try_from(BsonWithSchema(Bson::Int32(42), string_schema()));
        assert!(result.is_err());
    }

    #[test]
    fn bson_to_avro_error_enum_invalid_symbol() {
        let result = AvroValue::try_from(BsonWithSchema(
            Bson::String("YELLOW".to_string()), // not in [RED, GREEN, BLUE]
            enum_schema(),
        ));
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("failed to convert bson to enum"));
    }

    #[test]
    fn bson_to_avro_error_record_missing_field() {
        let bson_doc = Bson::Document(doc! {"name": "Alice"}); // missing "age"
        let result = AvroValue::try_from(BsonWithSchema(bson_doc, simple_record_schema()));
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("failed to obtain field"));
    }

    #[test]
    fn bson_to_avro_error_float_not_supported() {
        let float_schema = Schema::Float;
        let result = AvroValue::try_from(BsonWithSchema(Bson::Double(3.14), float_schema));
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("float (32-bit) is not supported"));
    }

    #[test]
    fn bson_to_avro_error_null_in_non_nullable_union() {
        // Union without null variant
        let non_nullable_union = Schema::parse_str(r#"["string", "int"]"#).unwrap();
        let result = AvroValue::try_from(BsonWithSchema(Bson::Null, non_nullable_union));
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("non-nullable"));
    }

    #[test]
    fn avro_to_bson_error_unsupported_bytes() {
        let avro_bytes = AvroTypeValue::Bytes(vec![0x01, 0x02]);
        let result = avro_to_bson(&avro_bytes, "field");
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("unsupported avro type"));
    }

    // =========================================================================
    // Group 5: Round-trip tests
    // =========================================================================

    #[test]
    fn roundtrip_simple_types() {
        // Test that BSON -> Avro -> BSON preserves values
        let test_cases: Vec<(Bson, Schema)> = vec![
            (Bson::Boolean(true), boolean_schema()),
            (Bson::Int32(42), int_schema()),
            (Bson::Int64(9999999999i64), long_schema()),
            (Bson::Double(3.14159), double_schema()),
            (Bson::String("hello".to_string()), string_schema()),
            (Bson::Null, null_schema()),
        ];

        for (bson_val, schema) in test_cases {
            let avro = AvroValue::try_from(BsonWithSchema(bson_val.clone(), schema)).unwrap();
            let bson_back = avro_to_bson(&avro.0, "field").unwrap();
            assert_eq!(bson_val, bson_back);
        }
    }

    #[test]
    fn roundtrip_array() {
        let bson_arr = Bson::Array(vec![
            Bson::String("a".to_string()),
            Bson::String("b".to_string()),
        ]);
        let avro = AvroValue::try_from(BsonWithSchema(bson_arr.clone(), array_of_strings_schema()))
            .unwrap();
        let bson_back = avro_to_bson(&avro.0, "field").unwrap();
        assert_eq!(bson_arr, bson_back);
    }

    #[test]
    fn roundtrip_record() {
        let bson_doc = Bson::Document(doc! {"name": "Alice", "age": 30});
        let avro =
            AvroValue::try_from(BsonWithSchema(bson_doc.clone(), simple_record_schema())).unwrap();
        let bson_back = avro_to_bson(&avro.0, "field").unwrap();

        if let (Bson::Document(orig), Bson::Document(back)) = (bson_doc, bson_back) {
            assert_eq!(orig.get("name"), back.get("name"));
            assert_eq!(orig.get("age"), back.get("age"));
        } else {
            panic!("expected Documents");
        }
    }

    #[test]
    fn roundtrip_enum() {
        let bson_enum = Bson::String("BLUE".to_string());
        let avro = AvroValue::try_from(BsonWithSchema(bson_enum.clone(), enum_schema())).unwrap();
        let bson_back = avro_to_bson(&avro.0, "field").unwrap();
        assert_eq!(bson_enum, bson_back);
    }

    #[test]
    fn roundtrip_nullable_null() {
        let avro =
            AvroValue::try_from(BsonWithSchema(Bson::Null, nullable_string_schema())).unwrap();
        let bson_back = avro_to_bson(&avro.0, "field").unwrap();
        assert_eq!(Bson::Null, bson_back);
    }

    #[test]
    fn roundtrip_nullable_value() {
        let bson_val = Bson::String("test".to_string());
        let avro = AvroValue::try_from(BsonWithSchema(bson_val.clone(), nullable_string_schema()))
            .unwrap();
        let bson_back = avro_to_bson(&avro.0, "field").unwrap();
        assert_eq!(bson_val, bson_back);
    }
}
