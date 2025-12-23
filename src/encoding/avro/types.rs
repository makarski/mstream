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
