use anyhow::Context;
use avro_rs::{types::Record, Schema, Writer};
use mongodb::bson::Document;

pub fn encode2(mongo_doc: Document, raw_schema: &str) -> anyhow::Result<Vec<u8>> {
    let schema = Schema::parse_str(raw_schema)?;

    let mut avro_writer = Writer::new(&schema, Vec::new());
    let mut record = Record::new(avro_writer.schema()).context("failed to create record")?;

    // need to read avro schema
    // pick a data type by field name
    // find a field in bson
    // cast the bson value to expected avro value
    // put avro value into the record

    let field_names: Vec<String> = record
        .fields
        .iter()
        .map(|(field_name, _)| field_name.clone())
        .collect();

    for field_name in field_names {
        let db_val = mongo_doc
            .get(&field_name)
            .ok_or_else(|| anyhow::anyhow!("failed to find value for key: {}", field_name))?;

        let avro_val = Wrap::from(db_val).0;
        record.put(&field_name, avro_val);
    }

    // println!("{:?}", record);

    avro_writer.append(record)?;
    Ok(avro_writer.into_inner()?)
}

use avro_rs::types::Value as AvroVal;
use mongodb::bson::spec::ElementType;
use mongodb::bson::Bson;

struct Wrap(AvroVal);

impl From<&Bson> for Wrap {
    fn from(bson_val: &Bson) -> Self {
        let bson_type = bson_val.element_type();

        match bson_type {
            ElementType::Boolean => Wrap(AvroVal::Boolean(bson_val.as_bool().unwrap())),
            ElementType::Double => Wrap(AvroVal::Double(bson_val.as_f64().unwrap())),
            ElementType::Int32 => Wrap(AvroVal::Int(bson_val.as_i32().unwrap())),
            ElementType::Int64 => Wrap(AvroVal::Long(bson_val.as_i64().unwrap())),
            ElementType::Null => Wrap(AvroVal::Null),
            ElementType::String => Wrap(AvroVal::String(bson_val.as_str().unwrap().to_owned())),
            ElementType::Undefined => Wrap(AvroVal::Null),
            ElementType::EmbeddedDocument => todo!(), // need - hashmap + recursive
            ElementType::Array => todo!(),            // need - recursive
            ElementType::Binary => todo!(),           // to check
            ElementType::ObjectId => todo!(),
            ElementType::DateTime => todo!(), // need
            ElementType::RegularExpression => todo!(),
            ElementType::DbPointer => todo!(),
            ElementType::JavaScriptCode => todo!(),
            ElementType::Symbol => todo!(),
            ElementType::JavaScriptCodeWithScope => todo!(),
            ElementType::Timestamp => todo!(),  // need
            ElementType::Decimal128 => todo!(), // need
            ElementType::MaxKey => todo!(),
            ElementType::MinKey => todo!(),
        }
    }
}


#[cfg(test)]
mod tests {
    use mongodb::bson::doc;
    use crate::encoding::avro::encode2;
    #[test]
    fn encode2_with_valid_schema_and_valid_payload(){
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "Employee",
                "fields" : [
                { "name" : "name" , "type" : "string" },
                { "name" : "age" , "type" : "int" },
                { "name": "gender", "type": "enum", "symbols": ["MALE", "FEMALE", "OTHER"]}
                ]
            }
        "###;
        let mongodb_document = doc!{
            "name": "Jon Doe",
            "age": 32,
            "gender": "OTHER",
            "additional_field": "foobar"
        };
        let result = encode2(mongodb_document, raw_schema);
        assert!(result.is_ok())
    }

    #[test]
    fn encode2_with_invalid_schema(){
        let raw_schema = r###"
            {
                "type" : "record",
                "name" : "Employee",
            }
        "###;
        let mongodb_document = doc!{"name": "Jon Doe", "age": 32};
        let result = encode2(mongodb_document, raw_schema);
        assert!(result.is_err())
    }

    #[test]
    fn encode2_with_valid_schema_but_invalid_payload(){
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
        let mongodb_document = doc!{"first_name": "Jon", "last_name": "Doe"};
        let result = encode2(mongodb_document, raw_schema);
        assert!(result.is_err())
    }
    
}
