use anyhow::bail;
use apache_avro::types::Value as AvroVal;
use apache_avro::{from_avro_datum, to_avro_datum, Schema};

pub fn validate(avro_b: Vec<u8>, schema: &Schema) -> anyhow::Result<Vec<u8>> {
    let mut reader = avro_b.as_slice();
    let avro_value = from_avro_datum(schema, &mut reader, None)?;
    if !avro_value.validate(schema) {
        bail!("failed to validate schema");
    }

    Ok(avro_b)
}

pub fn validate_many(avro_b: Vec<Vec<u8>>, schema: &Schema) -> anyhow::Result<Vec<u8>> {
    let mut records = Vec::with_capacity(avro_b.len());
    let batch_schema = Schema::Array(Box::new(schema.clone()));

    // Validate each item individually
    for item in avro_b.into_iter() {
        let mut reader = item.as_slice();
        let avro_value = from_avro_datum(schema, &mut reader, None)?;

        if !avro_value.validate(schema) {
            bail!("Failed to validate schema for an item in the batch");
        }

        records.push(avro_value);
    }

    // Encode the validated records as a single Avro array
    Ok(to_avro_datum(&batch_schema, AvroVal::Array(records))?)
}
