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
