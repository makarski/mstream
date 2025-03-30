use log::debug;
use mongodb::bson::Document;

pub mod avro;

pub fn json_to_bson_doc(payload: &[u8]) -> anyhow::Result<Document> {
    let v: Document = serde_json::from_slice(payload)?;
    debug!("deserialized json value: {}", v);
    Ok(v)
}
