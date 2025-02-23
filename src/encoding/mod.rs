use anyhow::Context;
use log::debug;
use mongodb::bson::{self, Document};
use serde_json::Value;

pub mod avro;

pub fn json_to_bson_doc(payload: &[u8]) -> anyhow::Result<Document> {
    let json_val: Value = serde_json::from_slice(payload).context("failed to parse json")?;
    debug!("deserialized json value: {}", json_val);
    let doc = bson::to_document(&json_val).context("failed to convert json to bson")?;
    Ok(doc)
}
