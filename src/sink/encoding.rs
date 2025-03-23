//! # Document Encoding Module
//!
//! This module handles format conversion between different data representations.
//! It provides a comprehensive set of transformations between BSON, JSON, and Avro formats.
//!
//! ## Supported Conversion Paths
//!
//! ### BSON Source
//! - BSON → Avro: Converts MongoDB BSON documents to Avro records
//! - BSON → JSON: Serializes BSON documents to JSON format
//! - BSON → BSON: Passthrough for MongoDB persistence (using Document type)
//!
//! ### Avro Source
//! - Avro → Avro: Passthrough or schema validation
//! - Avro → JSON: Deserializes Avro records to JSON format
//! - Avro → BSON: Converts Avro records to MongoDB BSON documents
//!
//! ### JSON Source
//! - JSON → JSON: Passthrough or format validation
//! - JSON → Avro: Parses JSON and encodes as Avro records
//! - JSON → BSON: Parses JSON and converts to BSON documents
//!
//! All JSON source operations are processed by first converting to BSON and then
//! applying the same transformation logic as BSON sources, unless the target
//! format is JSON itself.
use std::collections::HashMap;

use anyhow::bail;
use apache_avro::Schema;
use mongodb::bson::Document;

use crate::{
    config::{Encoding, ServiceConfigReference},
    encoding::{avro, json_to_bson_doc},
    source::SourceEvent,
};

pub struct SinkEvent {
    pub bson_doc: Option<Document>,
    pub raw_bytes: Option<Vec<u8>>,
    pub attributes: Option<HashMap<String, String>>,
    encoding: Encoding,
}

impl SinkEvent {
    pub fn from_source_event(
        se: SourceEvent,
        sink_cfg: &ServiceConfigReference,
        schema: &Schema, // todo: change in future to support different schemas
    ) -> anyhow::Result<Self> {
        match (se.encoding, sink_cfg.encoding.clone()) {
            (Encoding::Bson, Encoding::Bson) => Ok(Self {
                bson_doc: se.document,
                raw_bytes: se.raw_bytes,
                attributes: se.attributes,
                encoding: Encoding::Bson,
            }),
            (Encoding::Bson, Encoding::Avro) => match se.document {
                Some(doc) => {
                    let encoded = avro::encode(doc, schema)?;
                    Ok(Self {
                        bson_doc: None,
                        raw_bytes: Some(encoded),
                        attributes: se.attributes,
                        encoding: Encoding::Avro,
                    })
                }
                None => bail!("source event document is missing: {:?}", se.attributes),
            },
            (Encoding::Bson, Encoding::Json) => match se.document {
                Some(doc) => {
                    let encoded = serde_json::to_vec(&doc)?;
                    Ok(Self {
                        bson_doc: None,
                        raw_bytes: Some(encoded),
                        attributes: se.attributes,
                        encoding: Encoding::Json,
                    })
                }
                None => bail!("source event document is missing: {:?}", se.attributes),
            },
            (Encoding::Avro, Encoding::Avro) => {
                if se.raw_bytes.is_some() {
                    Ok(Self {
                        bson_doc: None,
                        raw_bytes: se.raw_bytes,
                        attributes: se.attributes,
                        encoding: Encoding::Avro,
                    })
                } else {
                    bail!("source event raw bytes are missing: {:?}", se.attributes)
                }
            }
            (Encoding::Avro, Encoding::Bson) => match se.raw_bytes {
                Some(b) => {
                    let decoded = avro::decode(&b, schema)?;
                    Ok(Self {
                        bson_doc: Some(decoded),
                        raw_bytes: None,
                        attributes: se.attributes,
                        encoding: Encoding::Bson,
                    })
                }
                None => bail!("source event raw bytes are missing: {:?}", se.attributes),
            },
            (Encoding::Avro, Encoding::Json) => match se.raw_bytes {
                Some(b) => {
                    // todo: refactor - avoid decoding to bson and then encoding to json
                    let decoded = avro::decode(&b, schema)?;
                    let encoded = serde_json::to_vec(&decoded)?;
                    Ok(Self {
                        bson_doc: None,
                        raw_bytes: Some(encoded),
                        attributes: se.attributes,
                        encoding: Encoding::Json,
                    })
                }
                None => bail!("source event raw bytes are missing: {:?}", se.attributes),
            },
            (Encoding::Json, Encoding::Json) => match se.raw_bytes {
                Some(ref b) => Ok(Self {
                    bson_doc: None,
                    raw_bytes: Some(b.clone()),
                    attributes: se.attributes,
                    encoding: Encoding::Json,
                }),
                None => bail!("source event raw bytes are missing: {:?}", se.attributes),
            },
            (Encoding::Json, Encoding::Bson) => match se.raw_bytes {
                Some(b) => {
                    let decoded = json_to_bson_doc(&b)?;
                    Ok(Self {
                        bson_doc: Some(decoded),
                        raw_bytes: None,
                        attributes: se.attributes,
                        encoding: Encoding::Bson,
                    })
                }
                None => bail!("source event raw bytes are missing: {:?}", se.attributes),
            },
            (Encoding::Json, Encoding::Avro) => match se.raw_bytes {
                Some(b) => {
                    let bson_doc = json_to_bson_doc(&b)?;
                    let encoded = avro::encode(bson_doc, schema)?;
                    Ok(Self {
                        bson_doc: None,
                        raw_bytes: Some(encoded),
                        attributes: se.attributes,
                        encoding: Encoding::Avro,
                    })
                }
                None => bail!("source event raw bytes are missing: {:?}", se.attributes),
            },
        }
    }
}
