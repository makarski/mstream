use std::collections::HashMap;

use crate::{config::Encoding, source::SourceEvent};

#[derive(Debug, Clone)]
pub struct SinkEvent {
    pub raw_bytes: Vec<u8>,
    // todo: decide if we want to keep this
    // pub bson_doc: Option<Document>,
    pub attributes: Option<HashMap<String, String>>,
    pub encoding: Encoding,
    pub is_framed_batch: bool,
}

impl From<SourceEvent> for SinkEvent {
    fn from(se: SourceEvent) -> Self {
        Self {
            raw_bytes: se.raw_bytes,
            // bson_doc: se.document,
            attributes: se.attributes,
            encoding: se.encoding,
            is_framed_batch: se.is_framed_batch,
        }
    }
}
