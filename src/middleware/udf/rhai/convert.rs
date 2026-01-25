//! # Type Conversion Utilities for Rhai Middleware
//!
//! This module provides conversion utilities between Rust types and Rhai types,
//! with special handling for binary data through base64 encoding/decoding.
//!
//! ## Core Components
//!

//! - [`RhaiMap`]: Converts between Rust `HashMap` and Rhai `Map`
//!
//! ## Binary Data Handling
//!
//! Binary data (non-UTF8 bytes) is automatically encoded as base64 with a `data:base64,`
//! prefix when converting to strings, and decoded back when converting from strings.
//!

use mongodb::bson::{self, Bson, RawBsonRef, RawDocumentBuf};
use rhai::{Array, Dynamic, EvalAltResult, Map};
use std::{collections::HashMap, convert::TryFrom, sync::Arc};

use crate::config::Encoding;

/// Errors that can occur during type conversions
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    /// Invalid number format in JSON
    #[error("Invalid number format: cannot convert to i64 or f64")]
    InvalidNumber,

    /// Float value cannot be represented in JSON
    #[error("Invalid float value: {0} cannot be represented in JSON")]
    InvalidFloat(f64),

    /// Type cannot be converted to JSON
    #[error("Unsupported type for JSON conversion: {type_name}")]
    UnsupportedType { type_name: String },

    /// Base64 decoding failed
    #[error("Failed to decode base64: {0}")]
    Base64DecodeError(#[from] base64::DecodeError),

    /// Invalid data URI format
    #[error("Invalid data URI format: expected 'data:base64,' prefix")]
    InvalidDataUri,

    #[error("Failed to decode BSON: {0}")]
    BsonDecodeError(#[from] bson::de::Error),

    #[error("Failed to encode BSON: {0}")]
    BsonEncodeError(#[from] bson::ser::Error),

    #[error("Failed to decode JSON: {0}")]
    JsonDecodeError(#[from] serde_json::Error),

    #[error("Unsupported encoding: {0:?}")]
    UnsupportedEncoding(Encoding),

    #[error("Script must return a Map/Object for BSON encoding")]
    InvalidBsonStructure,
}

pub trait RhaiEncodingExt {
    /// Decodes raw bytes into a Rhai Dynamic value based on the encoding
    fn decode_rhai(&self, bytes: &[u8], is_framed_batch: bool) -> Result<Dynamic, ConvertError>;

    /// Encodes a Rhai Dynamic value back into bytes based on the encoding
    fn encode_rhai(&self, data: Dynamic, as_framed_batch: bool) -> Result<Vec<u8>, ConvertError>;
}

impl RhaiEncodingExt for Encoding {
    fn decode_rhai(&self, bytes: &[u8], is_framed_batch: bool) -> Result<Dynamic, ConvertError> {
        if is_framed_batch {
            return Self::decode_framed(bytes);
        }

        match self {
            Encoding::Bson => {
                let lazy = LazyBsonDocument::new(bytes.to_vec())
                    .map_err(|_| ConvertError::InvalidBsonStructure)?;

                Ok(Dynamic::from(lazy))
            }
            Encoding::Json => {
                // Use native Rhai serde support (efficient)
                serde_json::from_slice(bytes).map_err(ConvertError::from)
            }
            _ => Err(ConvertError::UnsupportedEncoding(self.clone())),
        }
    }

    fn encode_rhai(&self, data: Dynamic, as_framed_batch: bool) -> Result<Vec<u8>, ConvertError> {
        if as_framed_batch {
            return Self::encode_framed(data, self);
        }

        match self {
            Encoding::Bson => {
                if data.is::<LazyBsonDocument>() {
                    let lazy = data.cast::<LazyBsonDocument>();
                    match lazy.state {
                        LazyBsonState::Raw { doc, .. } => return Ok(doc.as_bytes().to_vec()),
                        LazyBsonState::Materialized(map) => {
                            // if modified we serialize the map
                            // and reuse BsonConverter logic
                            let converter = BsonConverter::try_from(Dynamic::from(map))?;
                            match converter.0 {
                                Bson::Document(doc) => return Ok(mongodb::bson::to_vec(&doc)?),
                                _ => return Err(ConvertError::InvalidBsonStructure),
                            }
                        }
                    }
                }

                // Fallback: Eager conversion
                let converter = BsonConverter::try_from(data)?;
                match converter.0 {
                    Bson::Document(doc) => Ok(mongodb::bson::to_vec(&doc)?),
                    _ => Err(ConvertError::InvalidBsonStructure),
                }
            }

            Encoding::Json => {
                // Use native Rhai serde support
                serde_json::to_vec(&data).map_err(ConvertError::from)
            }
            _ => Err(ConvertError::UnsupportedEncoding(self.clone())),
        }
    }
}

impl Encoding {
    fn decode_framed(bytes: &[u8]) -> Result<Dynamic, ConvertError> {
        use crate::encoding::framed::BatchContentType;

        let (items, content_type) =
            crate::encoding::framed::decode(bytes).map_err(|e| ConvertError::UnsupportedType {
                type_name: format!("Framed decode error: {}", e),
            })?;

        let mut array = Vec::with_capacity(items.len());

        for item in items {
            let val = match content_type {
                BatchContentType::Bson => {
                    // Wrap the raw bytes in a LazyBsonDocument instead of eagerly parsing
                    let lazy =
                        LazyBsonDocument::new(item).map_err(|e| ConvertError::UnsupportedType {
                            type_name: format!("Lazy BSON parse error: {}", e),
                        })?;
                    Dynamic::from(lazy)
                }
                BatchContentType::Json => {
                    serde_json::from_slice(&item).map_err(ConvertError::from)?
                }
                BatchContentType::Raw => Dynamic::from_blob(item),
                _ => {
                    return Err(ConvertError::UnsupportedType {
                        type_name: "Avro/Other in Framed".into(),
                    });
                }
            };
            array.push(val);
        }
        Ok(Dynamic::from_array(array))
    }

    fn encode_framed(data: Dynamic, target_encoding: &Encoding) -> Result<Vec<u8>, ConvertError> {
        use crate::encoding::framed::{BatchContentType, FramedWriter};

        if !data.is_array() {
            return Err(ConvertError::UnsupportedType {
                type_name: "Expected Array for Framed encoding".into(),
            });
        }

        let array = data.into_array().expect("checked is_array");
        let capacity = array.len() * 4096; // 4Kb: Heuristic pre-allocation

        let content_type = match target_encoding {
            Encoding::Json => BatchContentType::Json,
            _ => BatchContentType::Bson,
        };

        let mut writer = FramedWriter::new(content_type, capacity);

        for item in array {
            match content_type {
                BatchContentType::Json => {
                    writer
                        .add_item_with(|buf| {
                            serde_json::to_writer(buf, &item).map_err(ConvertError::from)
                        })
                        .map_err(|e| ConvertError::UnsupportedType {
                            type_name: format!("Json encode error: {}", e),
                        })?;
                }
                BatchContentType::Bson => {
                    if item.is::<LazyBsonDocument>() {
                        let lazy = item.cast::<LazyBsonDocument>();
                        match lazy.state {
                            LazyBsonState::Raw { doc: raw, .. } => {
                                writer
                                    .add_item_with(|buf| {
                                        use std::io::Write;
                                        buf.write_all(raw.as_bytes()).map_err(|e| {
                                            ConvertError::BsonEncodeError(
                                                mongodb::bson::ser::Error::Io(std::sync::Arc::new(
                                                    e,
                                                )),
                                            )
                                        })
                                    })
                                    .map_err(|e| ConvertError::UnsupportedType {
                                        type_name: format!("Lazy BSON write error: {}", e),
                                    })?;
                                continue;
                            }
                            LazyBsonState::Materialized(map) => {
                                let converter = BsonConverter::try_from(Dynamic::from(map))?;
                                match converter.0 {
                                    Bson::Document(doc) => {
                                        writer
                                            .add_item_with(|buf| {
                                                doc.to_writer(buf)
                                                    .map_err(ConvertError::BsonEncodeError)
                                            })
                                            .map_err(|e| ConvertError::UnsupportedType {
                                                type_name: format!("Bson encode error: {}", e),
                                            })?;
                                    }
                                    _ => return Err(ConvertError::InvalidBsonStructure),
                                }
                                continue;
                            }
                        }
                    }

                    // fallback to eager conversion
                    let converter = BsonConverter::try_from(item)?;
                    match converter.0 {
                        Bson::Document(doc) => {
                            writer
                                .add_item_with(|buf| {
                                    doc.to_writer(buf).map_err(ConvertError::BsonEncodeError)
                                })
                                .map_err(|e| ConvertError::UnsupportedType {
                                    type_name: format!("Bson encode error: {}", e),
                                })?;
                        }
                        _ => return Err(ConvertError::InvalidBsonStructure),
                    }
                }

                _ => {
                    return Err(ConvertError::UnsupportedType {
                        type_name: "Unsupported content type".into(),
                    });
                }
            }
        }

        Ok(writer.finish())
    }
}

/// Converter between `mongodb::bson::Bson` and Rhai `Dynamic`
pub struct BsonConverter(pub Bson);

impl TryFrom<BsonConverter> for Dynamic {
    type Error = ConvertError;

    fn try_from(converter: BsonConverter) -> Result<Self, Self::Error> {
        let value = converter.0;

        Ok(match value {
            Bson::Null => Dynamic::UNIT,
            Bson::Boolean(b) => Dynamic::from(b),
            Bson::Int32(i) => Dynamic::from(i as i64),
            Bson::Int64(i) => Dynamic::from(i),
            Bson::Double(f) => Dynamic::from(f),
            Bson::String(s) => Dynamic::from(s),
            Bson::Array(arr) => {
                let mut rhai_arr = Array::new();
                for item in arr {
                    rhai_arr.push(Dynamic::try_from(BsonConverter(item))?);
                }
                Dynamic::from(rhai_arr)
            }
            Bson::Document(doc) => {
                let mut rhai_map = Map::new();
                for (key, value) in doc {
                    rhai_map.insert(key.into(), Dynamic::try_from(BsonConverter(value))?);
                }
                Dynamic::from(rhai_map)
            }
            Bson::DateTime(dt) => {
                // Convert to EJSON format: { "$date": { "$numberLong": "millis" } }
                let mut inner = Map::new();
                inner.insert(
                    "$numberLong".into(),
                    Dynamic::from(dt.timestamp_millis().to_string()),
                );
                let mut map = Map::new();
                map.insert("$date".into(), Dynamic::from(inner));
                Dynamic::from(map)
            }
            Bson::ObjectId(oid) => {
                // Convert to EJSON format: { "$oid": "hex" }
                let mut map = Map::new();
                map.insert("$oid".into(), Dynamic::from(oid.to_string()));
                Dynamic::from(map)
            }
            _ => Dynamic::UNIT,
        })
    }
}

impl TryFrom<Dynamic> for BsonConverter {
    type Error = ConvertError;

    fn try_from(dynamic: Dynamic) -> Result<Self, Self::Error> {
        let value = if dynamic.is_unit() {
            Bson::Null
        } else if let Some(b) = dynamic.as_bool().ok() {
            Bson::Boolean(b)
        } else if let Some(i) = dynamic.as_int().ok() {
            Bson::Int64(i)
        } else if let Some(f) = dynamic.as_float().ok() {
            Bson::Double(f)
        } else if let Some(s) = dynamic.clone().into_immutable_string().ok() {
            Bson::String(s.to_string())
        } else if let Some(lazy) = dynamic.clone().try_cast::<LazyBsonDocument>() {
            match &lazy.state {
                LazyBsonState::Raw { doc: raw, .. } => {
                    let doc =
                        bson::from_slice(raw.as_bytes()).map_err(ConvertError::BsonDecodeError)?;
                    Bson::Document(doc)
                }
                LazyBsonState::Materialized(map) => {
                    BsonConverter::try_from(Dynamic::from(map.clone()))?.0
                }
            }
        } else if let Some(arr) = dynamic.clone().try_cast::<Array>() {
            let mut bson_arr = Vec::new();
            for item in arr {
                let converter = BsonConverter::try_from(item)?;
                bson_arr.push(converter.0);
            }
            Bson::Array(bson_arr)
        } else if let Some(map) = dynamic.clone().try_cast::<Map>() {
            // Check for EJSON Date: { "$date": ... }
            if let Some(date_val) = map.get("$date") {
                if let Some(inner) = date_val.clone().try_cast::<Map>() {
                    if let Some(millis_str) = inner
                        .get("$numberLong")
                        .and_then(|v| v.clone().into_immutable_string().ok())
                    {
                        if let Ok(millis) = millis_str.parse::<i64>() {
                            return Ok(BsonConverter(Bson::DateTime(
                                mongodb::bson::DateTime::from_millis(millis),
                            )));
                        }
                    }
                }
            }

            // Check for EJSON ObjectId: { "$oid": "hex" }
            if let Some(oid_val) = map.get("$oid") {
                if let Some(oid_str) = oid_val.clone().into_immutable_string().ok() {
                    if let Ok(oid) = mongodb::bson::oid::ObjectId::parse_str(&oid_str) {
                        return Ok(BsonConverter(Bson::ObjectId(oid)));
                    }
                }
            }

            let mut bson_doc = mongodb::bson::Document::new();
            for (key, value) in map {
                let key_str = key.as_str();
                let converter = BsonConverter::try_from(value)?;
                bson_doc.insert(key_str.to_string(), converter.0);
            }
            Bson::Document(bson_doc)
        } else {
            Bson::String(dynamic.to_string())
        };

        Ok(BsonConverter(value))
    }
}

/// Wrapper for converting between Rust HashMap and Rhai Map
///
/// Provides conversion from optional HashMap references to Rhai maps,
/// with empty map as default for None values.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
/// use mstream::middleware::udf::rhai::convert::RhaiMap;
///
/// let mut map = HashMap::new();
/// map.insert("key".to_string(), "value".to_string());
///
/// let rhai_map = RhaiMap::from(Some(&map));
/// assert_eq!(rhai_map.0.len(), 1);
///
/// let empty_map = RhaiMap::from(None);
/// assert!(empty_map.0.is_empty());
/// ```
pub struct RhaiMap(pub rhai::Map);

impl From<Option<&HashMap<String, String>>> for RhaiMap {
    fn from(opt_map: Option<&HashMap<String, String>>) -> Self {
        match opt_map {
            Some(map) => {
                let mut rhai_map = rhai::Map::new();
                for (k, v) in map {
                    rhai_map.insert(k.into(), v.into());
                }
                RhaiMap(rhai_map)
            }
            None => RhaiMap(rhai::Map::new()),
        }
    }
}

#[derive(Clone, Debug)]
pub enum LazyBsonState {
    Raw {
        doc: Arc<RawDocumentBuf>,
        len: usize,
    },
    Materialized(Map),
}

#[derive(Clone, Debug)]
pub struct LazyBsonDocument {
    pub state: LazyBsonState,
}

impl std::fmt::Display for LazyBsonDocument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LazyBsonDocument")
    }
}

impl LazyBsonDocument {
    pub fn new(bytes: Vec<u8>) -> Result<Self, mongodb::bson::raw::Error> {
        let doc = RawDocumentBuf::from_bytes(bytes)?;
        let len = doc.iter().count();
        Ok(Self {
            state: LazyBsonState::Raw {
                doc: Arc::new(doc),
                len,
            },
        })
    }

    fn raw_get(doc: &RawDocumentBuf, key: &str) -> Result<Option<Dynamic>, Box<EvalAltResult>> {
        match doc.get(key) {
            Ok(Some(raw_val)) => convert_raw_to_dynamic(raw_val).map(Some),
            Ok(None) => Ok(None),
            Err(e) => Err(format!("BSON error: {}", e).into()),
        }
    }

    pub fn get(&mut self, key: &str) -> Result<Dynamic, Box<EvalAltResult>> {
        match &self.state {
            LazyBsonState::Raw { doc, .. } => {
                Self::raw_get(doc, key).map(|opt| opt.unwrap_or(Dynamic::UNIT))
            }
            LazyBsonState::Materialized(map) => Ok(map.get(key).cloned().unwrap_or(Dynamic::UNIT)),
        }
    }

    pub fn set(&mut self, key: &str, value: Dynamic) -> Result<(), Box<EvalAltResult>> {
        // If Raw, materialize first (Copy-on-Write)
        if let LazyBsonState::Raw { doc, .. } = &self.state {
            let map = convert_raw_to_map(doc)?;
            self.state = LazyBsonState::Materialized(map);
        }

        // Now we are Materialized
        if let LazyBsonState::Materialized(map) = &mut self.state {
            map.insert(key.into(), value);
            Ok(())
        } else {
            unreachable!()
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<Dynamic, Box<EvalAltResult>> {
        if let LazyBsonState::Raw { doc, .. } = &self.state {
            let map = convert_raw_to_map(doc)?;
            self.state = LazyBsonState::Materialized(map);
        }

        if let LazyBsonState::Materialized(map) = &mut self.state {
            Ok(map.remove(key).unwrap_or(Dynamic::UNIT))
        } else {
            unreachable!()
        }
    }

    pub fn contains(&mut self, key: &str) -> Result<bool, Box<EvalAltResult>> {
        match &self.state {
            LazyBsonState::Raw { doc, .. } => Self::raw_get(doc, key).map(|opt| opt.is_some()),
            LazyBsonState::Materialized(map) => Ok(map.contains_key(key)),
        }
    }

    fn collect_from_raw<F, T>(
        &self,
        raw: &RawDocumentBuf,
        f: F,
    ) -> Result<Vec<T>, Box<EvalAltResult>>
    where
        F: Fn(&str, RawBsonRef) -> Result<T, Box<EvalAltResult>>,
    {
        raw.iter()
            .map(|elem| {
                let (key, val) = elem.map_err(|e| format!("BSON error: {}", e))?;
                f(key, val)
            })
            .collect()
    }

    pub fn keys(&mut self) -> Result<Vec<Dynamic>, Box<EvalAltResult>> {
        match &self.state {
            LazyBsonState::Raw { doc, .. } => {
                self.collect_from_raw(doc, |key, _| Ok(Dynamic::from(key.to_string())))
            }
            LazyBsonState::Materialized(map) => {
                Ok(map.keys().map(|k| Dynamic::from(k.to_string())).collect())
            }
        }
    }

    pub fn values(&mut self) -> Result<Vec<Dynamic>, Box<EvalAltResult>> {
        match &self.state {
            LazyBsonState::Raw { doc, .. } => {
                self.collect_from_raw(doc, |_, val| convert_raw_to_dynamic(val))
            }
            LazyBsonState::Materialized(map) => Ok(map.values().cloned().collect()),
        }
    }

    /// Returns the number of fields in the document.
    ///
    /// This is O(1) for both raw and materialized states. The element count is
    /// cached during construction for raw BSON documents since the BSON format
    /// only stores byte size, not element count.
    pub fn len(&mut self) -> Result<i64, Box<EvalAltResult>> {
        match &self.state {
            LazyBsonState::Raw { len, .. } => Ok(*len as i64),
            LazyBsonState::Materialized(map) => Ok(map.len() as i64),
        }
    }

    /// Returns true if the document has no fields.
    ///
    /// This is O(1) since we use the cached element count.
    pub fn is_empty(&mut self) -> Result<bool, Box<EvalAltResult>> {
        match &self.state {
            LazyBsonState::Raw { len, .. } => Ok(*len == 0),
            LazyBsonState::Materialized(map) => Ok(map.is_empty()),
        }
    }
}

fn convert_raw_to_map(raw: &RawDocumentBuf) -> Result<Map, Box<EvalAltResult>> {
    let mut map = Map::new();
    for elem in raw.iter() {
        let (key, value) = elem.map_err(|e| format!("BSON error: {}", e))?;
        map.insert(key.into(), convert_raw_to_dynamic(value)?);
    }
    Ok(map)
}

fn convert_raw_to_dynamic(raw: RawBsonRef) -> Result<Dynamic, Box<EvalAltResult>> {
    match raw {
        RawBsonRef::Double(v) => Ok(Dynamic::from(v)),
        RawBsonRef::String(v) => Ok(Dynamic::from(v.to_string())),
        RawBsonRef::Array(v) => {
            let mut arr = Vec::new();
            for item in v.into_iter() {
                let item = item.map_err(|e| format!("BSON array error: {}", e))?;
                arr.push(convert_raw_to_dynamic(item)?);
            }
            Ok(Dynamic::from(arr))
        }
        RawBsonRef::Document(v) => {
            let buf = v.to_raw_document_buf();
            let len = buf.iter().count();
            Ok(Dynamic::from(LazyBsonDocument {
                state: LazyBsonState::Raw {
                    doc: Arc::new(buf),
                    len,
                },
            }))
        }
        RawBsonRef::Boolean(v) => Ok(Dynamic::from(v)),
        RawBsonRef::Null => Ok(Dynamic::UNIT),
        RawBsonRef::Int32(v) => Ok(Dynamic::from(v as i64)),
        RawBsonRef::Int64(v) => Ok(Dynamic::from(v)),
        RawBsonRef::DateTime(v) => {
            let mut inner = Map::new();
            inner.insert(
                "$numberLong".into(),
                Dynamic::from(v.timestamp_millis().to_string()),
            );
            let mut map = Map::new();
            map.insert("$date".into(), Dynamic::from(inner));
            Ok(Dynamic::from(map))
        }
        RawBsonRef::ObjectId(v) => {
            let mut map = Map::new();
            map.insert("$oid".into(), Dynamic::from(v.to_string()));
            Ok(Dynamic::from(map))
        }
        _ => Ok(Dynamic::UNIT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    #[test]
    fn test_rhai_map_conversion() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), "value1".to_string());
        map.insert("key2".to_string(), "value2".to_string());

        let rhai_map = RhaiMap::from(Some(&map));
        assert_eq!(rhai_map.0.len(), 2);
        assert!(rhai_map.0.contains_key("key1"));
        assert!(rhai_map.0.contains_key("key2"));

        let empty_map = RhaiMap::from(None);
        assert!(empty_map.0.is_empty());
    }

    fn create_test_bson_document() -> LazyBsonDocument {
        let bson_doc = doc! {
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com"
        };
        let bytes = bson::to_vec(&bson_doc).unwrap();
        LazyBsonDocument::new(bytes).unwrap()
    }

    fn create_empty_bson_document() -> LazyBsonDocument {
        let bson_doc = doc! {};
        let bytes = bson::to_vec(&bson_doc).unwrap();
        LazyBsonDocument::new(bytes).unwrap()
    }

    #[test]
    fn test_lazy_bson_document_contains_existing_field() {
        let mut doc = create_test_bson_document();
        assert!(doc.contains("name").unwrap());
        assert!(doc.contains("age").unwrap());
        assert!(doc.contains("email").unwrap());
    }

    #[test]
    fn test_lazy_bson_document_contains_missing_field() {
        let mut doc = create_test_bson_document();
        assert!(!doc.contains("nonexistent").unwrap());
        assert!(!doc.contains("address").unwrap());
    }

    #[test]
    fn test_lazy_bson_document_keys() {
        let mut doc = create_test_bson_document();
        let keys = doc.keys().unwrap();
        assert_eq!(keys.len(), 3);

        let key_strings: Vec<String> = keys
            .iter()
            .map(|k| k.clone().into_string().unwrap())
            .collect();
        assert!(key_strings.contains(&"name".to_string()));
        assert!(key_strings.contains(&"age".to_string()));
        assert!(key_strings.contains(&"email".to_string()));
    }

    #[test]
    fn test_lazy_bson_document_keys_empty() {
        let mut doc = create_empty_bson_document();
        let keys = doc.keys().unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_lazy_bson_document_values() {
        let mut doc = create_test_bson_document();
        let values = doc.values().unwrap();
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_lazy_bson_document_values_after_materialization() {
        let mut doc = create_test_bson_document();
        doc.set("new_field", Dynamic::from("value")).unwrap();

        let values = doc.values().unwrap();
        assert_eq!(values.len(), 4);
    }

    #[test]
    fn test_lazy_bson_document_len() {
        let mut doc = create_test_bson_document();
        assert_eq!(doc.len().unwrap(), 3);
    }

    #[test]
    fn test_lazy_bson_document_len_empty() {
        let mut doc = create_empty_bson_document();
        assert_eq!(doc.len().unwrap(), 0);
    }

    #[test]
    fn test_lazy_bson_document_is_empty() {
        let mut doc = create_test_bson_document();
        assert!(!doc.is_empty().unwrap());
    }

    #[test]
    fn test_lazy_bson_document_is_empty_true() {
        let mut doc = create_empty_bson_document();
        assert!(doc.is_empty().unwrap());
    }

    #[test]
    fn test_lazy_bson_document_contains_after_materialization() {
        let mut doc = create_test_bson_document();
        // Trigger materialization by setting a value
        doc.set("new_field", Dynamic::from("value")).unwrap();

        // contains should still work after materialization
        assert!(doc.contains("name").unwrap());
        assert!(doc.contains("new_field").unwrap());
        assert!(!doc.contains("nonexistent").unwrap());
    }

    #[test]
    fn test_lazy_bson_document_keys_after_materialization() {
        let mut doc = create_test_bson_document();
        doc.set("new_field", Dynamic::from("value")).unwrap();

        let keys = doc.keys().unwrap();
        assert_eq!(keys.len(), 4);

        let key_strings: Vec<String> = keys
            .iter()
            .map(|k| k.clone().into_string().unwrap())
            .collect();
        assert!(key_strings.contains(&"new_field".to_string()));
    }

    #[test]
    fn test_lazy_bson_document_len_after_materialization() {
        let mut doc = create_test_bson_document();
        doc.set("new_field", Dynamic::from("value")).unwrap();
        assert_eq!(doc.len().unwrap(), 4);
    }

    #[test]
    fn test_lazy_bson_document_is_empty_after_materialization() {
        let mut doc = create_empty_bson_document();
        assert!(doc.is_empty().unwrap());

        doc.set("field", Dynamic::from("value")).unwrap();
        assert!(!doc.is_empty().unwrap());
    }
}
