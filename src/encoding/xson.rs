use core::convert::TryFrom;

use apache_avro::Schema as AvroSchema;
use mongodb::bson::{self, Bson, Document};
use serde_json::Value as JsonValue;

use crate::encoding::avro;
use crate::schema::Schema;

/// Converts a BSON value to a flat JSON value, unwrapping Extended JSON wrappers.
/// - ObjectId → string hex
/// - DateTime → ISO 8601 string
/// - Int32/Int64 → number
/// - Binary → base64 string
/// - Decimal128 → string
/// - Timestamp → object with t and i fields
/// - etc.
/// Converts an f64 to a JSON value, handling special cases (NaN, Infinity).
fn f64_to_json(f: f64) -> JsonValue {
    if f.is_nan() {
        return JsonValue::String("NaN".to_string());
    }
    if f.is_infinite() {
        let s = if f.is_sign_positive() {
            "Infinity"
        } else {
            "-Infinity"
        };
        return JsonValue::String(s.to_string());
    }
    serde_json::Number::from_f64(f)
        .map(JsonValue::Number)
        .unwrap_or(JsonValue::Null)
}

fn bson_to_flat_json(bson: Bson) -> JsonValue {
    match bson {
        Bson::Double(f) => f64_to_json(f),
        Bson::String(s) => JsonValue::String(s),
        Bson::Array(arr) => JsonValue::Array(arr.into_iter().map(bson_to_flat_json).collect()),
        Bson::Document(doc) => document_to_flat_json(doc),
        Bson::Boolean(b) => JsonValue::Bool(b),
        Bson::Null => JsonValue::Null,
        Bson::RegularExpression(regex) => {
            let mut map = serde_json::Map::new();
            map.insert("pattern".to_string(), JsonValue::String(regex.pattern));
            map.insert("options".to_string(), JsonValue::String(regex.options));
            JsonValue::Object(map)
        }
        Bson::JavaScriptCode(code) => JsonValue::String(code),
        Bson::JavaScriptCodeWithScope(code_with_scope) => {
            let mut map = serde_json::Map::new();
            map.insert("code".to_string(), JsonValue::String(code_with_scope.code));
            map.insert(
                "scope".to_string(),
                document_to_flat_json(code_with_scope.scope),
            );
            JsonValue::Object(map)
        }
        Bson::Int32(i) => JsonValue::Number(i.into()),
        Bson::Int64(i) => JsonValue::Number(i.into()),
        Bson::Timestamp(ts) => {
            // Convert seconds since epoch to ISO 8601 string
            let dt = chrono::DateTime::from_timestamp(ts.time as i64, 0);
            match dt {
                Some(datetime) => JsonValue::String(datetime.to_rfc3339()),
                None => JsonValue::Number(ts.time.into()), // fallback to seconds
            }
        }
        Bson::Binary(bin) => {
            use base64::{Engine, engine::general_purpose::STANDARD};
            JsonValue::String(STANDARD.encode(&bin.bytes))
        }
        Bson::ObjectId(oid) => JsonValue::String(oid.to_hex()),
        Bson::DateTime(dt) => JsonValue::String(
            dt.try_to_rfc3339_string()
                .unwrap_or_else(|_| dt.timestamp_millis().to_string()),
        ),
        Bson::Symbol(s) => JsonValue::String(s),
        Bson::Decimal128(d) => JsonValue::String(d.to_string()),
        Bson::Undefined => JsonValue::Null,
        Bson::MaxKey => JsonValue::String("MaxKey".to_string()),
        Bson::MinKey => JsonValue::String("MinKey".to_string()),
        Bson::DbPointer(dbp) => {
            // DbPointer fields are private, so we use Debug representation
            JsonValue::String(format!("{:?}", dbp))
        }
    }
}

/// Converts a BSON Document to a flat JSON object.
fn document_to_flat_json(doc: Document) -> JsonValue {
    let map: serde_json::Map<String, JsonValue> = doc
        .into_iter()
        .map(|(k, v)| (k, bson_to_flat_json(v)))
        .collect();
    JsonValue::Object(map)
}

trait ApplyAvroSchema {
    fn apply_avro_schema(self, avro_schema: &AvroSchema) -> anyhow::Result<Document>;
}

impl<T> ApplyAvroSchema for T
where
    T: TryInto<Document, Error = anyhow::Error>,
{
    fn apply_avro_schema(self, avro_schema: &AvroSchema) -> anyhow::Result<Document> {
        let doc: Document = self.try_into()?;
        avro::encode(doc, avro_schema).and_then(|encoded| avro::decode(&encoded, avro_schema))
    }
}

// --- JSON and BSON Bytes ---

pub struct JsonBytes(pub Vec<u8>);
pub struct BsonBytes(pub Vec<u8>);

impl TryFrom<Vec<Document>> for JsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: Vec<Document>) -> Result<Self, Self::Error> {
        let json_bytes = serde_json::to_vec(&value)
            .map_err(|e| anyhow::anyhow!("failed to serialize JSON: {}", e))?;
        Ok(JsonBytes(json_bytes))
    }
}

impl TryInto<Vec<Document>> for JsonBytes {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Vec<Document>, Self::Error> {
        serde_json::from_slice(&self.0)
            .map_err(|e| anyhow::anyhow!("failed to deserialize JSON into a Vec<Document>: {}", e))
    }
}

impl TryInto<Document> for JsonBytes {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Document, Self::Error> {
        serde_json::from_slice(&self.0)
            .map_err(|e| anyhow::anyhow!("failed to deserialize JSON into a Document: {}", e))
    }
}

impl TryFrom<Vec<Document>> for BsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: Vec<Document>) -> Result<Self, Self::Error> {
        let bson_bytes =
            bson::to_vec(&value).map_err(|e| anyhow::anyhow!("failed to serialize BSON: {}", e))?;
        Ok(BsonBytes(bson_bytes))
    }
}

impl TryInto<Vec<Document>> for BsonBytes {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Vec<Document>, Self::Error> {
        bson::from_slice(&self.0)
            .map_err(|e| anyhow::anyhow!("failed to deserialize BSON into a Vec<Document>: {}", e))
    }
}

impl TryInto<Document> for BsonBytes {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Document, Self::Error> {
        bson::from_slice(&self.0)
            .map_err(|e| anyhow::anyhow!("failed to deserialize BSON into a Document: {}", e))
    }
}

impl TryFrom<JsonBytes> for BsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: JsonBytes) -> Result<Self, Self::Error> {
        let doc: Document = serde_json::from_slice(&value.0)?;
        Ok(BsonBytes(bson::to_vec(&doc)?))
    }
}

impl TryFrom<BsonBytes> for JsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: BsonBytes) -> Result<Self, Self::Error> {
        let doc: Document = bson::from_slice(&value.0)?;
        let flat_json = document_to_flat_json(doc);
        Ok(JsonBytes(serde_json::to_vec(&flat_json)?))
    }
}

// --- JSON With Schema ---

pub struct JsonBytesWithSchema<'a> {
    pub data: Vec<u8>,
    pub schema: &'a Schema,
}

impl<'a> JsonBytesWithSchema<'a> {
    pub fn new(data: Vec<u8>, schema: &'a Schema) -> Self {
        JsonBytesWithSchema { data, schema }
    }
}

impl TryFrom<JsonBytesWithSchema<'_>> for BsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: JsonBytesWithSchema) -> Result<Self, Self::Error> {
        match value.schema {
            Schema::Avro(avro_schema) => {
                let bson_doc = JsonBytes(value.data).apply_avro_schema(avro_schema)?;
                Ok(BsonBytes(bson::to_vec(&bson_doc)?))
            }
            Schema::Undefined => BsonBytes::try_from(JsonBytes(value.data)),
        }
    }
}

impl TryFrom<JsonBytesWithSchema<'_>> for JsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: JsonBytesWithSchema) -> Result<Self, Self::Error> {
        match value.schema {
            Schema::Avro(avro_schema) => {
                let bson_doc: Document = JsonBytes(value.data).apply_avro_schema(avro_schema)?;
                Ok(JsonBytes(serde_json::to_vec(&bson_doc)?))
            }
            Schema::Undefined => Ok(JsonBytes(value.data)),
        }
    }
}

// --- BSON With Schema ---

pub struct BsonBytesWithSchema<'a> {
    pub data: Vec<u8>,
    pub schema: &'a Schema,
}

impl<'a> BsonBytesWithSchema<'a> {
    pub fn new(data: Vec<u8>, schema: &'a Schema) -> Self {
        BsonBytesWithSchema { data, schema }
    }
}

impl TryFrom<BsonBytesWithSchema<'_>> for JsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: BsonBytesWithSchema) -> Result<Self, Self::Error> {
        match value.schema {
            Schema::Avro(avro_schema) => {
                let bson_doc: Document = BsonBytes(value.data).apply_avro_schema(avro_schema)?;
                let flat_json = document_to_flat_json(bson_doc);
                Ok(JsonBytes(serde_json::to_vec(&flat_json)?))
            }
            Schema::Undefined => BsonBytes(value.data).try_into(),
        }
    }
}

impl TryFrom<BsonBytesWithSchema<'_>> for BsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: BsonBytesWithSchema) -> Result<Self, Self::Error> {
        match value.schema {
            Schema::Avro(avro_schema) => {
                let bson_doc = BsonBytes(value.data).apply_avro_schema(avro_schema)?;
                Ok(BsonBytes(bson::to_vec(&bson_doc)?))
            }
            Schema::Undefined => Ok(BsonBytes(value.data)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::Schema as AvroSchema;
    use mongodb::bson::doc;
    use proptest::prelude::*;

    // =========================================================================
    // Group 1: JsonBytes conversions
    // =========================================================================

    #[test]
    fn json_bytes_from_vec_document() {
        let docs = vec![doc! {"name": "alice"}, doc! {"name": "bob"}];
        let json_bytes = JsonBytes::try_from(docs).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        assert!(json_str.contains("alice"));
        assert!(json_str.contains("bob"));
    }

    #[test]
    fn json_bytes_into_vec_document() {
        let json = br#"[{"name": "alice"}, {"name": "bob"}]"#.to_vec();
        let docs: Vec<Document> = JsonBytes(json).try_into().unwrap();

        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].get_str("name").unwrap(), "alice");
        assert_eq!(docs[1].get_str("name").unwrap(), "bob");
    }

    #[test]
    fn json_bytes_into_single_document() {
        let json = br#"{"key": "value", "num": 42}"#.to_vec();
        let doc: Document = JsonBytes(json).try_into().unwrap();

        assert_eq!(doc.get_str("key").unwrap(), "value");
        assert_eq!(doc.get_i32("num").unwrap(), 42);
    }

    #[test]
    fn json_bytes_into_vec_document_error_invalid_json() {
        let invalid_json = b"not valid json".to_vec();
        let result: Result<Vec<Document>, _> = JsonBytes(invalid_json).try_into();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("failed to deserialize JSON")
        );
    }

    #[test]
    fn json_bytes_into_document_error_invalid_json() {
        let invalid_json = b"{broken".to_vec();
        let result: Result<Document, _> = JsonBytes(invalid_json).try_into();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("failed to deserialize JSON")
        );
    }

    #[test]
    fn json_bytes_roundtrip_vec_document() {
        let original = vec![doc! {"a": 1}, doc! {"b": 2}];
        let json_bytes = JsonBytes::try_from(original.clone()).unwrap();
        let decoded: Vec<Document> = json_bytes.try_into().unwrap();

        assert_eq!(decoded.len(), original.len());
        assert_eq!(decoded[0].get_i32("a").unwrap(), 1);
        assert_eq!(decoded[1].get_i32("b").unwrap(), 2);
    }

    // =========================================================================
    // Group 2: BsonBytes conversions
    // =========================================================================

    #[test]
    fn bson_bytes_from_vec_document_via_json_roundtrip() {
        // BSON doesn't support top-level arrays directly, so we test via JSON roundtrip
        let docs = vec![doc! {"name": "charlie"}];
        let json_bytes = JsonBytes::try_from(docs.clone()).unwrap();
        let decoded: Vec<Document> = json_bytes.try_into().unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].get_str("name").unwrap(), "charlie");
    }

    #[test]
    fn bson_bytes_single_document_roundtrip() {
        // Test single document BSON serialization (which BSON does support)
        let doc = doc! {"name": "charlie", "age": 25};
        let bson_bytes = bson::to_vec(&doc).unwrap();
        let decoded: Document = BsonBytes(bson_bytes).try_into().unwrap();

        assert_eq!(decoded.get_str("name").unwrap(), "charlie");
        assert_eq!(decoded.get_i32("age").unwrap(), 25);
    }

    #[test]
    fn bson_bytes_into_single_document() {
        let doc = doc! {"key": "value", "num": 123};
        let bson_bytes = bson::to_vec(&doc).unwrap();
        let decoded: Document = BsonBytes(bson_bytes).try_into().unwrap();

        assert_eq!(decoded.get_str("key").unwrap(), "value");
        assert_eq!(decoded.get_i32("num").unwrap(), 123);
    }

    #[test]
    fn bson_bytes_into_vec_document_error_invalid_bson() {
        let invalid_bson = b"not valid bson".to_vec();
        let result: Result<Vec<Document>, _> = BsonBytes(invalid_bson).try_into();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("failed to deserialize BSON")
        );
    }

    #[test]
    fn bson_bytes_into_document_error_invalid_bson() {
        let invalid_bson = vec![0x01, 0x02, 0x03];
        let result: Result<Document, _> = BsonBytes(invalid_bson).try_into();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("failed to deserialize BSON")
        );
    }

    #[test]
    fn bson_bytes_vec_document_error_top_level_array() {
        // BSON doesn't support top-level arrays - verify this is the expected behavior
        let docs = vec![doc! {"x": 100}, doc! {"y": 200}];
        let result = BsonBytes::try_from(docs);

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("non-document type"));
    }

    // =========================================================================
    // Group 3: Cross-format conversions (JSON <-> BSON)
    // =========================================================================

    #[test]
    fn json_bytes_to_bson_bytes() {
        let json = br#"{"name": "eve", "age": 30}"#.to_vec();
        let bson_bytes = BsonBytes::try_from(JsonBytes(json)).unwrap();

        // Decode and verify
        let doc: Document = bson_bytes.try_into().unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "eve");
        assert_eq!(doc.get_i32("age").unwrap(), 30);
    }

    #[test]
    fn bson_bytes_to_json_bytes() {
        let doc = doc! {"name": "frank", "active": true};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        assert!(json_str.contains("frank"));
        assert!(json_str.contains("true"));
    }

    #[test]
    fn bson_to_json_flattens_objectid() {
        use mongodb::bson::oid::ObjectId;
        let oid = ObjectId::parse_str("60002e5e19e23b5c08a6c03c").unwrap();
        let doc = doc! {"_id": oid};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        // Should be flat string, not {"$oid": "..."}
        assert_eq!(json_str, r#"{"_id":"60002e5e19e23b5c08a6c03c"}"#);
    }

    #[test]
    fn bson_to_json_flattens_datetime() {
        use mongodb::bson::DateTime;
        // 2026-01-09T05:23:58.852Z in millis
        let dt = DateTime::from_millis(1767957838852);
        let doc = doc! {"lastCalculated": dt};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        // Should be ISO 8601 string, not {"$date": {"$numberLong": "..."}}
        assert!(json_str.contains("2026-01-09"));
        assert!(!json_str.contains("$date"));
        assert!(!json_str.contains("$numberLong"));
    }

    #[test]
    fn bson_to_json_flattens_binary() {
        use mongodb::bson::Binary;
        let bin = Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: vec![0x01, 0x02, 0x03, 0x04],
        };
        let doc = doc! {"data": bin};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        // Should be base64 string, not {"$binary": {...}}
        assert!(!json_str.contains("$binary"));
        assert!(json_str.contains("AQIDBA==")); // base64 of [1,2,3,4]
    }

    #[test]
    fn bson_to_json_flattens_int64() {
        let doc = doc! {"bigNum": 9223372036854775807_i64};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        // Should be plain number, not {"$numberLong": "..."}
        assert!(!json_str.contains("$numberLong"));
        assert!(json_str.contains("9223372036854775807"));
    }

    #[test]
    fn bson_to_json_handles_double_nan() {
        let doc = doc! {"value": f64::NAN};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        assert_eq!(json_str, r#"{"value":"NaN"}"#);
    }

    #[test]
    fn bson_to_json_handles_double_infinity() {
        let doc = doc! {"value": f64::INFINITY};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        assert_eq!(json_str, r#"{"value":"Infinity"}"#);
    }

    #[test]
    fn bson_to_json_handles_double_neg_infinity() {
        let doc = doc! {"value": f64::NEG_INFINITY};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        assert_eq!(json_str, r#"{"value":"-Infinity"}"#);
    }

    #[test]
    fn bson_to_json_handles_normal_double() {
        let doc = doc! {"value": 3.14159};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        assert!(json_str.contains("3.14159"));
        // Should be a number, not a string
        assert!(!json_str.contains(r#""3.14159""#));
    }

    #[test]
    fn bson_to_json_flattens_decimal128() {
        use std::str::FromStr;
        let dec = mongodb::bson::Decimal128::from_str("123.456").unwrap();
        let doc = doc! {"price": dec};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        // Should be string representation, not {"$numberDecimal": "..."}
        assert!(!json_str.contains("$numberDecimal"));
        assert!(json_str.contains("123.456"));
    }

    #[test]
    fn bson_to_json_flattens_timestamp() {
        use mongodb::bson::Timestamp;
        let ts = Timestamp {
            time: 1234567890, // 2009-02-13T23:31:30Z
            increment: 42,
        };
        let doc = doc! {"ts": ts};
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        // Should be ISO 8601 string, not {"$timestamp": {...}}
        assert!(!json_str.contains("$timestamp"));
        assert!(json_str.contains("2009-02-13"));
        assert!(json_str.contains("23:31:30"));
    }

    #[test]
    fn bson_to_json_flattens_nested_documents() {
        use mongodb::bson::DateTime;
        use mongodb::bson::oid::ObjectId;
        let oid = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let dt = DateTime::from_millis(1704067200000); // 2024-01-01
        let doc = doc! {
            "user": {
                "_id": oid,
                "createdAt": dt,
                "profile": {
                    "active": true
                }
            }
        };
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        assert!(!json_str.contains("$oid"));
        assert!(!json_str.contains("$date"));
        assert!(json_str.contains("507f1f77bcf86cd799439011"));
    }

    #[test]
    fn bson_to_json_flattens_arrays_with_special_types() {
        use mongodb::bson::oid::ObjectId;
        let oid1 = ObjectId::parse_str("60002e5e19e23b5c08a6c03c").unwrap();
        let oid2 = ObjectId::parse_str("60002e5e19e23b5c08a6c03d").unwrap();
        let doc = doc! {
            "ids": [oid1, oid2],
            "nested": [{"id": oid1}, {"id": oid2}]
        };
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();
        assert!(!json_str.contains("$oid"));
        assert!(json_str.contains("60002e5e19e23b5c08a6c03c"));
        assert!(json_str.contains("60002e5e19e23b5c08a6c03d"));
    }

    #[test]
    fn bson_to_json_flattens_customer_lp_like_document() {
        use mongodb::bson::DateTime;
        use mongodb::bson::oid::ObjectId;
        let doc = doc! {
            "_id": ObjectId::parse_str("60002e5e19e23b5c08a6c03c").unwrap(),
            "email": "paul@mycustomerlens.com",
            "lastCalculated": DateTime::from_millis(1767957838852_i64),
            "lastSynced": DateTime::from_millis(1767957865461_i64),
            "syncPending": false,
            "productsDownloaded": { "STUDIO_3T": true },
            "buyingRoles": {
                "LICENSE_OWNER": false,
                "LICENSE_ADMINISTRATOR": true
            }
        };
        let bson_bytes = BsonBytes(bson::to_vec(&doc).unwrap());
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let json_str = String::from_utf8(json_bytes.0).unwrap();

        // Verify no Extended JSON wrappers
        assert!(!json_str.contains("$oid"), "Should not contain $oid");
        assert!(!json_str.contains("$date"), "Should not contain $date");
        assert!(
            !json_str.contains("$numberLong"),
            "Should not contain $numberLong"
        );

        // Verify flat values are present
        assert!(json_str.contains(r#""_id":"60002e5e19e23b5c08a6c03c""#));
        assert!(json_str.contains("paul@mycustomerlens.com"));
        assert!(json_str.contains("2026-01-09")); // ISO date
    }

    #[test]
    fn roundtrip_json_to_bson_to_json() {
        let original_json = br#"{"key": "value", "count": 42}"#.to_vec();

        let bson_bytes = BsonBytes::try_from(JsonBytes(original_json)).unwrap();
        let json_bytes = JsonBytes::try_from(bson_bytes).unwrap();

        let doc: Document = json_bytes.try_into().unwrap();
        assert_eq!(doc.get_str("key").unwrap(), "value");
        assert_eq!(doc.get_i32("count").unwrap(), 42);
    }

    #[test]
    fn json_to_bson_error_invalid_json() {
        let invalid_json = b"{{invalid}}".to_vec();
        let result = BsonBytes::try_from(JsonBytes(invalid_json));

        assert!(result.is_err());
    }

    // =========================================================================
    // Group 4: Schema-aware conversions (Schema::Undefined - passthrough)
    // =========================================================================

    #[test]
    fn json_bytes_with_undefined_schema_to_bson() {
        let json = br#"{"field": "data"}"#.to_vec();
        let schema = Schema::Undefined;
        let with_schema = JsonBytesWithSchema::new(json, &schema);

        let bson_bytes = BsonBytes::try_from(with_schema).unwrap();
        let doc: Document = bson_bytes.try_into().unwrap();

        assert_eq!(doc.get_str("field").unwrap(), "data");
    }

    #[test]
    fn json_bytes_with_undefined_schema_to_json() {
        let json = br#"{"field": "data"}"#.to_vec();
        let schema = Schema::Undefined;
        let with_schema = JsonBytesWithSchema::new(json.clone(), &schema);

        let result = JsonBytes::try_from(with_schema).unwrap();

        // Should be passthrough (same data)
        assert_eq!(result.0, json);
    }

    #[test]
    fn bson_bytes_with_undefined_schema_to_json() {
        let doc = doc! {"field": "value"};
        let bson_data = bson::to_vec(&doc).unwrap();
        let schema = Schema::Undefined;
        let with_schema = BsonBytesWithSchema::new(bson_data, &schema);

        let json_bytes = JsonBytes::try_from(with_schema).unwrap();
        let json_str = String::from_utf8(json_bytes.0).unwrap();

        assert!(json_str.contains("value"));
    }

    #[test]
    fn bson_bytes_with_undefined_schema_to_bson() {
        let doc = doc! {"field": "value"};
        let bson_data = bson::to_vec(&doc).unwrap();
        let schema = Schema::Undefined;
        let with_schema = BsonBytesWithSchema::new(bson_data.clone(), &schema);

        let result = BsonBytes::try_from(with_schema).unwrap();

        // Should be passthrough (same data)
        assert_eq!(result.0, bson_data);
    }

    // =========================================================================
    // Group 5: Schema-aware conversions (Schema::Avro)
    // =========================================================================

    fn test_avro_schema() -> AvroSchema {
        AvroSchema::parse_str(
            r#"{
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                ]
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn json_bytes_with_avro_schema_to_bson() {
        let json = br#"{"name": "alice", "age": 25}"#.to_vec();
        let avro_schema = test_avro_schema();
        let schema = Schema::Avro(avro_schema);
        let with_schema = JsonBytesWithSchema::new(json, &schema);

        let bson_bytes = BsonBytes::try_from(with_schema).unwrap();
        let doc: Document = bson_bytes.try_into().unwrap();

        assert_eq!(doc.get_str("name").unwrap(), "alice");
        assert_eq!(doc.get_i32("age").unwrap(), 25);
    }

    #[test]
    fn json_bytes_with_avro_schema_to_json() {
        let json = br#"{"name": "bob", "age": 30}"#.to_vec();
        let avro_schema = test_avro_schema();
        let schema = Schema::Avro(avro_schema);
        let with_schema = JsonBytesWithSchema::new(json, &schema);

        let json_bytes = JsonBytes::try_from(with_schema).unwrap();
        let doc: Document = JsonBytes(json_bytes.0).try_into().unwrap();

        assert_eq!(doc.get_str("name").unwrap(), "bob");
        assert_eq!(doc.get_i32("age").unwrap(), 30);
    }

    #[test]
    fn bson_bytes_with_avro_schema_to_json() {
        let doc = doc! {"name": "charlie", "age": 35};
        let bson_data = bson::to_vec(&doc).unwrap();
        let avro_schema = test_avro_schema();
        let schema = Schema::Avro(avro_schema);
        let with_schema = BsonBytesWithSchema::new(bson_data, &schema);

        let json_bytes = JsonBytes::try_from(with_schema).unwrap();
        let decoded: Document = JsonBytes(json_bytes.0).try_into().unwrap();

        assert_eq!(decoded.get_str("name").unwrap(), "charlie");
        assert_eq!(decoded.get_i32("age").unwrap(), 35);
    }

    #[test]
    fn bson_bytes_with_avro_schema_to_bson() {
        let doc = doc! {"name": "dave", "age": 40};
        let bson_data = bson::to_vec(&doc).unwrap();
        let avro_schema = test_avro_schema();
        let schema = Schema::Avro(avro_schema);
        let with_schema = BsonBytesWithSchema::new(bson_data, &schema);

        let bson_bytes = BsonBytes::try_from(with_schema).unwrap();
        let decoded: Document = bson_bytes.try_into().unwrap();

        assert_eq!(decoded.get_str("name").unwrap(), "dave");
        assert_eq!(decoded.get_i32("age").unwrap(), 40);
    }

    #[test]
    fn json_bytes_with_avro_schema_error_missing_field() {
        // Missing required "age" field
        let json = br#"{"name": "eve"}"#.to_vec();
        let avro_schema = test_avro_schema();
        let schema = Schema::Avro(avro_schema);
        let with_schema = JsonBytesWithSchema::new(json, &schema);

        let result = BsonBytes::try_from(with_schema);
        assert!(result.is_err());
    }

    #[test]
    fn json_bytes_with_avro_schema_error_wrong_type() {
        // "age" should be int, not string
        let json = br#"{"name": "frank", "age": "not a number"}"#.to_vec();
        let avro_schema = test_avro_schema();
        let schema = Schema::Avro(avro_schema);
        let with_schema = JsonBytesWithSchema::new(json, &schema);

        let result = BsonBytes::try_from(with_schema);
        assert!(result.is_err());
    }

    // =========================================================================
    // Group 6: Edge cases
    // =========================================================================

    #[test]
    fn json_bytes_empty_array() {
        let json = b"[]".to_vec();
        let docs: Vec<Document> = JsonBytes(json).try_into().unwrap();
        assert!(docs.is_empty());
    }

    #[test]
    fn json_bytes_empty_object() {
        let json = b"{}".to_vec();
        let doc: Document = JsonBytes(json).try_into().unwrap();
        assert!(doc.is_empty());
    }

    #[test]
    fn bson_bytes_empty_vec_error() {
        // BSON doesn't support top-level arrays (even empty ones)
        let docs: Vec<Document> = vec![];
        let result = BsonBytes::try_from(docs);
        assert!(result.is_err());
    }

    #[test]
    fn json_bytes_nested_document() {
        let json = br#"{"outer": {"inner": {"deep": 123}}}"#.to_vec();
        let doc: Document = JsonBytes(json).try_into().unwrap();

        let outer = doc.get_document("outer").unwrap();
        let inner = outer.get_document("inner").unwrap();
        assert_eq!(inner.get_i32("deep").unwrap(), 123);
    }

    #[test]
    fn json_bytes_with_array_field() {
        let json = br#"{"items": [1, 2, 3]}"#.to_vec();
        let doc: Document = JsonBytes(json).try_into().unwrap();

        let items = doc.get_array("items").unwrap();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn json_bytes_with_schema_new_constructor() {
        let data = b"{}".to_vec();
        let schema = Schema::Undefined;
        let with_schema = JsonBytesWithSchema::new(data.clone(), &schema);

        assert_eq!(with_schema.data, data);
    }

    #[test]
    fn bson_bytes_with_schema_new_constructor() {
        let data = vec![0x05, 0x00, 0x00, 0x00, 0x00]; // minimal empty BSON doc
        let schema = Schema::Undefined;
        let with_schema = BsonBytesWithSchema::new(data.clone(), &schema);

        assert_eq!(with_schema.data, data);
    }

    // =========================================================================
    // Proptest: Property-based round-trip tests
    // =========================================================================

    proptest! {
        #[test]
        fn proptest_json_bytes_roundtrip_string_values(
            value in "[a-zA-Z0-9]{0,50}"
        ) {
            let doc = doc! { "field": &value };

            let json_bytes = JsonBytes::try_from(vec![doc.clone()]).unwrap();
            let decoded: Vec<Document> = json_bytes.try_into().unwrap();

            prop_assert_eq!(decoded.len(), 1);
            prop_assert_eq!(decoded[0].get_str("field").unwrap(), value);
        }

        #[test]
        fn proptest_json_bytes_roundtrip_numeric_values(
            int_val in any::<i32>(),
            bool_val in any::<bool>()
        ) {
            // JSON doesn't preserve i32 vs i64 - use i64 for comparison
            let doc = doc! {
                "int_field": int_val,
                "bool_field": bool_val
            };

            let json_bytes = JsonBytes::try_from(vec![doc.clone()]).unwrap();
            let decoded: Vec<Document> = json_bytes.try_into().unwrap();

            prop_assert_eq!(decoded.len(), 1);
            // Use get_i64 since JSON numeric deserialization may produce i64
            let decoded_val = decoded[0].get_i32("int_field")
                .map(|v| v as i64)
                .or_else(|_| decoded[0].get_i64("int_field"))
                .unwrap();
            prop_assert_eq!(decoded_val, int_val as i64);
            prop_assert_eq!(decoded[0].get_bool("bool_field").unwrap(), bool_val);
        }

        #[test]
        fn proptest_json_to_bson_roundtrip(
            value in "[a-zA-Z0-9]{0,30}"
        ) {
            let json = format!(r#"{{"test_key": "{}"}}"#, value);
            let json_bytes = JsonBytes(json.into_bytes());

            // JSON -> BSON -> JSON
            let bson_bytes = BsonBytes::try_from(json_bytes).unwrap();
            let json_back = JsonBytes::try_from(bson_bytes).unwrap();

            let doc: Document = json_back.try_into().unwrap();
            prop_assert_eq!(doc.get_str("test_key").unwrap(), value);
        }

        #[test]
        fn proptest_bson_single_doc_roundtrip(
            str_val in "[a-zA-Z0-9]{0,30}",
            int_val in any::<i32>()
        ) {
            let doc = doc! { "str_key": &str_val, "int_key": int_val };
            let bson_bytes = bson::to_vec(&doc).unwrap();

            let decoded: Document = BsonBytes(bson_bytes).try_into().unwrap();

            prop_assert_eq!(decoded.get_str("str_key").unwrap(), str_val);
            prop_assert_eq!(decoded.get_i32("int_key").unwrap(), int_val);
        }

        #[test]
        fn proptest_json_bytes_vec_document_count_preserved(
            count in 0usize..20
        ) {
            let docs: Vec<Document> = (0..count)
                .map(|i| doc! { "index": i as i32 })
                .collect();

            let json_bytes = JsonBytes::try_from(docs.clone()).unwrap();
            let decoded: Vec<Document> = json_bytes.try_into().unwrap();

            prop_assert_eq!(decoded.len(), count);
        }

        #[test]
        fn proptest_json_bytes_with_undefined_schema_passthrough(
            data in "[a-zA-Z0-9]{1,20}"
        ) {
            let json = format!(r#"{{"data": "{}"}}"#, data);
            let json_vec = json.clone().into_bytes();
            let schema = Schema::Undefined;
            let with_schema = JsonBytesWithSchema::new(json_vec.clone(), &schema);

            let result = JsonBytes::try_from(with_schema).unwrap();

            // Undefined schema should pass through unchanged
            prop_assert_eq!(result.0, json_vec);
        }

        #[test]
        fn proptest_bson_bytes_with_undefined_schema_passthrough(
            val in any::<i32>()
        ) {
            let doc = doc! { "test_key": val };
            let bson_data = bson::to_vec(&doc).unwrap();
            let schema = Schema::Undefined;
            let with_schema = BsonBytesWithSchema::new(bson_data.clone(), &schema);

            let result = BsonBytes::try_from(with_schema).unwrap();

            // Undefined schema should pass through unchanged
            prop_assert_eq!(result.0, bson_data);
        }
    }
}
