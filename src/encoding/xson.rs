use core::convert::TryFrom;

use apache_avro::Schema as AvroSchema;
use mongodb::bson::{self, Document};

use crate::encoding::avro;
use crate::schema::Schema;

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
        Ok(JsonBytes(serde_json::to_vec(&doc)?))
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
                Ok(JsonBytes(serde_json::to_vec(&bson_doc)?))
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
