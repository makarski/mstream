use anyhow::{anyhow, bail, Ok};
use log::debug;
use mongodb::bson::{self};

use super::Schema;
use crate::config::Encoding;
use crate::encoding::{avro, json_to_bson_doc};

pub enum SchemaEncoder {
    SingleEvent(Vec<u8>),
    Batch(Vec<Vec<u8>>),
}

impl SchemaEncoder {
    pub fn new_event(b: Vec<u8>) -> Self {
        SchemaEncoder::SingleEvent(b)
    }

    pub fn new_batch(b: Vec<Vec<u8>>) -> Self {
        SchemaEncoder::Batch(b)
    }

    pub fn apply_schema(
        self,
        from_encoding: &Encoding,
        target_encoding: &Encoding,
        item_schema: &Schema,
    ) -> anyhow::Result<Vec<u8>> {
        match (from_encoding, target_encoding) {
            (Encoding::Other, Encoding::Other) => match self {
                SchemaEncoder::SingleEvent(b) => Ok(b),
                SchemaEncoder::Batch(batch) => {
                    // For batch, use a a length-prefixed format
                    // Format:
                    // - 4 bytes: length of the batch (u32 in little-endian)
                    // - For each event in the batch:
                    //     - 4 bytes: length of the event (u32 in little-endian)
                    //     - N bytes: the event itself in original encoding

                    let message_count = batch.len() as u32;
                    let overhead_size = 4 + 4 * message_count;
                    let payload_size = batch.iter().map(|p| p.len()).sum::<usize>();
                    let total_size = overhead_size as usize + payload_size;

                    let mut result = Vec::with_capacity(total_size);

                    // Write 4 bytes: message count
                    result.extend_from_slice(&(message_count).to_le_bytes());

                    for event_bytes in batch {
                        let event_len = event_bytes.len() as u32;
                        // Write 4 bytes: event length
                        result.extend_from_slice(&event_len.to_le_bytes());
                        // Write N bytes: the event itself
                        result.extend_from_slice(&event_bytes);
                    }

                    Ok(result)
                }
            },
            (Encoding::Other, Encoding::Avro | Encoding::Bson | Encoding::Json) => Err(anyhow!(
                "conversion from Other to Avro/Bson/Json is not supported"
            )),
            (Encoding::Avro | Encoding::Json | Encoding::Bson, Encoding::Other) => Err(anyhow!(
                "conversion from Avro/Bson/Json to Other is not supported"
            )),
            (Encoding::Avro, _) => self.from_avro(&target_encoding, item_schema),
            (Encoding::Json | Encoding::Bson, Encoding::Avro) => {
                self.to_avro(&from_encoding, item_schema)
            }
            (Encoding::Json | Encoding::Bson, Encoding::Json | Encoding::Bson) => {
                debug!(
                    "entered exchange. from: {:?}. to: {:?}",
                    from_encoding, target_encoding
                );

                self.exchange_json_bson(&from_encoding, &target_encoding, item_schema)
            }
        }
        .map_err(|err| {
            anyhow!(
                "{}: from_encoding: {:?}, target_encoding: {:?}",
                err,
                from_encoding,
                target_encoding
            )
        })
    }

    fn from_avro(self, target_encoding: &Encoding, schema: &Schema) -> anyhow::Result<Vec<u8>> {
        let avro_schema = schema.as_avro().ok_or_else(|| {
            anyhow!("failed to retrieve underlying avro schema while converting from avro")
        })?;

        match self {
            SchemaEncoder::SingleEvent(b) => match target_encoding {
                Encoding::Avro => Ok(avro::validate(b, &avro_schema)?),
                Encoding::Json => {
                    let bson_doc = avro::decode(&b, avro_schema)?;
                    serde_json::to_vec(&bson_doc).map_err(|e| {
                        anyhow!("from_avro: failed to serialize bson doc to json: {}", e)
                    })
                }
                Encoding::Bson => {
                    let bson_doc = avro::decode(&b, avro_schema)?;
                    bson::to_vec(&bson_doc).map_err(|e| {
                        anyhow!("from_avro: failed to serialize bson doc to bson: {}", e)
                    })
                }
                Encoding::Other => bail!("unsupported target encoding: {:?}", target_encoding),
            },
            SchemaEncoder::Batch(batch) => {
                let doc_array = bson_enc::docs_from_avro(batch, avro_schema)?;

                match target_encoding {
                    Encoding::Avro => {
                        let avro_doc = avro::encode_many(doc_array, &avro_schema)?;
                        return Ok(avro_doc);
                    }
                    Encoding::Json | Encoding::Bson => {
                        bson_enc::docs_to_json_or_bson(doc_array, target_encoding).map_err(|e| {
                            anyhow!(
                                "from_avro: failed to serialize bson doc to json/bson. target: {:?}. error: {}", target_encoding,
                                e
                            )
                        })
                    }
                    Encoding::Other => bail!("unsupported target encoding: {:?}", target_encoding),
                }
            }
        }
    }

    fn to_avro(self, from_encoding: &Encoding, schema: &Schema) -> anyhow::Result<Vec<u8>> {
        let avro_schema = schema.as_avro().ok_or_else(|| {
            anyhow!("failed to retrieve underlying avro schema while converting to avro")
        })?;

        match self {
            SchemaEncoder::SingleEvent(b) => match from_encoding {
                Encoding::Avro => Ok(avro::validate(b, &avro_schema)?),
                Encoding::Json | Encoding::Bson => {
                    let bson_doc = if *from_encoding == Encoding::Json {
                        json_to_bson_doc(&b)?
                    } else {
                        bson::from_slice::<bson::Document>(&b)?
                    };

                    avro::encode(bson_doc, &avro_schema)
                }
                Encoding::Other => bail!("unsupported source encoding: {:?}", from_encoding),
            },
            SchemaEncoder::Batch(batch) => match from_encoding {
                Encoding::Avro => avro::validate_many(batch, &avro_schema),
                Encoding::Json | Encoding::Bson => {
                    let doc_array = bson_enc::docs_from_json_or_bson(batch, from_encoding)?;
                    avro::encode_many(doc_array, &avro_schema)
                }
                Encoding::Other => bail!("unsupported source encoding: {:?}", from_encoding),
            },
        }
    }

    fn exchange_json_bson(
        self,
        from_encoding: &Encoding,
        target_encoding: &Encoding,
        schema: &Schema,
    ) -> anyhow::Result<Vec<u8>> {
        match self {
            SchemaEncoder::SingleEvent(b) => {
                exchange_json_bson_for_one(b, from_encoding, target_encoding, schema)
            }
            SchemaEncoder::Batch(batch) => match schema {
                Schema::Avro(avro_schema) => {
                    let encoded = batch
                        .into_iter()
                        .map(|b| {
                            bson_enc::apply_avro_schema_for_json_or_bson(
                                b.clone(),
                                from_encoding,
                                avro_schema,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .and_then(|docs| bson_enc::docs_to_json_or_bson(docs, target_encoding))?;

                    Ok(encoded)
                }
                Schema::Undefined => {
                    let doc_array = bson_enc::docs_from_json_or_bson(batch, from_encoding)?;

                    if *target_encoding == Encoding::Bson {
                        bson_enc::docs_to_vec(doc_array).map_err(|e| {
                            anyhow!(
                                "batch from_avro: failed to serialize bson doc to bson: {}",
                                e
                            )
                        })
                    } else {
                        serde_json::to_vec(&doc_array).map_err(|e| {
                            anyhow!(
                                "batch from_avro: failed to serialize bson doc to json: {}",
                                e
                            )
                        })
                    }
                }
            },
        }
    }
}

fn exchange_json_bson_for_one(
    b: Vec<u8>,
    from_encoding: &Encoding,
    target_encoding: &Encoding,
    schema: &Schema,
) -> anyhow::Result<Vec<u8>> {
    let bson_doc = match schema {
        // expensive
        Schema::Avro(avro_schema) => {
            bson_enc::apply_avro_schema_for_json_or_bson(b, from_encoding, avro_schema)?
        }
        // if schema is undefined, we can just convert between json and bson
        Schema::Undefined => bson_enc::doc_from_json_or_bson(b, from_encoding)?,
    };

    bson_enc::doc_to_json_or_bson(bson_doc, target_encoding).map_err(|e| {
        anyhow!(
            "failed to serialize bson<>json exchange: from: {:?}, to: {:?}, error: {}",
            from_encoding,
            target_encoding,
            e
        )
    })
}

mod bson_enc {
    use anyhow::bail;
    use mongodb::bson::Document;

    use super::bson;
    use crate::{
        config::Encoding,
        encoding::{avro, json_to_bson_doc},
    };

    pub(super) fn docs_from_avro(
        batch: Vec<Vec<u8>>,
        avro_schema: &apache_avro::Schema,
    ) -> anyhow::Result<Vec<Document>> {
        let mut doc_array = Vec::with_capacity(batch.len());

        for event in batch {
            let bson_doc = avro::decode(&event, avro_schema)?;
            doc_array.push(bson_doc);
        }

        Ok(doc_array)
    }

    pub(super) fn docs_from_json_or_bson(
        batch: Vec<Vec<u8>>,
        from_encoding: &Encoding,
    ) -> anyhow::Result<Vec<bson::Document>> {
        let mut doc_array = Vec::with_capacity(batch.len());

        for event in batch {
            let bson_doc = doc_from_json_or_bson(event, from_encoding)?;
            doc_array.push(bson_doc);
        }

        Ok(doc_array)
    }

    pub(super) fn doc_from_json_or_bson(
        b: Vec<u8>,
        from_encoding: &Encoding,
    ) -> anyhow::Result<bson::Document> {
        let doc = if *from_encoding == Encoding::Json {
            json_to_bson_doc(&b)?
        } else {
            bson::from_slice::<bson::Document>(&b)?
        };

        Ok(doc)
    }

    pub(super) fn doc_to_json_or_bson(
        doc: Document,
        target_encoding: &Encoding,
    ) -> anyhow::Result<Vec<u8>> {
        let b = match target_encoding {
            Encoding::Json => serde_json::to_vec(&doc)?,
            Encoding::Bson => bson::to_vec(&doc)?,
            _ => bail!("unsupported target encoding: {:?}", target_encoding),
        };

        Ok(b)
    }

    pub(super) fn docs_to_json_or_bson(
        docs: Vec<Document>,
        target_encoding: &Encoding,
    ) -> anyhow::Result<Vec<u8>> {
        let encoded = match target_encoding {
            Encoding::Json => serde_json::to_vec(&docs)?,
            Encoding::Bson => docs_to_vec(docs)?,
            _ => bail!("unsupported target encoding: {:?}", target_encoding),
        };

        Ok(encoded)
    }

    pub(super) fn docs_to_vec(docs: Vec<Document>) -> anyhow::Result<Vec<u8>> {
        // BSON specification itself supports top-level arrays.
        // but the Rust bson crate (bson::to_vec) only allows top-level serialization of documents (i.e., structs or maps), not arrays.
        // this is a library-specific limitation, not a BSON limitation.
        let array_wrapper = bson::doc! {"items": docs};
        Ok(bson::to_vec(&array_wrapper)?)
    }

    pub(super) fn apply_avro_schema_for_json_or_bson(
        b: Vec<u8>,
        from_encoding: &Encoding,
        avro_schema: &apache_avro::Schema,
    ) -> anyhow::Result<Document> {
        // todo: think of a better way apply an avro schema
        // this is expensive
        let bson_doc = doc_from_json_or_bson(b, from_encoding)?;
        let avro_doc = avro::encode(bson_doc, avro_schema)?;
        avro::decode(&avro_doc, avro_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Encoding;
    use crate::schema::Schema;
    use apache_avro::Schema as AvroSchema;
    use mongodb::bson::{self, doc};

    // Helper function to create a sample Avro schema
    fn create_test_avro_schema() -> Schema {
        let raw_schema = r#"
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "value", "type": "int"}
            ]
        }
        "#;

        let avro_schema = AvroSchema::parse_str(raw_schema).unwrap();
        Schema::Avro(avro_schema)
    }

    // Helper to create valid Avro encoded data
    fn create_avro_data() -> Vec<u8> {
        // This would normally be actual Avro-encoded data
        // For testing purposes, we'll use a mock implementation
        let schema = create_test_avro_schema();
        let bson_doc = doc! {"id": "test123", "value": 42};

        match &schema {
            Schema::Avro(avro_schema) => avro::encode(bson_doc, avro_schema).unwrap(),
            _ => panic!("Expected Avro schema"),
        }
    }

    // Helper to create valid JSON encoded data
    fn create_json_data() -> Vec<u8> {
        serde_json::to_vec(&doc! {"id": "test123", "value": 42}).unwrap()
    }

    // Helper to create valid BSON encoded data
    fn create_bson_data() -> Vec<u8> {
        bson::to_vec(&doc! {"id": "test123", "value": 42}).unwrap()
    }

    #[test]
    fn test_other_to_other_encoding() {
        let data = b"some arbitrary binary data".to_vec();
        let schema = Schema::Undefined;

        let result = SchemaEncoder::new_event(data.clone()).apply_schema(
            &Encoding::Other,
            &Encoding::Other,
            &schema,
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_other_to_structured_format() {
        let data = b"some arbitrary binary data".to_vec();
        let schema = Schema::Undefined;

        // Test Other -> Avro
        let result = SchemaEncoder::new_event(data.clone()).apply_schema(
            &Encoding::Other,
            &Encoding::Avro,
            &schema,
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("conversion from Other to Avro/Bson/Json is not supported"));

        // Test Other -> Json
        let result = SchemaEncoder::new_event(data.clone()).apply_schema(
            &Encoding::Other,
            &Encoding::Json,
            &schema,
        );

        assert!(result.is_err());

        // Test Other -> Bson
        let result = SchemaEncoder::new_event(data.clone()).apply_schema(
            &Encoding::Other,
            &Encoding::Bson,
            &schema,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_structured_to_other_format() {
        let avro_data = create_avro_data();
        let json_data = create_json_data();
        let bson_data = create_bson_data();
        let schema = create_test_avro_schema();

        // Test Avro -> Other
        let result = SchemaEncoder::new_event(avro_data).apply_schema(
            &Encoding::Avro,
            &Encoding::Other,
            &schema,
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("conversion from Avro/Bson/Json to Other is not supported"));

        // Test Json -> Other
        let result = SchemaEncoder::new_event(json_data).apply_schema(
            &Encoding::Json,
            &Encoding::Other,
            &schema,
        );

        assert!(result.is_err());

        // Test Bson -> Other
        let result = SchemaEncoder::new_event(bson_data).apply_schema(
            &Encoding::Bson,
            &Encoding::Other,
            &schema,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_avro_to_other_formats() {
        let avro_data = create_avro_data();
        let schema = create_test_avro_schema();
        let expected_id = "test123";
        let expected_value = 42;

        // Test Avro -> Json
        let result = SchemaEncoder::new_event(avro_data.clone()).apply_schema(
            &Encoding::Avro,
            &Encoding::Json,
            &schema,
        );
        assert!(result.is_ok());

        // Verify JSON content
        let json_bytes = result.unwrap();
        let json_doc: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(json_doc["id"].as_str().unwrap(), expected_id);
        assert_eq!(json_doc["value"].as_i64().unwrap(), expected_value as i64);

        // Test Avro -> Bson
        let result = SchemaEncoder::new_event(avro_data.clone()).apply_schema(
            &Encoding::Avro,
            &Encoding::Bson,
            &schema,
        );
        assert!(result.is_ok());

        // Verify BSON content
        let bson_bytes = result.unwrap();
        let bson_doc = bson::from_slice::<bson::Document>(&bson_bytes).unwrap();
        assert_eq!(bson_doc.get_str("id").unwrap(), expected_id);
        assert_eq!(bson_doc.get_i32("value").unwrap(), expected_value);

        // Test Avro -> Avro (validation)
        let result = SchemaEncoder::new_event(avro_data.clone()).apply_schema(
            &Encoding::Avro,
            &Encoding::Avro,
            &schema,
        );
        assert!(result.is_ok());

        // Verify Avro content is unchanged
        let validated_avro = result.unwrap();
        assert_eq!(
            validated_avro, avro_data,
            "Avro validation should not change the data"
        );

        // Further verify by decoding back to a document
        match &schema {
            Schema::Avro(avro_schema) => {
                let decoded = avro::decode(&validated_avro, avro_schema).unwrap();
                assert_eq!(decoded.get_str("id").unwrap(), expected_id);
                assert_eq!(decoded.get_i32("value").unwrap(), expected_value);
            }
            _ => panic!("Expected Avro schema"),
        }
    }

    #[test]
    fn test_json_bson_to_avro() {
        let json_data = create_json_data();
        let bson_data = create_bson_data();
        let schema = create_test_avro_schema();
        let expected_id = "test123";
        let expected_value = 42;

        // Test Json -> Avro
        let result = SchemaEncoder::new_event(json_data.clone()).apply_schema(
            &Encoding::Json,
            &Encoding::Avro,
            &schema,
        );
        assert!(result.is_ok());

        // Verify Avro content by decoding it back
        let avro_from_json = result.unwrap();
        match &schema {
            Schema::Avro(avro_schema) => {
                let decoded = avro::decode(&avro_from_json, avro_schema).unwrap();
                assert_eq!(decoded.get_str("id").unwrap(), expected_id);
                assert_eq!(decoded.get_i32("value").unwrap(), expected_value);

                // Verify all fields from original JSON are present
                let json_doc: serde_json::Value = serde_json::from_slice(&json_data).unwrap();
                for key in json_doc.as_object().unwrap().keys() {
                    assert!(
                        decoded.contains_key(key),
                        "Missing field {key} in decoded Avro"
                    );
                }
            }
            _ => panic!("Expected Avro schema"),
        }

        // Test Bson -> Avro
        let result = SchemaEncoder::new_event(bson_data.clone()).apply_schema(
            &Encoding::Bson,
            &Encoding::Avro,
            &schema,
        );
        assert!(result.is_ok());

        // Verify Avro content by decoding it back
        let avro_from_bson = result.unwrap();
        match &schema {
            Schema::Avro(avro_schema) => {
                let decoded = avro::decode(&avro_from_bson, avro_schema).unwrap();
                assert_eq!(decoded.get_str("id").unwrap(), expected_id);
                assert_eq!(decoded.get_i32("value").unwrap(), expected_value);

                // Verify all fields from original BSON are present
                let bson_doc = bson::from_slice::<bson::Document>(&bson_data).unwrap();
                for key in bson_doc.keys() {
                    assert!(
                        decoded.contains_key(key),
                        "Missing field {key} in decoded Avro"
                    );
                }
            }
            _ => panic!("Expected Avro schema"),
        }

        // Cross-verify that Avro encoded from both sources is equivalent
        // This requires decoding both to check semantically equivalent content
        match &schema {
            Schema::Avro(avro_schema) => {
                let json_decoded = avro::decode(&avro_from_json, avro_schema).unwrap();
                let bson_decoded = avro::decode(&avro_from_bson, avro_schema).unwrap();
                assert_eq!(json_decoded.get_str("id"), bson_decoded.get_str("id"));
                assert_eq!(json_decoded.get_i32("value"), bson_decoded.get_i32("value"));
            }
            _ => panic!("Expected Avro schema"),
        }
    }

    #[test]
    fn test_json_to_bson_conversion() {
        let json_data = create_json_data();
        let expected_id = "test123";
        let expected_value = 42;

        // Test with Avro schema
        let avro_schema = create_test_avro_schema();
        let result = SchemaEncoder::new_event(json_data.clone()).apply_schema(
            &Encoding::Json,
            &Encoding::Bson,
            &avro_schema,
        );
        assert!(result.is_ok());

        // Verify BSON content with Avro schema path
        let bson_bytes = result.unwrap();
        let bson_doc = bson::from_slice::<bson::Document>(&bson_bytes).unwrap();
        assert_eq!(bson_doc.get_str("id").unwrap(), expected_id);
        assert_eq!(bson_doc.get_i32("value").unwrap(), expected_value);

        // Test with Undefined schema
        let undefined_schema = Schema::Undefined;
        let result = SchemaEncoder::new_event(json_data.clone()).apply_schema(
            &Encoding::Json,
            &Encoding::Bson,
            &undefined_schema,
        );
        assert!(result.is_ok());

        // Verify BSON content with Undefined schema path
        let bson_bytes = result.unwrap();
        let bson_doc = bson::from_slice::<bson::Document>(&bson_bytes).unwrap();
        assert_eq!(bson_doc.get_str("id").unwrap(), expected_id);
        assert_eq!(bson_doc.get_i32("value").unwrap(), expected_value);

        // Verify field count to ensure all fields were preserved
        let json_doc: serde_json::Value = serde_json::from_slice(&json_data).unwrap();
        assert_eq!(
            bson_doc.keys().count(),
            json_doc.as_object().unwrap().keys().count(),
            "Field count mismatch between JSON and BSON"
        );

        // Create additional test with complex nested document
        let complex_json = serde_json::to_vec(&serde_json::json!({
            "id": "complex123",
            "nested": {
                "field1": "value1",
                "field2": 123,
                "array": [1, 2, 3]
            },
            "tags": ["tag1", "tag2"]
        }))
        .unwrap();

        let result = SchemaEncoder::new_event(complex_json.clone()).apply_schema(
            &Encoding::Json,
            &Encoding::Bson,
            &undefined_schema,
        );
        assert!(result.is_ok());

        // Verify complex BSON content
        let complex_bson = result.unwrap();
        let complex_doc = bson::from_slice::<bson::Document>(&complex_bson).unwrap();
        assert_eq!(complex_doc.get_str("id").unwrap(), "complex123");

        // Verify nested document
        let nested = complex_doc.get_document("nested").unwrap();
        assert_eq!(nested.get_str("field1").unwrap(), "value1");
        assert_eq!(nested.get_i32("field2").unwrap(), 123);

        // Verify arrays
        let nested_array = nested.get_array("array").unwrap();
        assert_eq!(nested_array.len(), 3);
        assert_eq!(nested_array[0].as_i32().unwrap(), 1);

        let tags = complex_doc.get_array("tags").unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].as_str().unwrap(), "tag1");
        assert_eq!(tags[1].as_str().unwrap(), "tag2");
    }

    // Add a new test for BSON to JSON conversion
    #[test]
    fn test_bson_to_json_conversion() {
        let bson_data = create_bson_data();
        let expected_id = "test123";
        let expected_value = 42;

        // Test with both schema types
        for schema in [create_test_avro_schema(), Schema::Undefined].iter() {
            let result = SchemaEncoder::new_event(bson_data.clone()).apply_schema(
                &Encoding::Bson,
                &Encoding::Json,
                schema,
            );
            assert!(result.is_ok());

            // Verify JSON content
            let json_bytes = result.unwrap();
            let json_doc: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
            assert_eq!(json_doc["id"].as_str().unwrap(), expected_id);
            assert_eq!(json_doc["value"].as_i64().unwrap(), expected_value as i64);
        }

        // Test with complex nested BSON document
        let complex_bson = bson::to_vec(&doc! {
            "id": "complex123",
            "nested": doc! {
                "field1": "value1",
                "field2": 123,
                "array": [1, 2, 3]
            },
            "tags": ["tag1", "tag2"],
            "binary": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![1, 2, 3] },
            "timestamp": bson::Timestamp { time: 123456789, increment: 1 }
        }).unwrap();

        let result = SchemaEncoder::new_event(complex_bson).apply_schema(
            &Encoding::Bson,
            &Encoding::Json,
            &Schema::Undefined,
        );
        assert!(result.is_ok());

        // Verify complex JSON content and special BSON type handling
        let complex_json = result.unwrap();
        let json_doc: serde_json::Value = serde_json::from_slice(&complex_json).unwrap();
        assert_eq!(json_doc["id"].as_str().unwrap(), "complex123");

        // Check nested document in JSON
        assert!(json_doc["nested"].is_object());
        assert_eq!(json_doc["nested"]["field1"].as_str().unwrap(), "value1");
        assert_eq!(json_doc["nested"]["field2"].as_i64().unwrap(), 123);

        // Check arrays
        assert!(json_doc["nested"]["array"].is_array());
        assert_eq!(json_doc["nested"]["array"][0].as_i64().unwrap(), 1);

        assert!(json_doc["tags"].is_array());
        assert_eq!(json_doc["tags"][0].as_str().unwrap(), "tag1");
        assert_eq!(json_doc["tags"][1].as_str().unwrap(), "tag2");

        // Verify special BSON types - the exact format depends on mongodb's serialization
        // Instead of checking the specific structure, just verify the fields exist
        assert!(
            json_doc.get("binary").is_some(),
            "Binary field should exist in JSON"
        );
        assert!(
            json_doc.get("timestamp").is_some(),
            "Timestamp field should exist in JSON"
        );

        // If you need to specifically check the structure, first understand the actual format:
        println!("Binary as JSON: {:?}", json_doc["binary"]);
        println!("Timestamp as JSON: {:?}", json_doc["timestamp"]);

        // Verify BSON binary type (serialized as array of bytes)
        assert!(json_doc["binary"].is_array());
        let binary_array = json_doc["binary"].as_array().unwrap();
        assert_eq!(binary_array.len(), 3);
        assert_eq!(binary_array[0].as_i64().unwrap(), 1);
        assert_eq!(binary_array[1].as_i64().unwrap(), 2);
        assert_eq!(binary_array[2].as_i64().unwrap(), 3);

        // Verify BSON timestamp type (serialized as nested object)
        assert!(json_doc["timestamp"].is_object());
        assert!(json_doc["timestamp"].get("$timestamp").is_some());

        let timestamp = &json_doc["timestamp"]["$timestamp"];
        assert!(timestamp.is_object());
        assert_eq!(timestamp["t"].as_i64().unwrap(), 123456789);
        assert_eq!(timestamp["i"].as_i64().unwrap(), 1);
    }

    #[test]
    fn test_same_encoding_with_identity() {
        let json_data = create_json_data();
        let schema = Schema::Undefined;

        // Json -> Json should be identity with undefined schema
        let result = SchemaEncoder::new_event(json_data.clone()).apply_schema(
            &Encoding::Json,
            &Encoding::Json,
            &schema,
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json_data);
    }

    #[test]
    fn test_error_handling_with_invalid_data() {
        let invalid_json = b"{not valid json}".to_vec();
        let schema = Schema::Undefined;

        let result = SchemaEncoder::new_event(invalid_json).apply_schema(
            &Encoding::Json,
            &Encoding::Bson,
            &schema,
        );

        assert!(result.is_err());
    }
}
