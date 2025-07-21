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
pub struct JsonBatchBytes(pub Vec<JsonBytes>);
pub struct BsonBytes(pub Vec<u8>);
pub struct BsonBatchBytes(pub Vec<BsonBytes>);

impl TryInto<Vec<Document>> for BsonBatchBytes {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Vec<Document>, Self::Error> {
        self.0
            .into_iter()
            .map(|bson_bytes| bson::from_slice(&bson_bytes.0))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("failed to deserialize BSON batch: {}", e))
    }
}

impl TryInto<Vec<Document>> for JsonBatchBytes {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Vec<Document>, Self::Error> {
        self.0
            .into_iter()
            .map(|json_bytes| serde_json::from_slice(&json_bytes.0))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("failed to deserialize JSON batch: {}", e))
    }
}

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

// --- JSON Batch ---

pub struct JsonBatchBytesWithSchema<'a> {
    pub data: Vec<JsonBytesWithSchema<'a>>,
    pub schema: &'a Schema,
}

impl<'a> JsonBatchBytesWithSchema<'a> {
    pub fn new(data: Vec<Vec<u8>>, schema: &'a Schema) -> Self {
        let data = data
            .into_iter()
            .map(|d| JsonBytesWithSchema::new(d, schema))
            .collect();

        JsonBatchBytesWithSchema { data, schema }
    }
}

impl<'a> Iterator for JsonBatchBytesWithSchema<'a> {
    type Item = JsonBytesWithSchema<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.pop()
    }
}

impl TryFrom<JsonBatchBytesWithSchema<'_>> for JsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: JsonBatchBytesWithSchema) -> Result<Self, Self::Error> {
        match value.schema {
            Schema::Avro(_) => {
                let mut combined_data = vec![b'['];
                for item in value.data {
                    let json_b: JsonBytes = item.try_into()?;
                    combined_data.push(b',');
                    combined_data.extend(json_b.0);
                }
                combined_data.push(b']');
                Ok(JsonBytes(combined_data))
            }
            Schema::Undefined => {
                let mut combined_data = vec![b'['];

                for (i, item) in value.enumerate() {
                    if i > 0 {
                        combined_data.push(b',');
                    }

                    combined_data.push(b',');
                    combined_data.extend(item.data);
                }

                combined_data.push(b']');
                Ok(JsonBytes(combined_data))
            }
        }
    }
}

impl TryFrom<JsonBatchBytesWithSchema<'_>> for BsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: JsonBatchBytesWithSchema) -> Result<Self, Self::Error> {
        match value.schema {
            Schema::Avro(avro_schema) => {
                let mut combined_data = Vec::new();
                for item in value.data {
                    let bson_doc = JsonBytes(item.data).apply_avro_schema(avro_schema)?;
                    combined_data.push(bson_doc);
                }
                let combined_data = bson::to_vec(&combined_data)?;
                Ok(BsonBytes(combined_data))
            }
            Schema::Undefined => value.try_into().and_then(|jb: JsonBytes| jb.try_into()),
        }
    }
}

// --- BSON Batch ---

pub struct BsonBatchBytesWithSchema<'a> {
    pub data: Vec<BsonBytesWithSchema<'a>>,
    pub schema: &'a Schema,
}

impl<'a> BsonBatchBytesWithSchema<'a> {
    pub fn new(data: Vec<Vec<u8>>, schema: &'a Schema) -> Self {
        let data = data
            .into_iter()
            .map(|d| BsonBytesWithSchema::new(d, schema))
            .collect();

        BsonBatchBytesWithSchema { data, schema }
    }
}

impl<'a> Iterator for BsonBatchBytesWithSchema<'a> {
    type Item = BsonBytesWithSchema<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.pop()
    }
}

impl TryFrom<BsonBatchBytesWithSchema<'_>> for JsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: BsonBatchBytesWithSchema) -> Result<Self, Self::Error> {
        match value.schema {
            Schema::Avro(avro_schema) => {
                let mut combined_data = Vec::new();
                for item in value.data {
                    let bson_doc = BsonBytes(item.data).apply_avro_schema(avro_schema)?;
                    combined_data.push(bson_doc);
                }
                let combined_data = serde_json::to_vec(&combined_data)?;
                Ok(JsonBytes(combined_data))
            }
            Schema::Undefined => {
                let mut combined_data = vec![b'['];

                for (i, item) in value.enumerate() {
                    if i > 0 {
                        combined_data.push(b',');
                    }
                    let json_b: JsonBytes = BsonBytes(item.data).try_into()?;
                    combined_data.extend(json_b.0);
                }

                combined_data.push(b']');
                Ok(JsonBytes(combined_data))
            }
        }
    }
}

impl TryFrom<BsonBatchBytesWithSchema<'_>> for BsonBytes {
    type Error = anyhow::Error;

    fn try_from(value: BsonBatchBytesWithSchema) -> Result<Self, Self::Error> {
        match value.schema {
            Schema::Avro(avro_schema) => {
                let mut combined_data = Vec::new();
                for item in value.data {
                    let bson_doc = BsonBytes(item.data).apply_avro_schema(avro_schema)?;
                    combined_data.push(bson_doc);
                }
                let combined_data = bson::to_vec(&combined_data)?;
                Ok(BsonBytes(combined_data))
            }
            Schema::Undefined => {
                let json_array: JsonBytes = value.try_into()?;
                json_array
                    .try_into()
                    .map_err(|e| anyhow::anyhow!("failed to convert JSON batch to BSON: {}", e))
            }
        }
    }
}
