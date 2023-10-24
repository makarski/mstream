use anyhow::{anyhow, Ok};
use apache_avro::Schema;
use async_trait::async_trait;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};

const SCHEMA_REGISTRY_COLLECTION: &str = "mstream_schemas";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    schema_id: String,
    schema_encoding: SchemaEncoding,
    schema_definition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SchemaEncoding {
    Avro,
}

#[async_trait]
pub trait SchemaProvider {
    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema>;
}

pub struct MongoDbSchemaProvider {
    db: mongodb::Database,
}

impl MongoDbSchemaProvider {
    pub async fn new(db: mongodb::Database) -> Self {
        Self { db }
    }
}

#[async_trait]
impl SchemaProvider for MongoDbSchemaProvider {
    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema> {
        let collection = self
            .db
            .collection::<SchemaEntry>(SCHEMA_REGISTRY_COLLECTION);

        let schema = collection
            .find_one(doc! {"schema_id": &id}, None)
            .await?
            .ok_or_else(|| anyhow!("schema not found: {}", id))?;

        Ok(Schema::parse_str(&schema.schema_definition)?)
    }
}
