use anyhow::anyhow;
use apache_avro::Schema;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use tonic::async_trait;

use super::SchemaRegistry;

const SCHEMA_REGISTRY_COLLECTION: &str = "mstream_schemas";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaEntry {
    schema_id: String,
    schema_encoding: SchemaEncoding,
    schema_definition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SchemaEncoding {
    Avro,
}

pub struct MongoDbSchemaProvider {
    db: mongodb::Database,
}

impl MongoDbSchemaProvider {
    pub fn new(db: mongodb::Database) -> Self {
        Self { db }
    }
}

#[async_trait]
impl SchemaRegistry for MongoDbSchemaProvider {
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
