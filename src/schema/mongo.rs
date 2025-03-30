use anyhow::anyhow;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};

use crate::config::Encoding;

use super::Schema;

pub const SCHEMA_REGISTRY_COLLECTION: &str = "mstream_schemas";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaEntry {
    schema_id: String,
    schema_encoding: Encoding,
    schema_definition: String,
}

pub struct MongoDbSchemaProvider {
    db: mongodb::Database,
    collection_name: String,
}

impl MongoDbSchemaProvider {
    pub fn new(db: mongodb::Database, collection_name: String) -> Self {
        Self {
            db,
            collection_name,
        }
    }
}

impl MongoDbSchemaProvider {
    pub async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema> {
        let collection = self
            .db
            .collection::<SchemaEntry>(self.collection_name.as_str());

        let schema = collection
            .find_one(doc! {"schema_id": &id}, None)
            .await?
            .ok_or_else(|| anyhow!("schema not found: {}", id))?;

        Schema::parse(&schema.schema_definition, schema.schema_encoding)
    }
}
