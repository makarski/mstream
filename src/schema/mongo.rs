use async_trait::async_trait;
use mongodb::bson::doc;
use tokio_stream::StreamExt;

use super::{SchemaEntry, SchemaEntrySummary, SchemaRegistry, SchemaRegistryError};

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

    fn collection(&self) -> mongodb::Collection<SchemaEntry> {
        self.db.collection(&self.collection_name)
    }
}

impl MongoDbSchemaProvider {
    pub async fn get(&self, id: &str) -> Result<SchemaEntry, SchemaRegistryError> {
        self.collection()
            .find_one(doc! { "id": id })
            .await?
            .ok_or_else(|| SchemaRegistryError::NotFound(id.to_string()))
    }

    pub async fn list(&self) -> Result<Vec<SchemaEntrySummary>, SchemaRegistryError> {
        let mut cursor = self.collection().find(doc! {}).await?;
        let mut entries = Vec::new();

        while let Some(entry) = cursor.try_next().await? {
            entries.push(SchemaEntrySummary {
                id: entry.id,
                name: entry.name,
                encoding: entry.encoding,
            });
        }

        Ok(entries)
    }

    pub async fn save(&self, entry: &SchemaEntry) -> Result<(), SchemaRegistryError> {
        let filter = doc! { "id": &entry.id };
        let update = doc! { "$set": mongodb::bson::to_bson(entry).map_err(|e| SchemaRegistryError::Other(e.to_string()))? };

        self.collection()
            .update_one(filter, update)
            .upsert(true)
            .await?;

        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), SchemaRegistryError> {
        self.collection().delete_one(doc! { "id": id }).await?;
        Ok(())
    }
}

#[async_trait]
impl SchemaRegistry for MongoDbSchemaProvider {
    async fn get(&self, id: &str) -> Result<SchemaEntry, SchemaRegistryError> {
        MongoDbSchemaProvider::get(self, id).await
    }

    async fn list(&self) -> Result<Vec<SchemaEntrySummary>, SchemaRegistryError> {
        MongoDbSchemaProvider::list(self).await
    }

    async fn save(&self, entry: &SchemaEntry) -> Result<(), SchemaRegistryError> {
        MongoDbSchemaProvider::save(self, entry).await
    }

    async fn delete(&self, id: &str) -> Result<(), SchemaRegistryError> {
        MongoDbSchemaProvider::delete(self, id).await
    }
}
