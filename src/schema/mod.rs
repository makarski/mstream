use apache_avro::Schema;
use async_trait::async_trait;
use mongo::MongoDbSchemaProvider;

use crate::pubsub::{srvc::SchemaService, ServiceAccountAuth};
pub mod mongo;

pub enum SchemaProvider {
    PubSub(SchemaService<ServiceAccountAuth>),
    MongoDb(MongoDbSchemaProvider),
}

#[async_trait]
impl SchemaRegistry for SchemaProvider {
    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema> {
        match self {
            SchemaProvider::PubSub(sp) => sp.get_schema(id).await,
            SchemaProvider::MongoDb(sp) => sp.get_schema(id).await,
        }
    }
}

#[async_trait]
pub trait SchemaRegistry {
    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema>;
}
