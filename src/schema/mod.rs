use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use mongo::MongoDbSchemaProvider;

use crate::{
    config::Encoding,
    pubsub::{srvc::SchemaService, ServiceAccountAuth},
};
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

#[derive(Debug, Clone)]
pub enum Schema {
    Undefined,
    Avro(AvroSchema),
    // Json(String),
}

impl Schema {
    pub fn parse(defintion: &str, encoding: Encoding) -> anyhow::Result<Self> {
        let parsed = match encoding {
            Encoding::Avro => Self::Avro(AvroSchema::parse_str(defintion)?),
            // Encoding::Json => Self::Json(defintion.to_string()),
            _ => Self::Undefined,
        };

        Ok(parsed)
    }

    pub fn as_avro(&self) -> Option<&AvroSchema> {
        match self {
            Self::Avro(schema) => Some(schema),
            _ => None,
        }
    }
}

#[async_trait]
pub trait SchemaRegistry {
    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema>;
}
