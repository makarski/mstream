use anyhow::bail;
use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use mongo::MongoDbSchemaProvider;
use serde_json::Value as JsonValue;

use crate::{
    config::Encoding,
    pubsub::{ServiceAccountAuth, srvc::SchemaService},
};

pub mod convert;
pub mod encoding;
pub mod introspect;
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

#[derive(Debug, Clone, Default)]
pub enum Schema {
    #[default]
    Undefined,
    Avro(AvroSchema),
    Json(JsonValue),
}

impl Schema {
    pub fn parse(definition: &str, encoding: Encoding) -> anyhow::Result<Self> {
        let parsed = match encoding {
            Encoding::Avro => Self::Avro(AvroSchema::parse_str(definition)?),
            Encoding::Json => Self::Json(serde_json::from_str(definition)?),
            _ => Self::Undefined,
        };

        Ok(parsed)
    }

    pub fn try_as_avro(&self) -> anyhow::Result<&AvroSchema> {
        match self {
            Self::Avro(schema) => Ok(schema),
            _ => bail!("schema is not Avro"),
        }
    }
}

#[async_trait]
pub trait SchemaRegistry {
    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema>;
}
