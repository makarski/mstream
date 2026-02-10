use std::sync::Arc;

use anyhow::bail;
use apache_avro::Schema as AvroSchema;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mongo::MongoDbSchemaProvider;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    config::Encoding,
    pubsub::{ServiceAccountAuth, srvc::SchemaService},
};

pub mod convert;
pub mod encoding;
pub mod introspect;
pub mod mongo;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub id: String,
    pub name: Option<String>,
    pub encoding: Encoding,
    pub definition: String,
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "Utc::now")]
    pub updated_at: DateTime<Utc>,
}

impl SchemaEntry {
    pub fn to_schema(&self) -> anyhow::Result<Schema> {
        Schema::parse(&self.definition, self.encoding.clone())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SchemaEntrySummary {
    pub id: String,
    pub name: Option<String>,
    pub encoding: Encoding,
}

#[derive(thiserror::Error, Debug)]
pub enum SchemaRegistryError {
    #[error("schema not found: {0}")]
    NotFound(String),
    #[error("MongoDB error: {0}")]
    MongoDb(#[from] mongodb::error::Error),
    #[error("schema parse error: {0}")]
    Parse(String),
    #[error("{0}")]
    Other(String),
}

pub type DynSchemaRegistry = Arc<dyn SchemaRegistry + Send + Sync>;

pub enum SchemaProvider {
    PubSub(SchemaService<ServiceAccountAuth>),
    MongoDb(MongoDbSchemaProvider),
}

#[async_trait]
impl SchemaRegistry for SchemaProvider {
    async fn get(&self, id: &str) -> Result<SchemaEntry, SchemaRegistryError> {
        match self {
            SchemaProvider::PubSub(_sp) => Err(SchemaRegistryError::Other(
                "PubSub get not yet implemented".into(),
            )),
            SchemaProvider::MongoDb(sp) => sp.get(id).await,
        }
    }

    async fn list(&self) -> Result<Vec<SchemaEntrySummary>, SchemaRegistryError> {
        match self {
            SchemaProvider::PubSub(_sp) => Err(SchemaRegistryError::Other(
                "PubSub list not yet implemented".into(),
            )),
            SchemaProvider::MongoDb(sp) => sp.list().await,
        }
    }

    async fn save(&self, entry: &SchemaEntry) -> Result<(), SchemaRegistryError> {
        match self {
            SchemaProvider::PubSub(_sp) => Err(SchemaRegistryError::Other(
                "PubSub save not yet implemented".into(),
            )),
            SchemaProvider::MongoDb(sp) => sp.save(entry).await,
        }
    }

    async fn delete(&self, id: &str) -> Result<(), SchemaRegistryError> {
        match self {
            SchemaProvider::PubSub(_sp) => Err(SchemaRegistryError::Other(
                "PubSub delete not yet implemented".into(),
            )),
            SchemaProvider::MongoDb(sp) => sp.delete(id).await,
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
pub trait SchemaRegistry: Send + Sync {
    async fn get(&self, id: &str) -> Result<SchemaEntry, SchemaRegistryError>;
    async fn list(&self) -> Result<Vec<SchemaEntrySummary>, SchemaRegistryError>;
    async fn save(&self, entry: &SchemaEntry) -> Result<(), SchemaRegistryError>;
    async fn delete(&self, id: &str) -> Result<(), SchemaRegistryError>;
}

pub struct NoopSchemaRegistry;

#[async_trait]
impl SchemaRegistry for NoopSchemaRegistry {
    async fn get(&self, id: &str) -> Result<SchemaEntry, SchemaRegistryError> {
        Err(SchemaRegistryError::NotFound(id.to_string()))
    }

    async fn list(&self) -> Result<Vec<SchemaEntrySummary>, SchemaRegistryError> {
        Ok(vec![])
    }

    async fn save(&self, _entry: &SchemaEntry) -> Result<(), SchemaRegistryError> {
        Ok(())
    }

    async fn delete(&self, _id: &str) -> Result<(), SchemaRegistryError> {
        Ok(())
    }
}
