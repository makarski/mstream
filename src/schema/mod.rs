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
    #[serde(default)]
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
    PubSub {
        service: SchemaService<ServiceAccountAuth>,
        project_id: String,
    },
    MongoDb(MongoDbSchemaProvider),
}

impl SchemaProvider {
    fn pubsub_parent(project_id: &str) -> String {
        format!("projects/{project_id}")
    }

    fn pubsub_schema_name(project_id: &str, schema_id: &str) -> String {
        format!("projects/{project_id}/schemas/{schema_id}")
    }

    fn encoding_to_pubsub_type(encoding: &Encoding) -> crate::pubsub::api::schema::Type {
        match encoding {
            Encoding::Avro => crate::pubsub::api::schema::Type::Avro,
            _ => crate::pubsub::api::schema::Type::Unspecified,
        }
    }

    fn pubsub_type_to_encoding(
        t: crate::pubsub::api::schema::Type,
    ) -> Result<Encoding, SchemaRegistryError> {
        match t {
            crate::pubsub::api::schema::Type::Avro => Ok(Encoding::Avro),
            other => Err(SchemaRegistryError::Other(format!(
                "unsupported pubsub schema type: {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl SchemaRegistry for SchemaProvider {
    async fn get(&self, id: &str) -> Result<SchemaEntry, SchemaRegistryError> {
        match self {
            SchemaProvider::PubSub {
                service,
                project_id,
            } => {
                let full_name = Self::pubsub_schema_name(project_id, id);
                let schema = service
                    .get_schema(&full_name)
                    .await
                    .map_err(|e| SchemaRegistryError::Other(e.to_string()))?;

                let (encoding, definition) = match &schema {
                    Schema::Avro(avro) => (Encoding::Avro, avro.canonical_form()),
                    Schema::Json(json) => (Encoding::Json, json.to_string()),
                    Schema::Undefined => {
                        return Err(SchemaRegistryError::Other(
                            "schema has undefined encoding".into(),
                        ));
                    }
                };

                Ok(SchemaEntry {
                    id: id.to_string(),
                    name: Some(id.to_string()),
                    encoding,
                    definition,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                })
            }
            SchemaProvider::MongoDb(sp) => sp.get(id).await,
        }
    }

    async fn list(&self) -> Result<Vec<SchemaEntrySummary>, SchemaRegistryError> {
        match self {
            SchemaProvider::PubSub {
                service,
                project_id,
            } => {
                let parent = Self::pubsub_parent(project_id);
                let response = service
                    .list_schemas(parent)
                    .await
                    .map_err(|e| SchemaRegistryError::Other(e.to_string()))?;

                let summaries = response
                    .schemas
                    .into_iter()
                    .filter_map(|s| {
                        let schema_type =
                            crate::pubsub::api::schema::Type::try_from(s.r#type).ok()?;
                        let encoding = Self::pubsub_type_to_encoding(schema_type).ok()?;
                        let schema_id = s.name.rsplit('/').next().unwrap_or(&s.name).to_string();
                        Some(SchemaEntrySummary {
                            id: schema_id,
                            name: Some(s.name),
                            encoding,
                        })
                    })
                    .collect();

                Ok(summaries)
            }
            SchemaProvider::MongoDb(sp) => sp.list().await,
        }
    }

    async fn save(&self, entry: &SchemaEntry) -> Result<(), SchemaRegistryError> {
        match self {
            SchemaProvider::PubSub {
                service,
                project_id,
            } => {
                let parent = Self::pubsub_parent(project_id);
                let schema_id = entry.name.as_deref().unwrap_or(&entry.id);
                let pubsub_type = Self::encoding_to_pubsub_type(&entry.encoding);

                service
                    .create_schema(
                        parent,
                        schema_id.to_string(),
                        pubsub_type,
                        entry.definition.clone(),
                    )
                    .await
                    .map_err(|e| SchemaRegistryError::Other(e.to_string()))?;

                Ok(())
            }
            SchemaProvider::MongoDb(sp) => sp.save(entry).await,
        }
    }

    async fn delete(&self, id: &str) -> Result<(), SchemaRegistryError> {
        match self {
            SchemaProvider::PubSub {
                service,
                project_id,
            } => {
                let full_name = Self::pubsub_schema_name(project_id, id);
                service
                    .delete_schema(full_name)
                    .await
                    .map_err(|e| SchemaRegistryError::Other(e.to_string()))?;

                Ok(())
            }
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
