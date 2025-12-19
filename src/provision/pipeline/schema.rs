use std::sync::Arc;

use anyhow::{bail, Context};

use crate::{
    config::{SchemaServiceConfigReference, Service},
    provision::registry::ServiceRegistry,
    pubsub::srvc::SchemaService,
    schema::{mongo::MongoDbSchemaProvider, Schema, SchemaProvider, SchemaRegistry},
};

pub(super) struct SchemaBuilder {
    registry: Arc<ServiceRegistry>,
    configs: Vec<SchemaServiceConfigReference>,
}

pub struct SchemaDefinition {
    pub schema_id: String,
    pub schema: Schema,
}

impl SchemaBuilder {
    pub fn new(
        registry: Arc<ServiceRegistry>,
        configs: &Option<Vec<SchemaServiceConfigReference>>,
    ) -> Self {
        let configs = match configs {
            Some(cfgs) => cfgs.clone(),
            None => Vec::new(),
        };

        Self { registry, configs }
    }

    pub async fn build(&self) -> anyhow::Result<Vec<SchemaDefinition>> {
        if self.configs.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(self.configs.len());
        for schema_cfg in self.configs.iter() {
            let mut schema_service = self.schema(&schema_cfg).await?;

            // wait for the schema to be ready
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            let schema = schema_service
                .get_schema(schema_cfg.resource.clone())
                .await?;

            result.push(SchemaDefinition {
                schema_id: schema_cfg.id.clone(),
                schema,
            });
        }
        Ok(result)
    }

    async fn schema(&self, cfg: &SchemaServiceConfigReference) -> anyhow::Result<SchemaProvider> {
        let service_config = self
            .registry
            .service_definition(&cfg.service_name)
            .await
            .context("schema_provider")?;

        match service_config {
            Service::PubSub(gcp_conf) => {
                let tp = self.registry.gcp_auth(&gcp_conf.name).await?;
                Ok(SchemaProvider::PubSub(
                    SchemaService::with_interceptor(tp.clone()).await?,
                ))
            }
            Service::MongoDb(mongo_cfg) => {
                let mgo_client = self.registry.mongodb_client(&mongo_cfg.name).await?;
                let schema_collection = cfg.resource.clone();

                Ok(SchemaProvider::MongoDb(MongoDbSchemaProvider::new(
                    mgo_client.database(&mongo_cfg.db_name),
                    schema_collection,
                )))
            }
            _ => bail!(
                "failed to initialize schema: unsupported service: {}",
                service_config.name()
            ),
        }
    }
}
