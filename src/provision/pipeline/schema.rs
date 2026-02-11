use std::sync::Arc;

use anyhow::{anyhow, bail};
use tokio::sync::RwLock;

use crate::{
    config::{SchemaServiceConfigReference, Service},
    provision::{pipeline::builder::ComponentBuilder, registry::ServiceRegistry},
    pubsub::srvc::SchemaService,
    schema::{Schema, SchemaProvider, SchemaRegistry, mongo::MongoDbSchemaProvider},
};

pub(super) struct SchemaBuilder {
    registry: Arc<RwLock<ServiceRegistry>>,
    configs: Vec<SchemaServiceConfigReference>,
}

pub struct SchemaDefinition {
    pub schema_id: String,
    pub schema: Schema,
}

#[async_trait::async_trait]
impl ComponentBuilder for SchemaBuilder {
    type Output = Vec<SchemaDefinition>;

    async fn build(&self, _schemas: &[SchemaDefinition]) -> anyhow::Result<Vec<SchemaDefinition>> {
        if self.configs.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(self.configs.len());
        for schema_cfg in self.configs.iter() {
            let schema_provider = self.schema(&schema_cfg).await?;

            // wait for the schema to be ready
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            let entry = schema_provider.get(&schema_cfg.resource).await?;
            let schema = entry.to_schema()?;

            result.push(SchemaDefinition {
                schema_id: schema_cfg.id.clone(),
                schema,
            });
        }
        Ok(result)
    }

    fn service_deps(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .configs
            .iter()
            .map(|cfg| cfg.service_name.clone())
            .collect();

        names.sort_unstable();
        names.dedup();
        names
    }
}

impl SchemaBuilder {
    pub fn new(
        registry: Arc<RwLock<ServiceRegistry>>,
        configs: &Option<Vec<SchemaServiceConfigReference>>,
    ) -> Self {
        let configs = match configs {
            Some(cfgs) => cfgs.clone(),
            None => Vec::new(),
        };

        Self { registry, configs }
    }

    async fn schema(&self, cfg: &SchemaServiceConfigReference) -> anyhow::Result<SchemaProvider> {
        let registry_read = self.registry.read().await;

        let service_config = registry_read
            .service_definition(&cfg.service_name)
            .await
            .map_err(|err| anyhow!("failed to initialize a schema: {}", err))?;

        match service_config {
            Service::PubSub(gcp_conf) => {
                let tp = registry_read.gcp_auth(&gcp_conf.name).await?;
                Ok(SchemaProvider::PubSub {
                    service: SchemaService::with_interceptor(tp.clone()).await?,
                    project_id: gcp_conf.project_id.clone(),
                })
            }
            Service::MongoDb(mongo_cfg) => {
                let mgo_client = registry_read.mongodb_client(&mongo_cfg.name).await?;
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
