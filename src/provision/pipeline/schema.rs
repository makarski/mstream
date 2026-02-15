use std::sync::Arc;

use anyhow::anyhow;
use tokio::sync::RwLock;

use crate::{
    config::SchemaServiceConfigReference,
    provision::{pipeline::builder::ComponentBuilder, registry::ServiceRegistry},
    schema::{DynSchemaRegistry, Schema},
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
            let schema_registry = self.resolve_registry(&schema_cfg.service_name).await?;

            let entry = schema_registry.get(&schema_cfg.resource).await?;
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

    async fn resolve_registry(&self, service_name: &str) -> anyhow::Result<DynSchemaRegistry> {
        let registry_read = self.registry.read().await;
        registry_read
            .schema_registry_for(service_name)
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to initialize schema for service '{}': {}",
                    service_name,
                    err
                )
            })
    }
}
