use std::sync::Arc;

use anyhow::{anyhow, bail};
use tokio::sync::RwLock;

use crate::{
    config::{Service, ServiceConfigReference},
    http::middleware::HttpMiddleware,
    middleware::{MiddlewareProvider, udf::UdfMiddleware},
    provision::{
        pipeline::{SchemaDefinition, builder::ComponentBuilder},
        registry::ServiceRegistry,
    },
    schema::Schema,
};

pub(super) struct MiddlewareBuilder {
    registry: Arc<RwLock<ServiceRegistry>>,
    configs: Vec<ServiceConfigReference>,
}

pub struct MiddlewareDefinition {
    pub config: ServiceConfigReference,
    pub provider: MiddlewareProvider,
    pub schema: Schema,
}

#[async_trait::async_trait]
impl ComponentBuilder for MiddlewareBuilder {
    type Output = Vec<MiddlewareDefinition>;

    async fn build(
        &self,
        schemas: &[SchemaDefinition],
    ) -> anyhow::Result<Vec<MiddlewareDefinition>> {
        if self.configs.is_empty() {
            return Ok(Vec::new());
        }
        let mut middlewares = Vec::with_capacity(self.configs.len());

        for cfg in &self.configs {
            let middleware = self.middleware(cfg).await?;
            let schema = super::find_schema(cfg.schema_id.clone(), schemas);

            let middleware_def = MiddlewareDefinition {
                config: cfg.clone(),
                provider: middleware,
                schema,
            };

            middlewares.push(middleware_def);
        }

        Ok(middlewares)
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

impl MiddlewareBuilder {
    pub fn new(
        registry: Arc<RwLock<ServiceRegistry>>,
        configs: &Option<Vec<ServiceConfigReference>>,
    ) -> Self {
        let configs = configs.clone().unwrap_or_else(|| Vec::new());
        Self { registry, configs }
    }

    async fn middleware(&self, cfg: &ServiceConfigReference) -> anyhow::Result<MiddlewareProvider> {
        let registry_read = self.registry.read().await;

        let service_config = registry_read
            .service_definition(&cfg.service_name)
            .await
            .map_err(|err| anyhow!("failed to initialize a middleware: {}", err))?;

        match service_config {
            Service::Http(http_cfg) => {
                let http_service = registry_read.http_client(&http_cfg.name).await?;
                Ok(MiddlewareProvider::Http(HttpMiddleware::new(
                    cfg.resource.clone(),
                    cfg.output_encoding.clone(),
                    http_service,
                )))
            }
            Service::Udf(udf_cfg) => {
                let rhai_builder = registry_read.udf_middleware(&udf_cfg.name).await?;
                let rhai_middleware = rhai_builder(cfg.resource.clone())?;
                Ok(MiddlewareProvider::Udf(UdfMiddleware::Rhai(
                    rhai_middleware,
                )))
            }
            _ => bail!(
                "initializing middleware: unsupported service: {}",
                service_config.name()
            ),
        }
    }
}
