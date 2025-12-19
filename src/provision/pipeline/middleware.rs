use std::sync::Arc;

use anyhow::{bail, Context};

use crate::{
    config::{Service, ServiceConfigReference},
    http::middleware::HttpMiddleware,
    middleware::{udf::UdfMiddleware, MiddlewareProvider},
    provision::{pipeline::SchemaDefinition, registry::ServiceRegistry},
    schema::Schema,
};

pub(super) struct MiddlewareBuilder {
    registry: Arc<ServiceRegistry>,
    configs: Vec<ServiceConfigReference>,
}

pub struct MiddlewareDefinition {
    pub config: ServiceConfigReference,
    pub provider: MiddlewareProvider,
    pub schema: Schema,
}

impl MiddlewareBuilder {
    pub fn new(
        registry: Arc<ServiceRegistry>,
        configs: &Option<Vec<ServiceConfigReference>>,
    ) -> Self {
        let configs = configs.clone().unwrap_or_else(|| Vec::new());
        Self { registry, configs }
    }

    pub async fn build(
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

    async fn middleware(&self, cfg: &ServiceConfigReference) -> anyhow::Result<MiddlewareProvider> {
        let service_config = self
            .registry
            .service_definition(&cfg.service_name)
            .await
            .context("middleware config")?;

        match service_config {
            Service::Http(http_cfg) => {
                let http_service = self.registry.http_client(&http_cfg.name).await?;
                Ok(MiddlewareProvider::Http(HttpMiddleware::new(
                    cfg.resource.clone(),
                    cfg.output_encoding.clone(),
                    http_service,
                )))
            }
            Service::Udf(udf_cfg) => {
                let rhai_builder = self.registry.udf_middleware(&udf_cfg.name).await?;
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
