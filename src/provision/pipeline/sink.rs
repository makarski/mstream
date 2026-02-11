use std::sync::Arc;

use anyhow::{anyhow, bail};
use tokio::sync::RwLock;

use crate::{
    config::{Service, ServiceConfigReference},
    kafka::producer::KafkaProducer,
    mongodb::persister::MongoDbPersister,
    provision::{
        pipeline::{SchemaDefinition, builder::ComponentBuilder},
        registry::ServiceRegistry,
    },
    pubsub::srvc::PubSubPublisher,
    schema::Schema,
    sink::SinkProvider,
};

pub(crate) struct SinkBuilder {
    registry: Arc<RwLock<ServiceRegistry>>,
    configs: Vec<ServiceConfigReference>,
}

pub struct SinkDefinition {
    pub config: ServiceConfigReference,
    pub sink: SinkProvider,
    pub schema: Schema,
}

#[async_trait::async_trait]
impl ComponentBuilder for SinkBuilder {
    type Output = Vec<SinkDefinition>;

    async fn build(&self, schemas: &[SchemaDefinition]) -> anyhow::Result<Vec<SinkDefinition>> {
        let mut sinks = Vec::with_capacity(self.configs.len());
        for sink_cfg in self.configs.iter() {
            let (publisher, resolved_resource) = self.publisher(&sink_cfg).await?;
            let schema = super::find_schema(sink_cfg.schema_id.clone(), schemas);

            let mut config = sink_cfg.clone();
            config.resource = resolved_resource;

            let sink_definition = SinkDefinition {
                config,
                sink: publisher,
                schema,
            };

            sinks.push(sink_definition);
        }

        Ok(sinks)
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

impl SinkBuilder {
    pub fn new(
        registry: Arc<RwLock<ServiceRegistry>>,
        configs: Vec<ServiceConfigReference>,
    ) -> Self {
        Self { registry, configs }
    }

    async fn publisher(
        &self,
        cfg: &ServiceConfigReference,
    ) -> anyhow::Result<(SinkProvider, String)> {
        let registry_read = self.registry.read().await;

        let service_config = registry_read
            .service_definition(&cfg.service_name)
            .await
            .map_err(|err| anyhow!("failed to initialize a sink: {}", err))?;

        match service_config {
            Service::Kafka(k_conf) => Ok((
                SinkProvider::Kafka(KafkaProducer::new(&k_conf.config)?),
                cfg.resource.clone(),
            )),
            Service::PubSub(ps_conf) => {
                let tp = registry_read.gcp_auth(&ps_conf.name).await?;
                let resolved = format!("projects/{}/topics/{}", ps_conf.project_id, cfg.resource);
                Ok((
                    SinkProvider::PubSub(PubSubPublisher::with_interceptor(tp.clone()).await?),
                    resolved,
                ))
            }
            Service::MongoDb(mongo_cfg) => {
                let mgo_client = registry_read.mongodb_client(&mongo_cfg.name).await?;
                let db = mgo_client.database(&mongo_cfg.db_name);
                Ok((
                    SinkProvider::MongoDb(MongoDbPersister::new(db, mongo_cfg.write_mode.clone())),
                    cfg.resource.clone(),
                ))
            }
            Service::Http(http_config) => {
                let http_service = registry_read.http_client(&http_config.name).await?;
                Ok((
                    SinkProvider::Http(http_service.clone()),
                    cfg.resource.clone(),
                ))
            }
            _ => bail!(
                "publisher_service: unsupported service: {}",
                service_config.name()
            ),
        }
    }
}
