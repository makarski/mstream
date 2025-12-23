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
            let publisher = self.publisher(&sink_cfg).await?;
            let schema = super::find_schema(sink_cfg.schema_id.clone(), schemas);

            let sink_definition = SinkDefinition {
                config: sink_cfg.clone(),
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

    async fn publisher(&self, cfg: &ServiceConfigReference) -> anyhow::Result<SinkProvider> {
        let service_config = self
            .registry
            .read()
            .await
            .service_definition(&cfg.service_name)
            .await
            .map_err(|err| anyhow!("failed to initialize a sink: {}", err))?;

        match service_config {
            Service::Kafka(k_conf) => Ok(SinkProvider::Kafka(KafkaProducer::new(&k_conf.config)?)),
            Service::PubSub(ps_conf) => {
                let tp = self.registry.read().await.gcp_auth(&ps_conf.name).await?;
                Ok(SinkProvider::PubSub(
                    PubSubPublisher::with_interceptor(tp.clone()).await?,
                ))
            }
            Service::MongoDb(mongo_cfg) => {
                let mgo_client = self
                    .registry
                    .read()
                    .await
                    .mongodb_client(&mongo_cfg.name)
                    .await?;
                let db = mgo_client.database(&mongo_cfg.db_name);
                Ok(SinkProvider::MongoDb(MongoDbPersister::new(db)))
            }
            Service::Http(http_config) => {
                let http_service = self
                    .registry
                    .read()
                    .await
                    .http_client(&http_config.name)
                    .await?;
                Ok(SinkProvider::Http(http_service.clone()))
            }
            _ => bail!(
                "publisher_service: unsupported service: {}",
                service_config.name()
            ),
        }
    }
}
