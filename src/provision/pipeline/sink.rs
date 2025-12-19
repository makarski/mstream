use std::sync::Arc;

use anyhow::{bail, Context, Ok};

use crate::{
    config::{Service, ServiceConfigReference},
    kafka::producer::KafkaProducer,
    mongodb::persister::MongoDbPersister,
    provision::{pipeline::SchemaDefinition, registry::ServiceRegistry},
    pubsub::srvc::PubSubPublisher,
    schema::Schema,
    sink::SinkProvider,
};

pub(crate) struct SinkBuilder {
    registry: Arc<ServiceRegistry>,
    configs: Vec<ServiceConfigReference>,
}

pub struct SinkDefinition {
    pub config: ServiceConfigReference,
    pub sink: SinkProvider,
    pub schema: Schema,
}

impl SinkBuilder {
    pub fn new(registry: Arc<ServiceRegistry>, configs: Vec<ServiceConfigReference>) -> Self {
        Self { registry, configs }
    }

    pub async fn build(&self, schemas: &[SchemaDefinition]) -> anyhow::Result<Vec<SinkDefinition>> {
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

    pub async fn publisher(&self, cfg: &ServiceConfigReference) -> anyhow::Result<SinkProvider> {
        let service_config = self
            .registry
            .service_definition(&cfg.service_name)
            .await
            .context("publisher_service")?;

        match service_config {
            Service::Kafka(k_conf) => Ok(SinkProvider::Kafka(KafkaProducer::new(&k_conf.config)?)),
            Service::PubSub(ps_conf) => {
                let tp = self.registry.gcp_auth(&ps_conf.name).await?;
                Ok(SinkProvider::PubSub(
                    PubSubPublisher::with_interceptor(tp.clone()).await?,
                ))
            }
            Service::MongoDb(mongo_cfg) => {
                let mgo_client = self.registry.mongodb_client(&mongo_cfg.name).await?;
                let db = mgo_client.database(&mongo_cfg.db_name);
                Ok(SinkProvider::MongoDb(MongoDbPersister::new(db)))
            }
            Service::Http(http_config) => {
                let http_service = self.registry.http_client(&http_config.name).await?;
                Ok(SinkProvider::Http(http_service.clone()))
            }
            _ => bail!(
                "publisher_service: unsupported service: {}",
                service_config.name()
            ),
        }
    }
}
