use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Ok;
use gauth::serv_account::ServiceAccount;
use gauth::token_provider::AsyncTokenProvider;

use crate::config::Config;
use crate::config::GcpAuthConfig;
use crate::config::Service;
use crate::config::ServiceConfigReference;
use crate::kafka::consumer::KafkaConsumer;
use crate::mongodb::db_client;
use crate::mongodb::MongoDbChangeStreamListener;
use crate::pubsub::SCOPES;
use crate::schema::mongo::MongoDbSchemaProvider;
use crate::sink::SinkProvider;
use crate::source::SourceProvider;
use crate::{
    kafka::producer::KafkaProducer,
    pubsub::{
        srvc::{PubSubPublisher, SchemaService},
        ServiceAccountAuth, StaticAccessToken,
    },
    schema::SchemaProvider,
};

pub(crate) struct ServiceFactory<'a> {
    config: &'a Config,
}

impl<'a> ServiceFactory<'a> {
    pub fn new(config: &'a Config) -> ServiceFactory<'a> {
        Self { config }
    }

    pub async fn schema_provider(
        &self,
        schema_config: &ServiceConfigReference,
    ) -> anyhow::Result<SchemaProvider> {
        let service_config = self
            .service_definition(&schema_config.service_name)
            .context("schema_provider")?;

        match service_config {
            Service::PubSub { auth, .. } => {
                let tp = self.create_gcp_token_provider(auth).await?;

                Ok(SchemaProvider::PubSub(
                    SchemaService::with_interceptor(tp).await?,
                ))
            }
            Service::MongoDb {
                name,
                connection_string,
                db_name,
            } => {
                let db = db_client(name.clone(), &connection_string)
                    .await?
                    .database(&db_name);

                Ok(SchemaProvider::MongoDb(MongoDbSchemaProvider::new(db)))
            }
            _ => bail!(
                "schema_provider: unsupported service: {}",
                service_config.name()
            ),
        }
    }

    pub async fn publisher_service(
        &self,
        topic_cfg: &ServiceConfigReference,
    ) -> anyhow::Result<SinkProvider> {
        let service_config = self
            .service_definition(&topic_cfg.service_name)
            .context("publisher_service")?;

        match service_config {
            Service::Kafka { config, .. } => Ok(SinkProvider::Kafka(KafkaProducer::new(&config)?)),
            Service::PubSub { auth, .. } => {
                let tp = self.create_gcp_token_provider(auth).await?;
                // todo: thing about preloading the token providers
                // in a different place
                Ok(SinkProvider::PubSub(
                    PubSubPublisher::with_interceptor(tp).await?,
                ))
            }
            _ => Err(anyhow!(
                "publisher_service: unsupported service: {:?}",
                service_config.name()
            )),
        }
    }

    pub async fn source_provider(
        &self,
        sink_cfg: &ServiceConfigReference,
    ) -> anyhow::Result<SourceProvider> {
        let service_config = self
            .service_definition(&sink_cfg.service_name)
            .context("sink_service")?;

        match service_config {
            Service::MongoDb {
                name,
                connection_string,
                db_name,
            } => {
                let db = db_client(name.clone(), &connection_string)
                    .await?
                    .database(&db_name);

                Ok(SourceProvider::MongoDb(MongoDbChangeStreamListener::new(
                    db,
                    db_name,
                    sink_cfg.id.clone(),
                )))
            }
            Service::Kafka { config, .. } => {
                let consumer = KafkaConsumer::new(&config, sink_cfg.id.clone())?;
                Ok(SourceProvider::Kafka(consumer))
            }
            _ => Err(anyhow!(
                "sink_service: unsupported service: {:?}",
                service_config.name()
            )),
        }
    }

    async fn create_gcp_token_provider(
        &self,
        auth: GcpAuthConfig,
    ) -> anyhow::Result<ServiceAccountAuth> {
        match auth {
            GcpAuthConfig::ServiceAccount { account_key_path } => {
                let service_account = ServiceAccount::from_file(&account_key_path, SCOPES.to_vec());
                let tp = AsyncTokenProvider::new(service_account).with_interval(600);
                tp.watch_updates().await;

                Ok(ServiceAccountAuth::new(tp))
            }
            GcpAuthConfig::StaticToken { env_token_name } => {
                let token = std::env::var(env_token_name)
                    .context("failed to get static token from env var")?;

                let tp = StaticAccessToken(token);
                Ok(ServiceAccountAuth::new(tp))
            }
        }
    }

    fn service_definition(&self, service_name: &str) -> anyhow::Result<Service> {
        self.config
            .service_by_name(service_name)
            .cloned()
            .ok_or_else(|| anyhow!("service config not found for: {}", service_name))
    }
}
