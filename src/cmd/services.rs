use std::collections::HashMap;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Ok;
use gauth::serv_account::ServiceAccount;
use gauth::token_provider::AsyncTokenProvider;
use mongodb::Client;

use crate::config::Config;
use crate::config::GcpAuthConfig;
use crate::config::Service;
use crate::config::ServiceConfigReference;
use crate::kafka::consumer::KafkaConsumer;
use crate::mongodb::db_client;
use crate::mongodb::persister::MongoDbPersister;
use crate::mongodb::MongoDbChangeStreamListener;
use crate::pubsub::srvc::PubSubSubscriber;
use crate::pubsub::SCOPES;
use crate::schema::mongo::MongoDbSchemaProvider;
use crate::schema::mongo::SCHEMA_REGISTRY_COLLECTION;
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
    mongo_clients: HashMap<String, Client>,
    gcp_token_providers: HashMap<String, ServiceAccountAuth>,
}

impl<'a> ServiceFactory<'a> {
    pub async fn new(config: &'a Config) -> anyhow::Result<ServiceFactory<'a>> {
        let mut mongo_clients = HashMap::new();
        if config.has_mongo_db() {
            for service in config.services.iter() {
                if let Service::MongoDb {
                    name,
                    connection_string,
                    ..
                } = service
                {
                    let client = db_client(name.to_owned(), connection_string).await?;
                    mongo_clients.insert(name.clone(), client);
                }
            }
        }

        let mut gcp_token_providers = HashMap::new();
        if config.has_pubsub() {
            for service in config.services.iter() {
                if let Service::PubSub { name, auth } = service {
                    let tp = Self::create_gcp_token_provider(auth).await?;
                    gcp_token_providers.insert(name.clone(), tp);
                }
            }
        }

        Ok(ServiceFactory {
            config,
            mongo_clients,
            gcp_token_providers,
        })
    }

    pub async fn schema_provider(
        &self,
        schema_config: &ServiceConfigReference,
    ) -> anyhow::Result<SchemaProvider> {
        let service_config = self
            .service_definition(&schema_config.service_name)
            .context("schema_provider")?;

        match service_config {
            Service::PubSub { name, .. } => match self.gcp_token_providers.get(&name) {
                Some(tp) => Ok(SchemaProvider::PubSub(
                    SchemaService::with_interceptor(tp.clone()).await?,
                )),
                None => bail!(
                    "initializing schema provider: gcp token provider not found for: {}",
                    name
                ),
            },
            Service::MongoDb {
                name,
                db_name,
                schema_collection,
                ..
            } => match self.mongo_clients.get(&name) {
                Some(client) => {
                    let schema_collection = match schema_collection {
                        Some(collection) => collection,
                        None => SCHEMA_REGISTRY_COLLECTION.to_owned(),
                    };

                    Ok(SchemaProvider::MongoDb(MongoDbSchemaProvider::new(
                        client.database(&db_name),
                        schema_collection,
                    )))
                }
                None => bail!(
                    "initializing schema provider: mongo client not found for: {}",
                    name
                ),
            },
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
            Service::PubSub { name, .. } => match self.gcp_token_providers.get(&name) {
                Some(tp) => Ok(SinkProvider::PubSub(
                    PubSubPublisher::with_interceptor(tp.clone()).await?,
                )),
                None => bail!(
                    "initializing publisher service: gcp token provider not found for: {}",
                    name
                ),
            },
            Service::MongoDb { name, db_name, .. } => match self.mongo_clients.get(&name) {
                Some(db) => {
                    let db = db.database(&db_name);
                    Ok(SinkProvider::MongoDb(MongoDbPersister::new(db)))
                }
                None => bail!(
                    "initializing publisher service: mongo client not found for: {}",
                    name
                ),
            },
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
            Service::MongoDb { name, db_name, .. } => match self.mongo_clients.get(&name) {
                Some(db) => {
                    let db = db.database(&db_name);
                    Ok(SourceProvider::MongoDb(MongoDbChangeStreamListener::new(
                        db,
                        db_name,
                        sink_cfg.id.clone(),
                    )))
                }
                None => bail!(
                    "initializing source provider: mongo client not found for: {}",
                    name
                ),
            },
            Service::Kafka { config, .. } => {
                let consumer =
                    KafkaConsumer::new(&config, sink_cfg.id.clone(), sink_cfg.encoding.clone())?;
                Ok(SourceProvider::Kafka(consumer))
            }
            Service::PubSub { name, .. } => match self.gcp_token_providers.get(&name) {
                Some(tp) => {
                    let subscriber = PubSubSubscriber::new(
                        tp.clone(),
                        sink_cfg.id.clone(),
                        sink_cfg.encoding.clone(),
                    )
                    .await?;
                    Ok(SourceProvider::PubSub(subscriber))
                }
                None => bail!(
                    "initializing source provider: gcp token provider not found for: {}",
                    name
                ),
            },
        }
    }

    async fn create_gcp_token_provider(auth: &GcpAuthConfig) -> anyhow::Result<ServiceAccountAuth> {
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
