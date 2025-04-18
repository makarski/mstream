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
use crate::http;
use crate::http::middleware::HttpMiddleware;
use crate::kafka::consumer::KafkaConsumer;
use crate::middleware::MiddlewareProvider;
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
    http_services: HashMap<String, http::HttpService>,
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

        let mut http_services = HashMap::new();
        if config.has_http() {
            for service in config.services.iter() {
                if let Service::Http {
                    name,
                    host,
                    max_retries,
                    base_backoff_ms,
                    connection_timeout_sec,
                    timeout_sec,
                    tcp_keepalive_sec,
                } = service
                {
                    let http_service = http::HttpService::new(
                        host.clone(),
                        *max_retries,
                        *base_backoff_ms,
                        *connection_timeout_sec,
                        *timeout_sec,
                        *tcp_keepalive_sec,
                    )
                    .with_context(|| anyhow!("failed to initialize http service for: {}", name))?;
                    http_services.insert(name.clone(), http_service);
                }
            }
        }

        Ok(ServiceFactory {
            config,
            mongo_clients,
            gcp_token_providers,
            http_services,
        })
    }

    pub async fn middleware_service(
        &self,
        midware_config: &ServiceConfigReference,
    ) -> anyhow::Result<MiddlewareProvider> {
        let service_config = self
            .service_definition(&midware_config.service_name)
            .context("middleware config")?;

        match service_config {
            Service::Http { name, .. } => match self.http_services.get(&name) {
                Some(http_service) => Ok(MiddlewareProvider::Http(HttpMiddleware::new(
                    midware_config.id.clone(),
                    midware_config.encoding.clone(),
                    http_service.clone(),
                ))),
                None => bail!(
                    "initializing middleware: http service not found for: {}",
                    name
                ),
            },
            _ => bail!(
                "initializing middleware: unsupported service: {}",
                service_config.name()
            ),
        }
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
            Service::Http {
                name,
                host,
                max_retries,
                base_backoff_ms,
                connection_timeout_sec,
                timeout_sec,
                tcp_keepalive_sec,
            } => {
                let http_service = http::HttpService::new(
                    host.clone(),
                    max_retries,
                    base_backoff_ms,
                    connection_timeout_sec,
                    timeout_sec,
                    tcp_keepalive_sec,
                )
                .with_context(|| anyhow!("failed to initialize http service for: {}", name))?;

                Ok(SinkProvider::Http(http_service))
            }
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
            Service::Kafka {
                config,
                offset_seek_back_seconds,
                ..
            } => {
                let consumer = KafkaConsumer::new(
                    &config,
                    sink_cfg.id.clone(),
                    sink_cfg.encoding.clone(),
                    offset_seek_back_seconds,
                )?;

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
            _ => bail!(
                "source_provider: unsupported service: {}",
                service_config.name()
            ),
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
