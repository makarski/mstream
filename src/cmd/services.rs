use std::collections::HashMap;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Ok;
use gauth::serv_account::ServiceAccount;
use gauth::token_provider::AsyncTokenProvider;
use mongodb::Client;

use crate::config::Encoding;
use crate::config::SchemaServiceConfigReference;
use crate::config::SourceServiceConfigReference;
use crate::config::UdfEngine;
use crate::config::{Config, GcpAuthConfig, Service, ServiceConfigReference};
use crate::http::{self, middleware::HttpMiddleware};
use crate::kafka::{consumer::KafkaConsumer, producer::KafkaProducer};
use crate::middleware::udf::rhai::RhaiMiddleware;
use crate::middleware::udf::UdfMiddleware;
use crate::middleware::MiddlewareProvider;
use crate::mongodb::{db_client, persister::MongoDbPersister, MongoDbChangeStreamListener};
use crate::pubsub::{
    srvc::{PubSubPublisher, PubSubSubscriber, SchemaService},
    ServiceAccountAuth, StaticAccessToken, SCOPES,
};
use crate::schema::{mongo::MongoDbSchemaProvider, SchemaProvider};
use crate::sink::SinkProvider;
use crate::source::SourceProvider;

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
                    midware_config.resource.clone(),
                    midware_config.output_encoding.clone(),
                    http_service.clone(),
                ))),
                None => bail!(
                    "initializing middleware: http service not found for: {}",
                    name
                ),
            },
            Service::Udf {
                name,
                script_path,
                engine,
            } => match engine {
                UdfEngine::Rhai => Ok(MiddlewareProvider::Udf(UdfMiddleware::Rhai(
                    RhaiMiddleware::new(script_path.clone(), midware_config.resource.clone())
                        .with_context(|| {
                            format!(
                                "failed initializing RhaiScript middleware provider: {}:{}",
                                name, script_path
                            )
                        })?,
                ))),
            },

            _ => bail!(
                "initializing middleware: unsupported service: {}",
                service_config.name()
            ),
        }
    }

    pub async fn schema_provider(
        &self,
        schema_config: &SchemaServiceConfigReference,
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
            Service::MongoDb { name, db_name, .. } => match self.mongo_clients.get(&name) {
                Some(client) => {
                    let schema_collection = schema_config.resource.clone();

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
            _ => bail!(
                "publisher_service: unsupported service: {}",
                service_config.name()
            ),
        }
    }

    fn unwrap_source_input_encoding(
        service_config: &Service,
        source_cfg: &SourceServiceConfigReference,
    ) -> anyhow::Result<Encoding> {
        match service_config {
            Service::Kafka { .. } | Service::PubSub { .. } => {
                match source_cfg.input_encoding.as_ref() {
                    Some(encoding) => Ok(encoding.clone()),
                    None => {
                        bail!(
                            "initializing source provider: input encoding not found for: {}:{}",
                            source_cfg.service_name,
                            source_cfg.resource
                        )
                    }
                }
            }
            Service::MongoDb { .. } => {
                // MongoDB source provider does not require input encoding
                return Ok(Encoding::default());
            }
            Service::Http { .. } => {
                bail!(
                    "initializing source provider: unsupported service: {}",
                    service_config.name()
                )
            }
            _ => {
                bail!(
                    "initializing source provider: unsupported service: {}",
                    service_config.name()
                )
            }
        }
    }

    pub async fn source_provider(
        &self,
        source_cfg: &SourceServiceConfigReference,
    ) -> anyhow::Result<SourceProvider> {
        let service_config = self
            .service_definition(&source_cfg.service_name)
            .context("sink_service")?;

        let input_encoding = Self::unwrap_source_input_encoding(&service_config, source_cfg)?;

        match service_config {
            Service::MongoDb { name, db_name, .. } => match self.mongo_clients.get(&name) {
                Some(db) => {
                    let db = db.database(&db_name);
                    Ok(SourceProvider::MongoDb(MongoDbChangeStreamListener::new(
                        db,
                        db_name,
                        source_cfg.resource.clone(),
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
                    source_cfg.resource.clone(),
                    input_encoding,
                    offset_seek_back_seconds,
                )?;

                Ok(SourceProvider::Kafka(consumer))
            }
            Service::PubSub { name, .. } => match self.gcp_token_providers.get(&name) {
                Some(tp) => {
                    let subscriber = PubSubSubscriber::new(
                        tp.clone(),
                        source_cfg.resource.clone(),
                        input_encoding,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Encoding, GcpAuthConfig, Service, SourceServiceConfigReference};
    use std::collections::HashMap;

    #[test]
    fn test_unwrap_source_input_encoding() {
        // Create test service configs
        let mut kafka_config = HashMap::new();
        kafka_config.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        kafka_config.insert("group.id".to_string(), "test-group".to_string());
        kafka_config.insert("client.id".to_string(), "test-client".to_string());

        let kafka_service = Service::Kafka {
            name: "kafka-service".to_string(),
            offset_seek_back_seconds: Some(60),
            config: kafka_config,
        };

        let pubsub_service = Service::PubSub {
            name: "pubsub-service".to_string(),
            auth: GcpAuthConfig::StaticToken {
                env_token_name: "TEST_TOKEN".to_string(),
            },
        };

        let mongodb_service = Service::MongoDb {
            name: "mongodb-service".to_string(),
            connection_string: "mongodb://localhost:27017".to_string(),
            db_name: "test_db".to_string(),
        };

        let http_service = Service::Http {
            name: "http-service".to_string(),
            host: "http://localhost:8080".to_string(),
            max_retries: Some(3),
            base_backoff_ms: Some(100),
            connection_timeout_sec: Some(30),
            timeout_sec: Some(60),
            tcp_keepalive_sec: Some(60),
        };

        // Source config references for testing
        let source_with_encoding = SourceServiceConfigReference {
            service_name: "test-service".to_string(),
            resource: "test-resource".to_string(),
            schema_id: None,
            input_encoding: Some(Encoding::Json),
            output_encoding: Encoding::Avro,
        };

        let source_without_encoding = SourceServiceConfigReference {
            service_name: "test-service".to_string(),
            resource: "test-resource".to_string(),
            schema_id: None,
            input_encoding: None,
            output_encoding: Encoding::Avro,
        };

        // Case 1: Kafka/PubSub with input_encoding specified
        let result =
            ServiceFactory::unwrap_source_input_encoding(&kafka_service, &source_with_encoding);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Encoding::Json);

        let result =
            ServiceFactory::unwrap_source_input_encoding(&pubsub_service, &source_with_encoding);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Encoding::Json);

        // Case 2: Kafka/PubSub without input_encoding (should error)
        let result =
            ServiceFactory::unwrap_source_input_encoding(&kafka_service, &source_without_encoding);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("input encoding not found"));

        let result =
            ServiceFactory::unwrap_source_input_encoding(&pubsub_service, &source_without_encoding);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("input encoding not found"));

        // Case 3: MongoDB service (should return default encoding regardless of input_encoding)
        let result =
            ServiceFactory::unwrap_source_input_encoding(&mongodb_service, &source_with_encoding);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Encoding::default());

        let result = ServiceFactory::unwrap_source_input_encoding(
            &mongodb_service,
            &source_without_encoding,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Encoding::default());

        // Case 4: HTTP service (should return error as it's not supported as a source)
        let result =
            ServiceFactory::unwrap_source_input_encoding(&http_service, &source_with_encoding);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unsupported service"));

        let result =
            ServiceFactory::unwrap_source_input_encoding(&http_service, &source_without_encoding);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unsupported service"));
    }
}
