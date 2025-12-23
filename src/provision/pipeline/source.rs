use std::sync::Arc;

use anyhow::{anyhow, bail};
use tokio::sync::RwLock;

use crate::{
    config::{Encoding, Service, SourceServiceConfigReference},
    kafka::consumer::KafkaConsumer,
    mongodb::MongoDbChangeStreamListener,
    provision::{
        pipeline::{builder::ComponentBuilder, SchemaDefinition},
        registry::ServiceRegistry,
    },
    pubsub::srvc::PubSubSubscriber,
    schema::Schema,
    source::SourceProvider,
};

pub(super) struct SourceBuilder {
    registry: Arc<RwLock<ServiceRegistry>>,
    config: SourceServiceConfigReference,
}

pub(super) struct SourceDefinition {
    pub source_provider: SourceProvider,
    pub schema: Schema,
}

#[async_trait::async_trait]
impl ComponentBuilder for SourceBuilder {
    type Output = SourceDefinition;

    async fn build(&self, schemas: &[SchemaDefinition]) -> anyhow::Result<SourceDefinition> {
        let service_config = self
            .registry
            .read()
            .await
            .service_definition(&self.config.service_name)
            .await
            .map_err(|err| anyhow!("failed to initialize a source: {}", err))?;

        let schema = super::find_schema(self.config.schema_id.clone(), schemas);
        let source_provider = self.source(service_config).await?;

        Ok(SourceDefinition {
            source_provider,
            schema,
        })
    }

    fn service_deps(&self) -> Vec<String> {
        vec![self.config.service_name.clone()]
    }
}

impl SourceBuilder {
    pub fn new(
        registry: Arc<RwLock<ServiceRegistry>>,
        config: SourceServiceConfigReference,
    ) -> Self {
        SourceBuilder { registry, config }
    }

    async fn source(&self, service_config: Service) -> anyhow::Result<SourceProvider> {
        let input_encoding = Self::resolve_source_encoding(&service_config, &self.config)?;

        match service_config {
            Service::MongoDb(mongo_conf) => {
                let mongo_client = self
                    .registry
                    .read()
                    .await
                    .mongodb_client(&mongo_conf.name)
                    .await?;
                let db = mongo_client.database(&mongo_conf.db_name);
                Ok(SourceProvider::MongoDb(MongoDbChangeStreamListener::new(
                    db,
                    mongo_conf.db_name,
                    self.config.resource.clone(),
                )))
            }
            Service::Kafka(k_conf) => {
                let consumer = KafkaConsumer::new(
                    &k_conf.config,
                    self.config.resource.clone(),
                    input_encoding,
                    k_conf.offset_seek_back_seconds,
                )?;

                Ok(SourceProvider::Kafka(consumer))
            }
            Service::PubSub(ps_conf) => {
                let tp = self.registry.read().await.gcp_auth(&ps_conf.name).await?;
                let subscriber =
                    PubSubSubscriber::new(tp.clone(), self.config.resource.clone(), input_encoding)
                        .await?;
                Ok(SourceProvider::PubSub(subscriber))
            }
            _ => bail!(
                "source_provider: unsupported service: {}",
                service_config.name()
            ),
        }
    }

    fn resolve_source_encoding(
        service_config: &Service,
        cfg: &SourceServiceConfigReference,
    ) -> anyhow::Result<Encoding> {
        match service_config {
            Service::Kafka(_) | Service::PubSub(_) => match cfg.input_encoding.as_ref() {
                Some(encoding) => Ok(encoding.clone()),
                None => {
                    bail!(
                        "initializing source provider: input encoding not found for: {}:{}",
                        cfg.service_name,
                        cfg.resource
                    )
                }
            },
            Service::MongoDb(_) => {
                return Ok(Encoding::Bson);
            }
            Service::Http(_) | Service::Udf(_) => {
                bail!(
                    "initializing source provider: unsupported service: {}",
                    service_config.name()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        service_config::{GcpAuthConfig, HttpConfig, KafkaConfig, MongoDbConfig, PubSubConfig},
        Encoding, Service, SourceServiceConfigReference,
    };
    use std::collections::HashMap;

    #[test]
    fn test_resolve_source_encoding() {
        // Create test service configs
        let mut kafka_config = HashMap::new();
        kafka_config.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        kafka_config.insert("group.id".to_string(), "test-group".to_string());
        kafka_config.insert("client.id".to_string(), "test-client".to_string());

        let kafka_service = Service::Kafka(KafkaConfig {
            name: "kafka-service".to_string(),
            offset_seek_back_seconds: Some(60),
            config: kafka_config,
        });

        let pubsub_service = Service::PubSub(PubSubConfig {
            name: "pubsub-service".to_string(),
            auth: GcpAuthConfig::StaticToken {
                env_token_name: "TEST_TOKEN".to_string(),
            },
        });

        let mongodb_service = Service::MongoDb(MongoDbConfig {
            name: "mongodb-service".to_string(),
            connection_string: "mongodb://localhost:27017".to_string(),
            db_name: "test_db".to_string(),
        });

        let http_service = Service::Http(HttpConfig {
            name: "http-service".to_string(),
            host: "http://localhost:8080".to_string(),
            max_retries: Some(3),
            base_backoff_ms: Some(100),
            connection_timeout_sec: Some(30),
            timeout_sec: Some(60),
            tcp_keepalive_sec: Some(60),
        });

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
        let result = SourceBuilder::resolve_source_encoding(&kafka_service, &source_with_encoding);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Encoding::Json);

        let result = SourceBuilder::resolve_source_encoding(&pubsub_service, &source_with_encoding);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Encoding::Json);

        // Case 2: Kafka/PubSub without input_encoding (should error)
        let result =
            SourceBuilder::resolve_source_encoding(&kafka_service, &source_without_encoding);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("input encoding not found"));

        let result =
            SourceBuilder::resolve_source_encoding(&pubsub_service, &source_without_encoding);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("input encoding not found"));

        // Case 3: MongoDB service (should return default encoding regardless of input_encoding)
        let result =
            SourceBuilder::resolve_source_encoding(&mongodb_service, &source_with_encoding);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Encoding::Bson);

        let result =
            SourceBuilder::resolve_source_encoding(&mongodb_service, &source_without_encoding);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Encoding::Bson);

        // Case 4: HTTP service (should return error as it's not supported as a source)
        let result = SourceBuilder::resolve_source_encoding(&http_service, &source_with_encoding);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unsupported service"));

        let result =
            SourceBuilder::resolve_source_encoding(&http_service, &source_without_encoding);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unsupported service"));
    }
}
