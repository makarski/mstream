use serde::{Deserialize, Serialize};

use crate::config::{
    service_config::{HttpConfig, KafkaConfig, MongoDbConfig, PubSubConfig, UdfConfig},
    system::SystemConfig,
};

pub mod service_config;
pub mod system;

pub trait Masked {
    fn masked(&self) -> Self;
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Config {
    pub system: Option<SystemConfig>,
    pub services: Vec<Service>,
    pub connectors: Vec<Connector>,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let cfg = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&cfg)?)
    }

    pub fn service_by_name(&self, name: &str) -> Option<&Service> {
        self.services.iter().find(|s| s.name() == name)
    }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
#[serde(tag = "provider")]
pub enum Service {
    #[serde(rename = "pubsub")]
    PubSub(PubSubConfig),
    #[serde(rename = "kafka")]
    Kafka(KafkaConfig),
    #[serde(rename = "mongodb")]
    MongoDb(MongoDbConfig),
    #[serde(rename = "http")]
    Http(HttpConfig),
    #[serde(rename = "udf")]
    Udf(UdfConfig),
}

impl Service {
    pub fn name(&self) -> &str {
        match self {
            Service::PubSub(c) => &c.name,
            Service::Kafka(c) => &c.name,
            Service::MongoDb(c) => &c.name,
            Service::Http(c) => &c.name,
            Service::Udf(c) => &c.name,
        }
    }

    pub fn provider(&self) -> &'static str {
        match self {
            Service::PubSub(_) => "pubsub",
            Service::Kafka(_) => "kafka",
            Service::MongoDb(_) => "mongodb",
            Service::Http(_) => "http",
            Service::Udf(_) => "udf",
        }
    }
}

impl Masked for Service {
    fn masked(&self) -> Self {
        match self {
            Service::PubSub(c) => Service::PubSub(c.masked()),
            Service::Kafka(c) => Service::Kafka(c.masked()),
            Service::MongoDb(c) => Service::MongoDb(c.masked()),
            Service::Http(c) => Service::Http(c.masked()),
            Service::Udf(c) => Service::Udf(c.masked()),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Connector {
    #[serde(default = "default_connector_enabled")]
    #[serde(skip_serializing)]
    pub enabled: bool,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<BatchConfig>,
    pub checkpoint: Option<CheckpointConnectorConfig>,
    pub source: SourceServiceConfigReference,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub middlewares: Option<Vec<ServiceConfigReference>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<SchemaServiceConfigReference>>,
    pub sinks: Vec<ServiceConfigReference>,
}

impl Connector {
    pub fn batch_config(&self) -> (usize, bool) {
        match &self.batch {
            Some(BatchConfig::Count { size }) => (*size, true),
            None => (1, false),
        }
    }
}

fn default_connector_enabled() -> bool {
    true
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct ServiceConfigReference {
    pub service_name: String,
    pub resource: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
    pub output_encoding: Encoding,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct SourceServiceConfigReference {
    pub service_name: String,
    pub resource: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_encoding: Option<Encoding>,
    pub output_encoding: Encoding,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct SchemaServiceConfigReference {
    pub id: String,
    pub service_name: String,
    pub resource: String,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BatchConfig {
    Count { size: usize },
    // Window { step_seconds: u64 }
}

#[derive(Deserialize, Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    Avro,
    Json,
    Bson,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct CheckpointConnectorConfig {
    pub enabled: bool,
}

impl Default for CheckpointConnectorConfig {
    fn default() -> Self {
        Self { enabled: false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_connector_config_default_is_disabled() {
        let config = CheckpointConnectorConfig::default();
        assert!(!config.enabled);
    }

    #[test]
    fn checkpoint_connector_config_deserialize_enabled() {
        let toml_str = r#"enabled = true"#;
        let config: CheckpointConnectorConfig = toml::from_str(toml_str).expect("deserialize");
        assert!(config.enabled);
    }

    #[test]
    fn checkpoint_connector_config_deserialize_disabled() {
        let toml_str = r#"enabled = false"#;
        let config: CheckpointConnectorConfig = toml::from_str(toml_str).expect("deserialize");
        assert!(!config.enabled);
    }

    #[test]
    fn checkpoint_connector_config_serialize_roundtrip() {
        let config = CheckpointConnectorConfig { enabled: true };
        let serialized = serde_json::to_string(&config).expect("serialize");
        let deserialized: CheckpointConnectorConfig =
            serde_json::from_str(&serialized).expect("deserialize");
        assert!(deserialized.enabled);
    }

    mod service_provider_tests {
        use super::*;
        use crate::config::service_config::{
            GcpAuthConfig, HttpConfig, KafkaConfig, MongoDbConfig, PubSubConfig, UdfConfig,
            UdfEngine,
        };
        use std::collections::HashMap;

        #[test]
        fn mongodb_provider() {
            let service = Service::MongoDb(MongoDbConfig {
                name: "test".to_string(),
                connection_string: "mongodb://localhost".to_string(),
                db_name: "test_db".to_string(),
                write_mode: Default::default(),
            });
            assert_eq!(service.provider(), "mongodb");
        }

        #[test]
        fn kafka_provider() {
            let service = Service::Kafka(KafkaConfig {
                name: "test".to_string(),
                offset_seek_back_seconds: None,
                config: HashMap::new(),
            });
            assert_eq!(service.provider(), "kafka");
        }

        #[test]
        fn pubsub_provider() {
            let service = Service::PubSub(PubSubConfig {
                name: "test".to_string(),
                auth: GcpAuthConfig::StaticToken {
                    env_token_name: "TOKEN".to_string(),
                },
            });
            assert_eq!(service.provider(), "pubsub");
        }

        #[test]
        fn http_provider() {
            let service = Service::Http(HttpConfig {
                name: "test".to_string(),
                host: "http://localhost".to_string(),
                max_retries: None,
                base_backoff_ms: None,
                connection_timeout_sec: None,
                timeout_sec: None,
                tcp_keepalive_sec: None,
            });
            assert_eq!(service.provider(), "http");
        }

        #[test]
        fn udf_provider() {
            let service = Service::Udf(UdfConfig {
                name: "test".to_string(),
                script_path: "/path/to/script.rhai".to_string(),
                engine: UdfEngine::Rhai,
                sources: None,
            });
            assert_eq!(service.provider(), "udf");
        }
    }
}
