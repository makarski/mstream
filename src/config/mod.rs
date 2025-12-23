use serde::{Deserialize, Serialize};

use crate::config::service_config::{
    HttpConfig, KafkaConfig, MongoDbConfig, PubSubConfig, UdfConfig,
};

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Config {
    pub services: Vec<Service>,
    pub connectors: Vec<Connector>,
}

pub mod service_config;

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let cfg = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&cfg)?)
    }

    pub fn has_service(&self, name: &str) -> bool {
        self.services.iter().any(|s| s.name() == name)
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
}

#[derive(Deserialize, Debug, Clone)]
pub struct Connector {
    #[serde(default = "default_connector_enabled")]
    pub enabled: bool,
    pub name: String,
    pub batch: Option<BatchConfig>,
    pub source: SourceServiceConfigReference,
    pub middlewares: Option<Vec<ServiceConfigReference>>,
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

#[derive(Deserialize, Debug, Clone)]
pub struct ServiceConfigReference {
    pub service_name: String,
    pub resource: String,
    pub schema_id: Option<String>,
    pub output_encoding: Encoding,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SourceServiceConfigReference {
    pub service_name: String,
    pub resource: String,
    pub schema_id: Option<String>,
    pub input_encoding: Option<Encoding>,
    pub output_encoding: Encoding,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SchemaServiceConfigReference {
    pub id: String,
    pub service_name: String,
    pub resource: String,
}

#[derive(Deserialize, Debug, Clone)]
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
