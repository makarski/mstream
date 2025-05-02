use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Config {
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

    pub fn has_mongo_db(&self) -> bool {
        self.services
            .iter()
            .any(|s| matches!(s, Service::MongoDb { .. }))
    }

    pub fn has_pubsub(&self) -> bool {
        self.services
            .iter()
            .any(|s| matches!(s, Service::PubSub { .. }))
    }

    pub fn has_http(&self) -> bool {
        self.services
            .iter()
            .any(|s| matches!(s, Service::Http { .. }))
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "provider")]
pub enum Service {
    #[serde(rename = "pubsub")]
    PubSub { name: String, auth: GcpAuthConfig },
    #[serde(rename = "kafka")]
    Kafka {
        name: String,
        offset_seek_back_seconds: Option<u64>,
        #[serde(flatten)]
        #[serde(deserialize_with = "deserialize_hasmap_with_env_vals")]
        config: HashMap<String, String>,
    },
    #[serde(rename = "mongodb")]
    MongoDb {
        name: String,
        connection_string: String,
        db_name: String,
    },
    #[serde(rename = "http")]
    Http {
        name: String,
        host: String,
        max_retries: Option<u32>,
        base_backoff_ms: Option<u64>,
        connection_timeout_sec: Option<u64>,
        timeout_sec: Option<u64>,
        tcp_keepalive_sec: Option<u64>,
    },
}

fn deserialize_hasmap_with_env_vals<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let configs: HashMap<String, String> = HashMap::deserialize(deserializer)?;
    let mut deserialized = HashMap::with_capacity(configs.capacity());

    for (k, v) in configs.into_iter() {
        if !v.starts_with("env:") {
            deserialized.insert(k, v);
            continue;
        }

        let env_key = &v[4..];
        let val = std::env::var(env_key).map_err(|err| {
            Error::custom(format!(
                "Environment variable '{}' not found: '{}'",
                env_key, err
            ))
        })?;

        deserialized.insert(k, val);
    }

    Ok(deserialized)
}

impl Service {
    pub fn name(&self) -> &str {
        match self {
            Service::PubSub { name, .. } => name.as_str(),
            Service::Kafka { name, .. } => name.as_str(),
            Service::MongoDb { name, .. } => name.as_str(),
            Service::Http { name, .. } => name.as_str(),
        }
    }
}

// rework this with GCP auth and service caching?
// think about cases for kafka
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind")]
pub enum GcpAuthConfig {
    #[serde(rename = "static_token")]
    StaticToken { env_token_name: String },
    #[serde(rename = "service_account")]
    ServiceAccount { account_key_path: String },
}

#[derive(Deserialize, Debug, Clone)]
pub struct Connector {
    pub name: String,
    pub source: SourceServiceConfigReference,
    pub middlewares: Option<Vec<ServiceConfigReference>>,
    pub schemas: Option<Vec<SchemaServiceConfigReference>>,
    pub sinks: Vec<ServiceConfigReference>,
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

#[derive(Deserialize, Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    Avro,
    Json,
    Bson,
    // Other can be used for any other encoding
    // e.g. Protobuf, MsgPack, etc.
    // This can be used for pathtrough or in
    // combination with middlewares
    Other,
}

impl Default for Encoding {
    fn default() -> Self {
        Encoding::Other
    }
}
