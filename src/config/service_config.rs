use std::collections::HashMap;

use serde::{Deserialize, Deserializer};

#[derive(Deserialize, Debug, Clone)]
pub struct KafkaConfig {
    pub name: String,
    pub offset_seek_back_seconds: Option<u64>,
    #[serde(flatten)]
    #[serde(deserialize_with = "deserialize_hasmap_with_env_vals")]
    pub config: HashMap<String, String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct UdfConfig {
    pub name: String,
    pub script_path: String,
    pub engine: UdfEngine,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind")]
pub enum UdfEngine {
    #[serde(rename = "rhai")]
    Rhai,
}

#[derive(Deserialize, Debug, Clone)]
pub struct MongoDbConfig {
    pub name: String,
    pub connection_string: String,
    pub db_name: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct HttpConfig {
    pub name: String,
    pub host: String,
    pub max_retries: Option<u32>,
    pub base_backoff_ms: Option<u64>,
    pub connection_timeout_sec: Option<u64>,
    pub timeout_sec: Option<u64>,
    pub tcp_keepalive_sec: Option<u64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PubSubConfig {
    pub name: String,
    pub auth: GcpAuthConfig,
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

impl<'a> TryFrom<&'a super::Service> for &'a KafkaConfig {
    type Error = anyhow::Error;

    fn try_from(service: &'a super::Service) -> Result<Self, Self::Error> {
        match service {
            super::Service::Kafka(cfg) => Ok(cfg),
            _ => anyhow::bail!(
                "expected KafkaConfig service defintion, found: {}",
                service.name()
            ),
        }
    }
}

impl<'a> TryFrom<&'a super::Service> for &'a UdfConfig {
    type Error = anyhow::Error;

    fn try_from(service: &'a super::Service) -> Result<Self, Self::Error> {
        match service {
            super::Service::Udf(cfg) => Ok(cfg),
            _ => anyhow::bail!(
                "expected UdfConfig service defintion, found: {}",
                service.name()
            ),
        }
    }
}

impl<'a> TryFrom<&'a super::Service> for &'a MongoDbConfig {
    type Error = anyhow::Error;

    fn try_from(service: &'a super::Service) -> Result<Self, Self::Error> {
        match service {
            super::Service::MongoDb(cfg) => Ok(cfg),
            _ => anyhow::bail!(
                "expected MongoDbConfig service defintion, found: {}",
                service.name()
            ),
        }
    }
}
impl<'a> TryFrom<&'a super::Service> for &'a HttpConfig {
    type Error = anyhow::Error;

    fn try_from(service: &'a super::Service) -> Result<Self, Self::Error> {
        match service {
            super::Service::Http(cfg) => Ok(cfg),
            _ => anyhow::bail!(
                "expected HttpConfig service defintion, found: {}",
                service.name()
            ),
        }
    }
}

impl<'a> TryFrom<&'a super::Service> for &'a PubSubConfig {
    type Error = anyhow::Error;

    fn try_from(service: &'a super::Service) -> Result<Self, Self::Error> {
        match service {
            super::Service::PubSub(cfg) => Ok(cfg),
            _ => anyhow::bail!(
                "expected PubSubConfig service defintion, found: {}",
                service.name()
            ),
        }
    }
}
