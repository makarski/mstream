use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct KafkaConfig {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_seek_back_seconds: Option<u64>,
    #[serde(flatten)]
    #[serde(deserialize_with = "deserialize_hashmap_with_env_vals")]
    #[serde(serialize_with = "serialize_hashmap_with_secret_vals")]
    pub config: HashMap<String, String>,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct UdfConfig {
    pub name: String,
    pub script_path: String,
    pub engine: UdfEngine,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<UdfScript>>,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct UdfScript {
    pub filename: String,
    pub content: String,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
#[serde(tag = "kind")]
pub enum UdfEngine {
    #[serde(rename = "rhai")]
    Rhai,
    #[cfg(test)]
    #[serde(rename = "undefined")]
    Undefined,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct MongoDbConfig {
    pub name: String,
    #[serde(serialize_with = "serialize_secret")]
    pub connection_string: String,
    pub db_name: String,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct HttpConfig {
    pub name: String,
    pub host: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_backoff_ms: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_timeout_sec: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_sec: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_keepalive_sec: Option<u64>,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct PubSubConfig {
    pub name: String,
    pub auth: GcpAuthConfig,
}

// rework this with GCP auth and service caching?
// think about cases for kafka
#[derive(Deserialize, Debug, Clone, Serialize)]
#[serde(tag = "kind")]
pub enum GcpAuthConfig {
    #[serde(rename = "static_token")]
    StaticToken {
        #[serde(serialize_with = "serialize_secret")]
        env_token_name: String,
    },
    #[serde(rename = "service_account")]
    ServiceAccount { account_key_path: String },
}

fn serialize_secret<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str("****")
}

fn serialize_hashmap_with_secret_vals<S>(
    configs: &HashMap<String, String>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut ser_map = serializer.serialize_map(Some(configs.len()))?;

    for (k, v) in configs.iter() {
        if k.contains("password")
            || k.contains("secret")
            || k.contains("token")
            || k.contains("jaas")
            || k.ends_with(".key")
            || k.contains("credential")
        {
            ser_map.serialize_entry(k, "****")?;
        } else {
            ser_map.serialize_entry(k, v)?;
        }
    }

    ser_map.end()
}

fn deserialize_hashmap_with_env_vals<'de, D>(
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
                "expected KafkaConfig service definition, found: {}",
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
                "expected UdfConfig service definition, found: {}",
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
                "expected MongoDbConfig service definition, found: {}",
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
                "expected HttpConfig service definition, found: {}",
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
                "expected PubSubConfig service definition, found: {}",
                service.name()
            ),
        }
    }
}
