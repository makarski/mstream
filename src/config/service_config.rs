use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};

use crate::config::Masked;

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct KafkaConfig {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_seek_back_seconds: Option<u64>,
    #[serde(flatten)]
    #[serde(deserialize_with = "deserialize_hashmap_with_env_vals")]
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
    StaticToken { env_token_name: String },
    #[serde(rename = "service_account")]
    ServiceAccount { account_key_path: String },
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

impl Masked for KafkaConfig {
    fn masked(&self) -> Self {
        let mut masked_cfg = self.config.clone();
        for (k, v) in masked_cfg.iter_mut() {
            if is_sensitive_key(k) {
                *v = "****".to_string();
            }
        }
        Self {
            config: masked_cfg,
            ..self.clone()
        }
    }
}

fn is_sensitive_key(key: &str) -> bool {
    key.contains("password")
        || key.contains("secret")
        || key.contains("token")
        || key.contains("jaas")
        || key.ends_with(".key")
        || key.contains("credential")
}

impl Masked for UdfConfig {
    fn masked(&self) -> Self {
        self.clone()
    }
}

impl Masked for MongoDbConfig {
    fn masked(&self) -> Self {
        Self {
            connection_string: "****".to_string(),
            ..self.clone()
        }
    }
}

impl Masked for HttpConfig {
    fn masked(&self) -> Self {
        self.clone()
    }
}

impl Masked for PubSubConfig {
    fn masked(&self) -> Self {
        match &self.auth {
            GcpAuthConfig::StaticToken { .. } => Self {
                auth: GcpAuthConfig::StaticToken {
                    env_token_name: "****".to_string(),
                },
                ..self.clone()
            },
            GcpAuthConfig::ServiceAccount { .. } => self.clone(),
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
