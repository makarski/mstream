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
    let key_lower = key.to_lowercase();
    key_lower.contains("password")
        || key_lower.contains("secret")
        || key_lower.contains("token")
        || key_lower.contains("jaas")
        || key_lower.ends_with(".key")
        || key_lower.contains("credential")
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

#[cfg(test)]
mod tests {
    use super::*;

    mod is_sensitive_key_tests {
        use super::*;

        #[test]
        fn detects_password() {
            assert!(is_sensitive_key("db_password"));
            assert!(is_sensitive_key("password"));
            assert!(is_sensitive_key("PASSWORD_FIELD"));
            assert!(is_sensitive_key("DB_PASSWORD"));
        }

        #[test]
        fn detects_secret() {
            assert!(is_sensitive_key("client_secret"));
            assert!(is_sensitive_key("secret_key"));
        }

        #[test]
        fn detects_token() {
            assert!(is_sensitive_key("auth_token"));
            assert!(is_sensitive_key("token_value"));
        }

        #[test]
        fn detects_jaas() {
            assert!(is_sensitive_key("sasl.jaas.config"));
        }

        #[test]
        fn detects_dot_key_suffix() {
            assert!(is_sensitive_key("ssl.key"));
            assert!(is_sensitive_key("private.key"));
            // should not match partial
            assert!(!is_sensitive_key("keystore"));
        }

        #[test]
        fn detects_credential() {
            assert!(is_sensitive_key("user_credential"));
            assert!(is_sensitive_key("credentials"));
        }

        #[test]
        fn ignores_non_sensitive() {
            assert!(!is_sensitive_key("bootstrap.servers"));
            assert!(!is_sensitive_key("group.id"));
            assert!(!is_sensitive_key("client_id"));
            assert!(!is_sensitive_key("hostname"));
        }
    }

    mod deserialize_env_vals_tests {
        use super::*;

        #[test]
        fn plain_values_pass_through() {
            let json = r#"{"name": "test", "bootstrap.servers": "localhost:9092"}"#;
            let cfg: KafkaConfig = serde_json::from_str(json).unwrap();

            assert_eq!(
                cfg.config.get("bootstrap.servers").unwrap(),
                "localhost:9092"
            );
        }

        #[test]
        fn env_prefix_substitutes_value() {
            // SAFETY: test is single-threaded, no other code depends on this var
            unsafe {
                std::env::set_var("TEST_KAFKA_PASSWORD", "super_secret");
            }

            let json = r#"{"name": "test", "sasl.password": "env:TEST_KAFKA_PASSWORD"}"#;
            let cfg: KafkaConfig = serde_json::from_str(json).unwrap();

            assert_eq!(cfg.config.get("sasl.password").unwrap(), "super_secret");

            // SAFETY: test cleanup
            unsafe {
                std::env::remove_var("TEST_KAFKA_PASSWORD");
            }
        }

        #[test]
        fn missing_env_var_errors() {
            let json = r#"{"name": "test", "sasl.password": "env:NONEXISTENT_VAR_12345"}"#;
            let result: Result<KafkaConfig, _> = serde_json::from_str(json);

            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("NONEXISTENT_VAR_12345"));
        }
    }

    mod masked_tests {
        use super::*;

        #[test]
        fn kafka_config_masks_sensitive_keys() {
            let mut config = HashMap::new();
            config.insert(
                "bootstrap.servers".to_string(),
                "localhost:9092".to_string(),
            );
            config.insert("sasl.password".to_string(), "my_password".to_string());
            config.insert("ssl.key".to_string(), "private_key_data".to_string());

            let cfg = KafkaConfig {
                name: "test".to_string(),
                offset_seek_back_seconds: None,
                config,
            };

            let masked = cfg.masked();

            assert_eq!(
                masked.config.get("bootstrap.servers").unwrap(),
                "localhost:9092"
            );
            assert_eq!(masked.config.get("sasl.password").unwrap(), "****");
            assert_eq!(masked.config.get("ssl.key").unwrap(), "****");
            assert_eq!(masked.name, "test");
        }

        #[test]
        fn mongodb_config_masks_connection_string() {
            let cfg = MongoDbConfig {
                name: "mongo".to_string(),
                connection_string: "mongodb://user:pass@host:27017".to_string(),
                db_name: "testdb".to_string(),
            };

            let masked = cfg.masked();

            assert_eq!(masked.connection_string, "****");
            assert_eq!(masked.name, "mongo");
            assert_eq!(masked.db_name, "testdb");
        }

        #[test]
        fn pubsub_static_token_masks_env_name() {
            let cfg = PubSubConfig {
                name: "pubsub".to_string(),
                auth: GcpAuthConfig::StaticToken {
                    env_token_name: "MY_SECRET_TOKEN".to_string(),
                },
            };

            let masked = cfg.masked();

            match masked.auth {
                GcpAuthConfig::StaticToken { env_token_name } => {
                    assert_eq!(env_token_name, "****");
                }
                _ => panic!("expected StaticToken variant"),
            }
        }

        #[test]
        fn pubsub_service_account_unchanged() {
            let cfg = PubSubConfig {
                name: "pubsub".to_string(),
                auth: GcpAuthConfig::ServiceAccount {
                    account_key_path: "/path/to/key.json".to_string(),
                },
            };

            let masked = cfg.masked();

            match masked.auth {
                GcpAuthConfig::ServiceAccount { account_key_path } => {
                    assert_eq!(account_key_path, "/path/to/key.json");
                }
                _ => panic!("expected ServiceAccount variant"),
            }
        }

        #[test]
        fn http_config_unchanged() {
            let cfg = HttpConfig {
                name: "api".to_string(),
                host: "https://api.example.com".to_string(),
                max_retries: Some(3),
                base_backoff_ms: None,
                connection_timeout_sec: None,
                timeout_sec: None,
                tcp_keepalive_sec: None,
            };

            let masked = cfg.masked();

            assert_eq!(masked.host, "https://api.example.com");
        }
    }

    mod try_from_tests {
        use super::*;
        use crate::config::Service;

        #[test]
        fn mongodb_from_service_succeeds() {
            let cfg = MongoDbConfig {
                name: "mongo".to_string(),
                connection_string: "mongodb://localhost".to_string(),
                db_name: "test".to_string(),
            };
            let service = Service::MongoDb(cfg);

            let result: Result<&MongoDbConfig, _> = (&service).try_into();
            assert!(result.is_ok());
            assert_eq!(result.unwrap().name, "mongo");
        }

        #[test]
        fn mongodb_from_wrong_service_fails() {
            let cfg = HttpConfig {
                name: "http".to_string(),
                host: "https://example.com".to_string(),
                max_retries: None,
                base_backoff_ms: None,
                connection_timeout_sec: None,
                timeout_sec: None,
                tcp_keepalive_sec: None,
            };
            let service = Service::Http(cfg);

            let result: Result<&MongoDbConfig, _> = (&service).try_into();
            assert!(result.is_err());
        }
    }
}
