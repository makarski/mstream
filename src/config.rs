use serde_derive::Deserialize;
use std::env;

const DEFAULT_KEY_PATH: &str = "service_account_key.json";
const KEY_PATH_ENV_VAR: &str = "MSTREAM_SERVICE_ACCOUNT_KEY_PATH";

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Config {
    pub connectors: Vec<Connector>,
    pub gcp_serv_acc_key_path: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Connector {
    pub name: String,
    pub db_connection: String,
    pub db_name: String,
    pub db_collection: String,
    pub schema: String,
    pub topic: String,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let cfg = std::fs::read_to_string(path)?;
        let mut decoded = toml::from_str::<Config>(&cfg)?;

        let gcp_key_path =
            env::var(KEY_PATH_ENV_VAR).unwrap_or_else(|_| DEFAULT_KEY_PATH.to_string());

        decoded.gcp_serv_acc_key_path = Some(gcp_key_path);

        Ok(decoded)
    }
}
