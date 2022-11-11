use serde_derive::Deserialize;
use toml;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub connectors: Vec<Connector>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Connector {
    pub name: String,
    pub db_connection: String,
    pub db_name: String,
    pub db_collection: String,
    pub schema: String,
    pub topic: String,
    pub subscription: String,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let cfg = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&cfg)?)
    }
}
