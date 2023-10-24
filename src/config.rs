use serde_derive::Deserialize;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Config {
    #[serde(rename = "gcp_service_account_key_path")]
    pub gcp_serv_acc_key_path: String,
    pub connectors: Vec<Connector>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Connector {
    pub name: String,
    pub db_connection: String,
    pub db_name: String,
    pub db_collection: String,
    pub schema: SchemaCfg,
    pub topic: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SchemaCfg {
    pub provider: SchemaProviderName,
    pub id: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum SchemaProviderName {
    Gcp,
    MongoDB,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let cfg = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&cfg)?)
    }
}
