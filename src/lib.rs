use gauth::serv_account::ServiceAccount;
use log::info;

pub mod cmd;
pub mod config;
mod db;
mod encoding;
pub mod pubsub;

pub async fn run_app(config_path: &str) -> anyhow::Result<()> {
    let config = config::Config::load(config_path)?;
    info!("config: {:?}", config);

    let service_account = ServiceAccount::from_file(
        config.gcp_serv_acc_key_path.as_ref().unwrap(),
        pubsub::SCOPES.to_vec(),
    );

    cmd::listener::listen_streams(config, service_account).await
}
