use log::info;

pub mod cmd;
pub mod config;
mod db;
mod encoding;
pub mod pubsub;
mod registry;

pub async fn run_app(access_token: &String, config_path: &str) -> anyhow::Result<()> {
    let config = config::Config::load(config_path)?;
    info!("config: {:?}", config);

    cmd::listener::listen(config, access_token.to_string()).await?;

    Ok(())
}
