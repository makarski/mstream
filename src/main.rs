use anyhow::anyhow;
use log::info;

mod cmd;
mod config;
mod db;
mod encoding;
mod pubsub;
mod registry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::try_init()?;

    let config = config::Config::load("config.toml")?;
    info!("config: {:?}", config);

    let agrs: Vec<String> = std::env::args().collect();
    info!("cli args: {:?}", agrs);

    let access_token = agrs
        .get(1)
        .ok_or_else(|| anyhow!("access token not provided"))?;

    cmd::listener::listen(config.clone(), access_token.to_string()).await?;
    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => log::error!("unable to listen to shutdown signal: {}", err),
    }

    Ok(())
}
