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

    let agrs: Vec<String> = std::env::args().collect();
    info!("cli args: {:?}", agrs);

    let access_token = agrs
        .get(1)
        .ok_or_else(|| anyhow!("access token not provided"))?;

    mstream::run_app(access_token, "config.toml").await?;
    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => log::error!("unable to listen to shutdown signal: {}", err),
    }

    Ok(())
}
