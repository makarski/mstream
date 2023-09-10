use log::info;

const CONFIG_FILE: &str = "mstream-config.toml";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::try_init()?;
    info!("starting mstream...");

    mstream::run_app(CONFIG_FILE).await?;
    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => log::error!("unable to listen to shutdown signal: {}", err),
    }

    Ok(())
}
