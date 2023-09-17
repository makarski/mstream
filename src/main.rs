use log::info;

const CONFIG_FILE: &str = "mstream-config.toml";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::try_init_timed()?;
    info!("starting mstream...");
    mstream::run_app(CONFIG_FILE).await?;

    Ok(())
}
