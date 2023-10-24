use log::{debug, warn};
use tokio::sync::mpsc;

pub mod cmd;
pub mod config;
mod db;
mod encoding;
pub mod pubsub;
pub mod schema;

pub async fn run_app(config_path: &str) -> anyhow::Result<()> {
    let config = config::Config::load(config_path)?;
    debug!("config: {:?}", config);

    let worker_count = config.connectors.len();
    let (tx, mut rx) = mpsc::channel::<String>(worker_count);

    cmd::listener::listen_streams(tx, config).await?;
    for _ in 0..worker_count {
        match rx.recv().await {
            Some(cnt_name) => warn!("stream listener exited: {}", cnt_name),
            None => warn!("stream listener exited: None"),
        }
    }

    warn!("all stream listeners exited");

    Ok(())
}
