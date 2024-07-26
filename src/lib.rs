use anyhow::Context;
use log::{debug, warn};
use tokio::sync::mpsc;

mod db;
mod encoding;
mod kafka;
mod sink;

pub mod cmd;
pub mod config;
pub mod pubsub;
pub mod schema;

pub async fn run_app(config_path: &str) -> anyhow::Result<()> {
    let config = config::Config::load(config_path).with_context(|| "failed to load config")?;
    debug!("config: {:?}", config);

    let worker_count = config.connectors.len();
    let (tx, mut rx) = mpsc::channel::<String>(worker_count);

    cmd::listen_streams(tx, config).await?;
    for _ in 0..worker_count {
        match rx.recv().await {
            Some(cnt_name) => warn!("stream listener exited: {}", cnt_name),
            None => warn!("stream listener exited: None"),
        }
    }

    warn!("all stream listeners exited");

    Ok(())
}
