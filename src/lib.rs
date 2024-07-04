use gauth::{serv_account::ServiceAccount, token_provider::AsyncTokenProvider};
use log::{debug, warn};
use tokio::sync::mpsc;

mod db;
mod encoding;
mod sink;

pub mod cmd;
pub mod config;
pub mod pubsub;
pub mod schema;

pub async fn run_app(config_path: &str) -> anyhow::Result<()> {
    let config = config::Config::load(config_path)?;
    debug!("config: {:?}", config);

    let worker_count = config.connectors.len();
    let (tx, mut rx) = mpsc::channel::<String>(worker_count);

    let service_account =
        ServiceAccount::from_file(&config.gcp_serv_acc_key_path, pubsub::SCOPES.to_vec());

    let tp = AsyncTokenProvider::new(service_account).with_interval(600);
    tp.watch_updates().await;

    cmd::listener::listen_streams(tx, config, tp).await?;
    for _ in 0..worker_count {
        match rx.recv().await {
            Some(cnt_name) => warn!("stream listener exited: {}", cnt_name),
            None => warn!("stream listener exited: None"),
        }
    }

    warn!("all stream listeners exited");

    Ok(())
}
