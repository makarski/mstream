use std::{env, sync::Arc};

use anyhow::Context;
use log::{debug, warn};
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info};

use crate::api::AppState;

mod encoding;
mod http;
mod kafka;
mod mongodb;
mod sink;

pub mod api;
pub mod cmd;
pub mod config;
pub mod job_manager;
pub mod middleware;
pub mod provision;
pub mod pubsub;
pub mod schema;
pub mod source;

pub async fn run_app(config_path: &str) -> anyhow::Result<()> {
    let config = config::Config::load(config_path).with_context(|| "failed to load config")?;
    debug!("config: {:?}", config);

    let api_port = env::var("MSTREAM_API_PORT")
        .ok()
        .and_then(|port_str| port_str.parse::<u16>().ok())
        .unwrap_or(8787);

    let (exit_tx, mut exit_rx) = mpsc::unbounded_channel::<String>();
    let jm = cmd::listen_streams(exit_tx, config).await?;
    info!(
        "running jobs: {}",
        jm.list_jobs()
            .iter()
            .map(|metadata| metadata.name.clone())
            .collect::<Vec<String>>()
            .join("; ")
    );

    let shared_jm = Arc::new(Mutex::new(jm));
    let api_app_state = AppState::new(shared_jm);

    tokio::spawn(async move {
        if let Err(err) = api::start_server(api_app_state, api_port).await {
            error!("failed to start API server: {}", err);
        }
    });

    while let Some(job_name) = exit_rx.recv().await {
        warn!("stream listener exited: {}", job_name)
    }

    warn!("all stream listeners exited");

    Ok(())
}
