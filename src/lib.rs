use std::{env, sync::Arc};

use anyhow::Context;
use log::{debug, warn};
use tokio::{
    signal,
    sync::{Mutex, mpsc},
};
use tracing::{error, info};

use crate::{api::AppState, job_manager::JobStateChange};

mod encoding;
mod http;
mod kafka;
mod mcp;
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

    let (exit_tx, mut exit_rx) = mpsc::unbounded_channel::<JobStateChange>();
    let jm = cmd::listen_streams(exit_tx, config).await?;
    match jm.list_jobs().await {
        Ok(jobs) if jobs.is_empty() => {
            warn!("no jobs configured");
        }
        Ok(jobs) => {
            info!("configured jobs: {}", jobs.len());
        }
        Err(err) => {
            error!("failed to list jobs: {}", err);
        }
    };

    let shared_jm = Arc::new(Mutex::new(jm));
    let api_app_state = AppState::new(shared_jm.clone(), api_port);

    tokio::spawn(async move {
        if let Err(err) = api::start_server(api_app_state, api_port).await {
            error!("failed to start API server: {}", err);
        }
    });

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("received Ctrl+C, shutting down...");
                shared_jm.lock().await.shutdown().await;
            }
            res = exit_rx.recv() => {
                match res {
                    Some(state_change) => {
                        let job_name = state_change.job_name.clone();
                        warn!("stream listener exited: {}", job_name);
                        match shared_jm
                            .lock()
                            .await
                            .handle_job_exit(state_change)
                            .await
                        {
                            Ok(_) => {
                                info!("handled job exit for: {}", job_name);
                            }
                            Err(err) => {
                                error!("failed to handle job exit for {}: {}", job_name, err);
                            }
                        }
                    }
                    None => {
                        warn!("all stream listeners have exited");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
