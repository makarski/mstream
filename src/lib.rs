use std::{env, sync::Arc};

use anyhow::Context;
use tokio::{
    signal,
    sync::{Mutex, mpsc},
};
use tracing::{error, info, warn};

use crate::{
    api::{AppState, Stores},
    config::system::LogsConfig,
    job_manager::JobStateChange,
    logs::LogBuffer,
};

mod http;
mod kafka;
mod mongodb;
mod sink;

pub mod api;
pub mod checkpoint;
pub mod cmd;
pub mod config;
pub mod encoding;
pub mod job_manager;
pub mod logs;
pub mod middleware;
pub mod provision;
pub mod pubsub;
pub mod schema;
pub mod source;
pub mod testing;
pub mod ui;
pub mod workspace;

pub async fn run_app(config_path: &str) -> anyhow::Result<()> {
    let config = config::Config::load(config_path).with_context(|| "failed to load config")?;
    let logs_config = config
        .system
        .as_ref()
        .and_then(|s| s.logs.clone())
        .unwrap_or_default();
    let log_buffer = LogBuffer::new(logs_config.buffer_capacity);
    run_app_with_log_buffer(config, log_buffer, logs_config).await
}

async fn log_job_count(jm: &job_manager::JobManager) {
    match jm.list_jobs().await {
        Ok(jobs) if jobs.is_empty() => warn!("no jobs configured"),
        Ok(jobs) => info!("configured jobs: {}", jobs.len()),
        Err(err) => error!("failed to list jobs: {}", err),
    }
}

async fn extract_stores(shared_jm: &Arc<Mutex<job_manager::JobManager>>) -> Stores {
    let jm_lock = shared_jm.lock().await;
    let registry = jm_lock.service_registry.read().await;
    Stores {
        test_suite_store: registry.test_suite_store(),
        workspace_store: registry.workspace_store(),
    }
}

async fn handle_job_exit(
    shared_jm: &Arc<Mutex<job_manager::JobManager>>,
    state_change: JobStateChange,
) {
    let job_name = state_change.job_name.clone();
    tracing::warn!(job_name = %job_name, "stream listener exited");
    match shared_jm.lock().await.handle_job_exit(state_change).await {
        Ok(_) => tracing::info!(job_name = %job_name, "handled job exit"),
        Err(err) => tracing::error!(job_name = %job_name, "failed to handle job exit: {}", err),
    }
}

pub async fn run_app_with_log_buffer(
    config: config::Config,
    log_buffer: LogBuffer,
    logs_config: LogsConfig,
) -> anyhow::Result<()> {
    let api_port = env::var("MSTREAM_API_PORT")
        .ok()
        .and_then(|port_str| port_str.parse::<u16>().ok())
        .unwrap_or(8719);

    let (exit_tx, mut exit_rx) = mpsc::unbounded_channel::<JobStateChange>();
    let jm = cmd::listen_streams(exit_tx, config).await?;
    log_job_count(&jm).await;

    let shared_jm = Arc::new(Mutex::new(jm));
    let stores = extract_stores(&shared_jm).await;
    let api_app_state = AppState::new(shared_jm.clone(), log_buffer, logs_config, stores);

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
                    Some(state_change) => handle_job_exit(&shared_jm, state_change).await,
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
