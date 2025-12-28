use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::Config;
use crate::config::system::StartupState;
use crate::job_manager::{JobManager, JobState, JobStateChange};
use crate::provision::registry::ServiceRegistry;
use crate::provision::system::init_job_storage;

/// Initializes and starts the event listeners for all the connectors
pub async fn listen_streams(
    exit_tx: UnboundedSender<JobStateChange>,
    cfg: Config,
) -> anyhow::Result<JobManager> {
    let service_registry = ServiceRegistry::new(cfg.clone()).await?;
    let service_registry = Arc::new(RwLock::new(service_registry));

    let (job_store, startup_state) = init_job_storage(&cfg, service_registry.clone()).await?;
    let mut job_manager = JobManager::new(service_registry, job_store, exit_tx);

    match startup_state {
        StartupState::Keep => startup_keep(&mut job_manager).await?,
        StartupState::ForceFromFile => startup_force_from_file(&mut job_manager, &cfg).await?,
        StartupState::SeedFromFile => startup_seed_from_file(&mut job_manager, &cfg).await?,
    };

    Ok(job_manager)
}

async fn startup_keep(job_manager: &mut JobManager) -> anyhow::Result<()> {
    let all_jobs = job_manager.list_jobs().await?;
    for job in all_jobs {
        if matches!(job.desired_state, JobState::Running) {
            if let Some(connector_cfg) = job.pipeline {
                job_manager.start_job(connector_cfg).await?;
            }
        }
    }

    Ok(())
}

async fn startup_force_from_file(job_manager: &mut JobManager, cfg: &Config) -> anyhow::Result<()> {
    job_manager.stop_all_jobs().await?;
    for connector_cfg in cfg.connectors.iter().filter(|c| c.enabled).cloned() {
        job_manager.start_job(connector_cfg).await?;
    }

    Ok(())
}

async fn startup_seed_from_file(job_manager: &mut JobManager, cfg: &Config) -> anyhow::Result<()> {
    let all_jobs = job_manager.list_jobs().await?;
    if all_jobs.is_empty() {
        for connector_cfg in cfg.connectors.iter().filter(|c| c.enabled).cloned() {
            job_manager.start_job(connector_cfg).await?;
        }
        return Ok(());
    }

    startup_keep(job_manager).await
}
