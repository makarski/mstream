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

    if matches!(startup_state, StartupState::ForceFromFile) {
        job_manager.stop_all_jobs().await?;
        for connector_cfg in cfg.connectors.iter().filter(|c| c.enabled).cloned() {
            job_manager.start_job(connector_cfg).await?;
        }
    }

    if matches!(startup_state, StartupState::Keep) {
        let all_jobs = job_manager.list_jobs().await?;
        for job in all_jobs {
            if matches!(job.desired_state, JobState::Running) {
                if let Some(connector_cfg) = job.pipeline {
                    job_manager.start_job(connector_cfg).await?;
                }
            }
        }
    }

    if matches!(startup_state, StartupState::SeedFromFile) {
        let all_jobs = job_manager.list_jobs().await?;
        if all_jobs.is_empty() {
            for connector_cfg in cfg.connectors.iter().filter(|c| c.enabled).cloned() {
                job_manager.start_job(connector_cfg).await?;
            }
        } else {
            for job in all_jobs {
                if matches!(job.desired_state, JobState::Running) {
                    if let Some(connector_cfg) = job.pipeline {
                        job_manager.start_job(connector_cfg).await?;
                    }
                }
            }
        }
    }

    Ok(job_manager)
}
