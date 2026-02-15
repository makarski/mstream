use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info};

use crate::config::system::StartupState;
use crate::config::{Config, Connector};
use crate::job_manager::{JobManager, JobState, JobStateChange};
use crate::provision::registry::ServiceRegistry;
use crate::provision::system::{init_job_storage, init_service_storage};

/// Initializes and starts the event listeners for all the connectors
pub async fn listen_streams(
    exit_tx: UnboundedSender<JobStateChange>,
    cfg: Config,
) -> anyhow::Result<JobManager> {
    let mut service_storage = init_service_storage(&cfg).await?;
    for service in cfg.services.iter().cloned() {
        service_storage.save(service).await?;
    }
    let mut service_registry = ServiceRegistry::new(service_storage, cfg.system.clone());
    service_registry.init().await?;

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

async fn start_jobs(job_manager: &mut JobManager, connectors: impl IntoIterator<Item = Connector>) {
    let mut started = 0u32;
    let mut failed = 0u32;

    for connector_cfg in connectors {
        let name = connector_cfg.name.clone();
        match job_manager.start_job(connector_cfg).await {
            Ok(_) => started += 1,
            Err(err) => {
                error!(job_name = %name, "failed to start job: {}", err);
                failed += 1;
            }
        }
    }

    info!("startup: {} started, {} failed", started, failed);
}

async fn startup_keep(job_manager: &mut JobManager) -> anyhow::Result<()> {
    let connectors: Vec<_> = job_manager
        .list_jobs()
        .await?
        .into_iter()
        .filter(|j| matches!(j.desired_state, JobState::Running))
        .filter_map(|j| j.pipeline)
        .collect();

    start_jobs(job_manager, connectors).await;
    Ok(())
}

async fn startup_force_from_file(job_manager: &mut JobManager, cfg: &Config) -> anyhow::Result<()> {
    job_manager.stop_all_jobs().await?;
    let connectors: Vec<_> = cfg
        .connectors
        .iter()
        .filter(|c| c.enabled)
        .cloned()
        .collect();
    start_jobs(job_manager, connectors).await;
    Ok(())
}

async fn startup_seed_from_file(job_manager: &mut JobManager, cfg: &Config) -> anyhow::Result<()> {
    let all_jobs = job_manager.list_jobs().await?;
    if all_jobs.is_empty() {
        let connectors: Vec<_> = cfg
            .connectors
            .iter()
            .filter(|c| c.enabled)
            .cloned()
            .collect();
        start_jobs(job_manager, connectors).await;
        return Ok(());
    }

    startup_keep(job_manager).await
}
