use tokio::sync::mpsc::UnboundedSender;

use crate::config::Config;
use crate::job_manager::JobManager;
use crate::provision::registry::ServiceRegistry;

/// Initializes and starts the event listeners for all the connectors
pub async fn listen_streams(
    exit_tx: UnboundedSender<String>,
    cfg: Config,
) -> anyhow::Result<JobManager> {
    let service_registry = ServiceRegistry::new(cfg.clone()).await?;
    let mut job_manager = JobManager::new(service_registry, exit_tx);

    for connector_cfg in cfg.connectors.iter().filter(|c| c.enabled).cloned() {
        job_manager.start_job(connector_cfg).await?;
    }

    Ok(job_manager)
}
