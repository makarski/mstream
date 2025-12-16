pub mod event_handler;
pub mod services;

use services::ServiceFactory;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::Config;
use crate::job_manager::JobManager;

/// Initializes and starts the event listeners for all the connectors
pub async fn listen_streams(
    done_ch: UnboundedSender<String>,
    cfg: Config,
) -> anyhow::Result<JobManager> {
    let service_container = ServiceFactory::new(cfg.clone()).await?;
    let mut job_manager = JobManager::new(service_container);

    for connector_cfg in cfg.connectors.iter().filter(|c| c.enabled).cloned() {
        job_manager
            .start_job(connector_cfg, done_ch.clone())
            .await?;
    }

    Ok(job_manager)
}
