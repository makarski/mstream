use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    config::{Config, Service, system::StartupState},
    job_manager::{JobStorage, in_memory::InMemoryJobStore},
    provision::registry::ServiceRegistry,
};

pub async fn init_job_storage(
    config: &Config,
    sr: Arc<RwLock<ServiceRegistry>>,
) -> anyhow::Result<(JobStorage, StartupState)> {
    let lifecycle_cfg = match config
        .system
        .as_ref()
        .and_then(|s| s.job_lifecycle.as_ref())
    {
        Some(cfg) => cfg,
        None => {
            return Ok((
                Box::new(InMemoryJobStore::new()),
                StartupState::ForceFromFile,
            ));
        }
    };

    let service_name = &lifecycle_cfg.service_name;
    let registry = sr.read().await;
    let service_definition = registry.service_definition(service_name).await?;
    let startup_state = lifecycle_cfg.startup_state.clone();

    match service_definition {
        Service::MongoDb(mongo_config) => {
            let db_client = registry.mongodb_client(&service_name).await?;
            let db = db_client.database(&mongo_config.db_name);
            Ok((
                Box::new(crate::job_manager::mongodb_store::MongoDBJobStore::new(
                    db,
                    lifecycle_cfg.resource.clone(),
                )),
                startup_state,
            ))
        }
        _ => anyhow::bail!(
            "unsupported service provider for job lifecycle management: {}",
            service_name
        ),
    }
}
