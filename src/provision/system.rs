use std::sync::Arc;

use anyhow::anyhow;
use tokio::sync::RwLock;

use crate::{
    config::{Config, Service, service_config::MongoDbConfig, system::StartupState},
    job_manager::{JobStorage, in_memory::InMemoryJobStore, mongodb_store::MongoDBJobStore},
    mongodb::db_client,
    provision::{
        encryption::{self, Encryptor},
        registry::{
            ServiceRegistry, ServiceStorage, in_memory::InMemoryServiceStorage,
            mongodb_storage::MongoDbServiceStorage,
        },
    },
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
            let job_store: JobStorage = Box::new(InMemoryJobStore::new());
            return Ok((job_store, StartupState::ForceFromFile));
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
            let job_store: JobStorage =
                Box::new(MongoDBJobStore::new(db, lifecycle_cfg.resource.clone()));
            Ok((job_store, startup_state))
        }
        _ => anyhow::bail!(
            "unsupported service provider for job lifecycle management: {}",
            service_name
        ),
    }
}

pub async fn init_service_storage(config: &Config) -> anyhow::Result<ServiceStorage> {
    let (enc_key_path, storage_cfg) = match config
        .system
        .as_ref()
        .map(|s| (s.encryption_key_path.as_ref(), s.service_lifecycle.as_ref()))
    {
        Some((key_path, Some(cfg))) => (key_path, cfg),
        _ => {
            let in_memory: ServiceStorage = Box::new(InMemoryServiceStorage::new());
            return Ok(in_memory);
        }
    };

    let service_name = &storage_cfg.service_name;
    let service_config = config.service_by_name(service_name).ok_or_else(|| {
        anyhow!(
            "service storage initialization: service not found: {}",
            service_name
        )
    })?;

    let mongo_config: &MongoDbConfig = service_config.try_into()?;

    let mongo_client =
        db_client(mongo_config.name.clone(), &mongo_config.connection_string).await?;
    let db = mongo_client.database(&mongo_config.db_name);

    let encryption_key = encryption::get_encryption_key(enc_key_path).await?;
    let encryptor = Encryptor::new(encryption_key.as_slice())?;

    let service_storage: ServiceStorage = Box::new(MongoDbServiceStorage::new(
        db,
        &storage_cfg.resource,
        encryptor,
    ));

    Ok(service_storage)
}
