use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::{
    sync::{RwLock, mpsc::UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub mod error;
pub mod in_memory;
pub mod mongodb_store;

use crate::{
    checkpoint::{Checkpoint, CheckpointerError},
    config::CheckpointConnectorConfig,
    job_manager::error::{JobManagerError, Result},
};
use crate::{
    config::{Connector, Masked, Service},
    provision::{pipeline::builder::PipelineBuilder, registry::ServiceRegistry},
};

pub type JobStorage = Box<dyn JobLifecycleStorage + Send + Sync>;

pub struct JobManager {
    service_registry: Arc<RwLock<ServiceRegistry>>,
    cancel_all: CancellationToken,
    job_store: JobStorage,
    running_jobs: HashMap<String, JobContainer>,
    exit_tx: Option<UnboundedSender<JobStateChange>>,
}

struct JobContainer {
    work_handle: JoinHandle<()>,
    source_handle: JoinHandle<()>,
    cancel_token: CancellationToken,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JobMetadata {
    pub name: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub stopped_at: Option<chrono::DateTime<chrono::Utc>>,
    pub state: JobState,
    pub desired_state: JobState,
    #[serde(rename = "linked_services")]
    pub service_deps: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<Connector>,
}

pub struct JobStateChange {
    pub job_name: String,
    pub new_state: JobState,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Running,
    Stopped,
    Failed,
}

impl Display for JobState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JobState::Running => write!(f, "running"),
            JobState::Stopped => write!(f, "stopped"),
            JobState::Failed => write!(f, "failed"),
        }
    }
}

impl JobManager {
    pub fn new(
        service_registry: Arc<RwLock<ServiceRegistry>>,
        job_store: JobStorage,
        exit_tx: UnboundedSender<JobStateChange>,
    ) -> JobManager {
        Self {
            service_registry,
            cancel_all: CancellationToken::new(),
            running_jobs: HashMap::new(),
            job_store,
            exit_tx: Some(exit_tx),
        }
    }

    async fn register_job(
        &mut self,
        name: String,
        work_handle: JoinHandle<()>,
        source_handle: JoinHandle<()>,
        cancel_token: CancellationToken,
        service_deps: Vec<String>,
        pipeline: Option<Connector>,
    ) -> anyhow::Result<JobMetadata> {
        let metadata = JobMetadata {
            name: name.clone(),
            started_at: chrono::Utc::now(),
            stopped_at: None,
            state: JobState::Running,
            desired_state: JobState::Running,
            service_deps,
            pipeline,
        };

        let job_container = JobContainer {
            work_handle,
            source_handle,
            cancel_token,
        };

        self.job_store.save(metadata.clone()).await?;
        self.running_jobs.insert(name, job_container);
        Ok(metadata)
    }

    pub async fn handle_job_exit(&mut self, state_change: JobStateChange) -> anyhow::Result<()> {
        let job_name = &state_change.job_name;
        if let Some(jc) = self.running_jobs.remove(job_name) {
            warn!("handling exited job: {}", job_name);
            jc.cancel_token.cancel();
        }

        if let Some(mut metadata) = self.job_store.get(job_name).await? {
            metadata.state = state_change.new_state;
            metadata.stopped_at = Some(chrono::Utc::now());
            metadata.service_deps.clear();
            self.job_store.save(metadata).await?;
        } else {
            warn!("failed to find metadata for exited job: {}", job_name);
        }

        Ok(())
    }

    pub async fn stop_job(&mut self, job_name: &str) -> Result<()> {
        // Check if job exists first
        let job_exists = self.job_store.get(job_name).await.ok().flatten().is_some();
        if !job_exists {
            return Err(JobManagerError::JobNotFound(job_name.to_string()));
        }

        self.set_desired_job_state(job_name, JobState::Stopped)
            .await
            .ok(); // Ignore errors here since we already checked existence

        if let Some(jc) = self.running_jobs.remove(job_name) {
            info!("stopping job: {}", job_name);
            jc.cancel_token.cancel();

            // wait for the job to finish
            let (work_res, source_res) = tokio::join!(jc.work_handle, jc.source_handle);
            if let Err(err) = work_res {
                error!("job work task panicked: {}: {}", err, job_name);
            }

            if let Err(err) = source_res {
                error!("job source task panicked: {}: {}", err, job_name);
            }

            info!("job stopped: {}", job_name);
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) {
        info!("shutting down all jobs");
        self.cancel_all.cancel();

        // ban new jobs
        // wait for all running jobs to finish
        self.exit_tx = None;
    }

    pub async fn stop_all_jobs(&mut self) -> anyhow::Result<()> {
        let jobs = self.job_store.list_all().await?;

        for job in jobs {
            if let Err(err) = self.stop_job(&job.name).await {
                error!("failed to stop job during reset '{}': {}", job.name, err);
            }
        }

        Ok(())
    }

    pub async fn list_jobs(&self) -> anyhow::Result<Vec<JobMetadata>> {
        self.job_store.list_all().await
    }

    pub async fn restart_job(&mut self, job_name: &str) -> Result<JobMetadata> {
        let job_metadata = self
            .job_store
            .get(job_name)
            .await
            .ok()
            .flatten()
            .ok_or_else(|| JobManagerError::JobNotFound(job_name.to_string()))?;

        self.stop_job(job_name).await?;

        if let Some(conn_cfg) = job_metadata.pipeline.clone() {
            self.start_job(conn_cfg).await
        } else {
            // This shouldn't happen in normal circumstances, treat as internal error
            // Return as anyhow error which will be converted to InternalError in API layer
            Err(JobManagerError::InternalError(format!(
                "job '{}' has unexpected config error",
                job_name
            )))
        }
    }

    pub async fn start_job(&mut self, conn_cfg: Connector) -> Result<JobMetadata> {
        // Check if job already exists (both running and stored)
        if self.job_exists(&conn_cfg.name).await.unwrap_or(false) {
            return Err(JobManagerError::JobAlreadyExists(conn_cfg.name.clone()));
        }

        let exit_tx = self
            .exit_tx
            .as_ref()
            .ok_or_else(|| {
                JobManagerError::InternalError("job manager is shutting down".to_string())
            })?
            .clone();

        let checkpoint = self
            .load_checkpoint(&conn_cfg.name, &conn_cfg.checkpoint)
            .await;
        let pipeline_builder =
            PipelineBuilder::new(self.service_registry.clone(), conn_cfg.clone(), checkpoint);

        let service_deps = pipeline_builder.service_deps();
        let pipeline = pipeline_builder.build().await.map_err(|e| {
            JobManagerError::InternalError(format!("failed to build pipeline: {}", e))
        })?;

        let job_cancel = self.cancel_all.child_token();
        let task_cancel = job_cancel.clone();

        let (job_name, work_handle, source_handle) =
            pipeline.run(task_cancel, exit_tx).await.map_err(|e| {
                JobManagerError::InternalError(format!("failed to run pipeline: {}", e))
            })?;

        self.register_job(
            job_name,
            work_handle,
            source_handle,
            job_cancel,
            service_deps,
            Some(conn_cfg),
        )
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to register job: {}", e)))
    }

    pub async fn list_services(&self) -> anyhow::Result<Vec<ServiceStatus>> {
        let all_conf_services = self
            .service_registry
            .read()
            .await
            .all_service_definitions()
            .await?;

        let mut statuses = Vec::with_capacity(all_conf_services.len());

        for service in all_conf_services {
            let name = service.name();
            let used_by = self.job_store.get_dependent_jobs(&name).await?;

            statuses.push(ServiceStatus {
                service,
                used_by_jobs: used_by,
            });
        }

        Ok(statuses)
    }

    pub async fn get_job(&self, name: &str) -> anyhow::Result<Option<JobMetadata>> {
        self.job_store.get(name).await
    }

    pub async fn create_service(&self, service_cfg: Service) -> Result<()> {
        let service_name = service_cfg.name().to_string();

        // Check if service already exists
        if self
            .service_registry
            .read()
            .await
            .service_exists(&service_name)
            .await
            .unwrap_or(false)
        {
            return Err(JobManagerError::ServiceAlreadyExists(service_name));
        }

        self.service_registry
            .write()
            .await
            .register_service(service_cfg)
            .await
            .map_err(|e| {
                JobManagerError::InternalError(format!("failed to register service: {}", e))
            })?;

        info!("service '{}' created", service_name);
        Ok(())
    }

    pub async fn remove_service(&self, service_name: &str) -> Result<()> {
        // Check if service exists first
        if !self
            .service_registry
            .read()
            .await
            .service_exists(service_name)
            .await
            .unwrap_or(false)
        {
            return Err(JobManagerError::ServiceNotFound(service_name.to_string()));
        }

        // acquire a write lock before checking for consumers to avoid race conditions
        let mut registry = self.service_registry.write().await;

        let jobs = self
            .job_store
            .get_dependent_jobs(service_name)
            .await
            .map_err(|e| {
                JobManagerError::InternalError(format!("failed to check dependent jobs: {}", e))
            })?;

        if !jobs.is_empty() {
            return Err(JobManagerError::ServiceInUse(
                service_name.to_string(),
                jobs.join(", "),
            ));
        }

        registry.remove_service(service_name).await.map_err(|e| {
            JobManagerError::InternalError(format!("failed to remove service: {}", e))
        })?;

        info!("service '{}' removed", service_name);
        Ok(())
    }

    pub async fn get_service(&self, service_name: &str) -> Result<ServiceStatus> {
        let registry = self.service_registry.read().await;
        let service = registry
            .service_definition(service_name)
            .await
            .map_err(|_| JobManagerError::ServiceNotFound(service_name.to_string()))?;
        let used_by = self
            .job_store
            .get_dependent_jobs(service_name)
            .await
            .map_err(|e| JobManagerError::InternalError(e.to_string()))?;
        Ok(ServiceStatus {
            service,
            used_by_jobs: used_by,
        })
    }

    /// Check if a job exists
    pub async fn job_exists(&self, name: &str) -> anyhow::Result<bool> {
        Ok(self.running_jobs.contains_key(name))
    }

    async fn set_desired_job_state(
        &mut self,
        job_name: &str,
        state: JobState,
    ) -> anyhow::Result<()> {
        if let Some(mut metadata) = self.job_store.get(job_name).await? {
            metadata.desired_state = state;
            self.job_store.save(metadata).await?;
        }

        Ok(())
    }

    async fn load_checkpoint(
        &self,
        job_name: &str,
        checkpoint_cfg: &Option<CheckpointConnectorConfig>,
    ) -> Option<Checkpoint> {
        if !checkpoint_cfg.as_ref().map_or(false, |cfg| cfg.enabled) {
            info!("checkpointing disabled for job: {}", job_name);
            return None;
        }

        let cp = self
            .service_registry
            .read()
            .await
            .checkpointer()
            .load(job_name)
            .await;

        match cp {
            Ok(checkpoint) => Some(checkpoint),
            Err(err) => match err {
                CheckpointerError::NotFound { job_name } => {
                    warn!("no checkpoint found for job: {}", job_name);
                    None
                }
                _ => {
                    error!("failed to load checkpoint for job '{}': {}", job_name, err);
                    None
                }
            },
        }
    }
}

#[derive(Serialize)]
pub struct ServiceStatus {
    #[serde(flatten)]
    pub service: Service,
    pub used_by_jobs: Vec<String>,
}

impl Masked for ServiceStatus {
    fn masked(&self) -> Self {
        ServiceStatus {
            service: self.service.masked(),
            used_by_jobs: self.used_by_jobs.clone(),
        }
    }
}

#[async_trait::async_trait]
pub trait JobLifecycleStorage {
    async fn list_all(&self) -> anyhow::Result<Vec<JobMetadata>>;
    async fn save(&mut self, metadata: JobMetadata) -> anyhow::Result<()>;
    async fn get(&self, name: &str) -> anyhow::Result<Option<JobMetadata>>;
    async fn get_dependent_jobs(&self, service_name: &str) -> anyhow::Result<Vec<String>>;
}
