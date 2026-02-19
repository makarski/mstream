use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
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
    provision::{
        pipeline::builder::PipelineBuilder,
        registry::{RegistryError, ServiceRegistry},
    },
};

pub type JobStorage = Box<dyn JobLifecycleStorage + Send + Sync>;

pub struct JobManager {
    pub service_registry: Arc<RwLock<ServiceRegistry>>,
    cancel_all: CancellationToken,
    job_store: JobStorage,
    running_jobs: HashMap<String, JobContainer>,
    exit_tx: Option<UnboundedSender<JobStateChange>>,
}

struct JobContainer {
    work_handle: JoinHandle<()>,
    source_handle: JoinHandle<()>,
    cancel_token: CancellationToken,
    metrics: Arc<JobMetricsCounter>,
}

pub struct JobMetricsCounter {
    pub events_processed: AtomicU64,
    pub bytes_processed: AtomicU64,
    pub errors: AtomicU64,
    pub last_processed_at: AtomicI64,
}

impl JobMetricsCounter {
    pub fn new() -> Self {
        Self {
            events_processed: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_processed_at: AtomicI64::new(0),
        }
    }

    pub fn record_success(&self, bytes: u64) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
        self.last_processed_at
            .store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
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

/// Summary of job counts by state, used by health and stats endpoints.
pub struct JobStateCounts {
    pub running: usize,
    pub stopped: usize,
    pub failed: usize,
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
        metrics: Arc<JobMetricsCounter>,
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
            metrics,
        };

        self.job_store.save(metadata.clone()).await?;
        self.running_jobs.insert(name, job_container);
        Ok(metadata)
    }

    pub async fn handle_job_exit(&mut self, state_change: JobStateChange) -> anyhow::Result<()> {
        let job_name = &state_change.job_name;
        if let Some(jc) = self.running_jobs.remove(job_name) {
            warn!(job_name = %job_name, "handling exited job");
            jc.cancel_token.cancel();
        }

        if let Some(mut metadata) = self.job_store.get(job_name).await? {
            metadata.state = state_change.new_state;
            metadata.stopped_at = Some(chrono::Utc::now());
            metadata.service_deps.clear();
            self.job_store.save(metadata).await?;
        } else {
            warn!(job_name = %job_name, "failed to find metadata for exited job");
        }

        Ok(())
    }

    pub async fn stop_job(&mut self, job_name: &str) -> Result<()> {
        // Check if job exists first
        let metadata = self
            .job_store
            .get(job_name)
            .await
            .ok()
            .flatten()
            .ok_or_else(|| JobManagerError::JobNotFound(job_name.to_string()))?;

        self.set_desired_job_state(job_name, JobState::Stopped)
            .await
            .ok();

        if let Some(jc) = self.running_jobs.remove(job_name) {
            info!(job_name = %job_name, "stopping job");
            jc.cancel_token.cancel();

            let (work_res, source_res) = tokio::join!(jc.work_handle, jc.source_handle);
            if let Err(err) = work_res {
                error!(job_name = %job_name, "job work task panicked: {}", err);
            }

            if let Err(err) = source_res {
                error!(job_name = %job_name, "job source task panicked: {}", err);
            }

            info!(job_name = %job_name, "job stopped");
        } else if matches!(metadata.state, JobState::Running) {
            warn!(job_name = %job_name, "no running task found, marking as stopped");
            if let Some(mut meta) = self.job_store.get(job_name).await? {
                meta.state = JobState::Stopped;
                meta.stopped_at = Some(chrono::Utc::now());
                meta.service_deps.clear();
                self.job_store.save(meta).await?;
            }
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
                error!(job_name = %job.name, "failed to stop job during reset: {}", err);
            }
        }

        Ok(())
    }

    pub async fn list_jobs(&self) -> anyhow::Result<Vec<JobMetadata>> {
        self.job_store.list_all().await
    }

    /// Returns live metrics for a running job, or None if the job is not running.
    pub fn job_metrics(&self, name: &str) -> Option<&Arc<JobMetricsCounter>> {
        self.running_jobs.get(name).map(|jc| &jc.metrics)
    }

    /// Returns job counts grouped by state.
    pub async fn job_state_counts(&self) -> anyhow::Result<JobStateCounts> {
        let jobs = self.job_store.list_all().await?;
        let mut counts = JobStateCounts {
            running: 0,
            stopped: 0,
            failed: 0,
        };
        for job in &jobs {
            match job.state {
                JobState::Running => counts.running += 1,
                JobState::Stopped => counts.stopped += 1,
                JobState::Failed => counts.failed += 1,
            }
        }
        Ok(counts)
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
        let mut pipeline = pipeline_builder.build().await.map_err(|e| {
            JobManagerError::InternalError(format!("failed to build pipeline: {}", e))
        })?;

        let metrics = Arc::new(JobMetricsCounter::new());
        let job_cancel = self.cancel_all.child_token();
        let task_cancel = job_cancel.clone();

        pipeline.metrics = Some(metrics.clone());

        let (job_name, work_handle, source_handle) =
            pipeline.run(task_cancel, exit_tx).await.map_err(|e| {
                JobManagerError::InternalError(format!("failed to run pipeline: {}", e))
            })?;

        self.register_job(
            job_name,
            work_handle,
            source_handle,
            job_cancel,
            metrics,
            service_deps,
            Some(conn_cfg),
        )
        .await
        .map_err(|e| JobManagerError::InternalError(format!("failed to register job: {}", e)))
    }

    pub async fn list_services(&self) -> anyhow::Result<Vec<ServiceStatus>> {
        let registry = self.service_registry.read().await;
        let all_conf_services = registry.all_service_definitions().await?;

        let mut statuses = Vec::with_capacity(all_conf_services.len());

        for service in all_conf_services {
            let name = service.name();
            let used_by = self.job_store.get_dependent_jobs(&name).await?;
            let is_system = registry.is_system_service(&name);

            statuses.push(ServiceStatus {
                service,
                used_by_jobs: used_by,
                is_system,
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

    pub async fn update_resource(
        &self,
        service_name: &str,
        resource: &str,
        content: &str,
    ) -> Result<()> {
        let mut registry = self.service_registry.write().await;

        if !registry.service_exists(service_name).await.unwrap_or(false) {
            return Err(JobManagerError::ServiceNotFound(service_name.to_string()));
        }

        registry
            .update_udf_resource(service_name, resource, content)
            .await
            .map_err(|e| match e {
                RegistryError::ResourceNotFound(res, svc) => {
                    JobManagerError::ResourceNotFound(res, svc)
                }
                RegistryError::NotUdfService(svc) => JobManagerError::InvalidRequest(format!(
                    "service '{}' is not a UDF service",
                    svc
                )),
                RegistryError::ScriptPathNotDirectory(path, svc) => {
                    JobManagerError::InvalidRequest(format!(
                        "script_path '{}' for service '{}' is not a directory",
                        path, svc
                    ))
                }
                RegistryError::Other(err) => {
                    JobManagerError::InternalError(format!("failed to update resource: {}", err))
                }
            })?;

        info!(
            "resource '{}' updated for service '{}'",
            resource, service_name
        );
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
            .map_err(|e| JobManagerError::InternalError(e.to_string()))?;
        let used_by = self
            .job_store
            .get_dependent_jobs(&service_name)
            .await
            .map_err(|e| JobManagerError::InternalError(e.to_string()))?;
        let is_system = registry.is_system_service(service_name);
        Ok(ServiceStatus {
            service,
            used_by_jobs: used_by,
            is_system,
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

    pub async fn load_checkpoint(
        &self,
        job_name: &str,
        checkpoint_cfg: &Option<CheckpointConnectorConfig>,
    ) -> Option<Checkpoint> {
        if !checkpoint_cfg.as_ref().map_or(false, |cfg| cfg.enabled) {
            info!(job_name = %job_name, "checkpointing disabled for job");
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
                    warn!(job_name = %job_name, "no checkpoint found for job");
                    None
                }
                _ => {
                    error!(job_name = %job_name, "failed to load checkpoint for job: {}", err);
                    None
                }
            },
        }
    }

    /// List all checkpoints for a job (history)
    pub async fn list_checkpoints(&self, job_name: &str) -> Vec<Checkpoint> {
        match self
            .service_registry
            .read()
            .await
            .checkpointer()
            .load_all(job_name)
            .await
        {
            Ok(checkpoints) => checkpoints,
            Err(err) => {
                error!(job_name = %job_name, "failed to load checkpoints for job: {}", err);
                vec![]
            }
        }
    }
}

#[derive(Serialize)]
pub struct ServiceStatus {
    #[serde(flatten)]
    pub service: Service,
    pub used_by_jobs: Vec<String>,
    pub is_system: bool,
}

impl Masked for ServiceStatus {
    fn masked(&self) -> Self {
        ServiceStatus {
            service: self.service.masked(),
            used_by_jobs: self.used_by_jobs.clone(),
            is_system: self.is_system,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provision::registry::{
        ServiceRegistry, ServiceStorage, in_memory::InMemoryServiceStorage,
    };
    use tokio::sync::mpsc;

    fn test_job_manager() -> (JobManager, mpsc::UnboundedReceiver<JobStateChange>) {
        let storage: ServiceStorage = Box::new(InMemoryServiceStorage::new());
        let registry = ServiceRegistry::new(storage, None);
        let (exit_tx, exit_rx) = mpsc::unbounded_channel();
        let jm = JobManager::new(
            Arc::new(RwLock::new(registry)),
            Box::new(in_memory::InMemoryJobStore::new()),
            exit_tx,
        );
        (jm, exit_rx)
    }

    fn make_job(name: &str, state: JobState) -> JobMetadata {
        JobMetadata {
            name: name.to_string(),
            started_at: chrono::Utc::now(),
            stopped_at: None,
            state,
            desired_state: JobState::Running,
            service_deps: vec![],
            pipeline: None,
        }
    }

    #[tokio::test]
    async fn job_state_counts_empty() {
        let (jm, _rx) = test_job_manager();
        let counts = jm.job_state_counts().await.unwrap();
        assert_eq!(counts.running, 0);
        assert_eq!(counts.stopped, 0);
        assert_eq!(counts.failed, 0);
    }

    #[tokio::test]
    async fn job_state_counts_mixed_states() {
        let (mut jm, _rx) = test_job_manager();
        save_jobs(&mut jm, &["a", "b"], JobState::Running).await;
        save_jobs(&mut jm, &["c"], JobState::Stopped).await;
        save_jobs(&mut jm, &["d", "e"], JobState::Failed).await;

        assert_state_counts(&jm, 2, 1, 2).await;
    }

    async fn assert_state_counts(jm: &JobManager, running: usize, stopped: usize, failed: usize) {
        let counts = jm.job_state_counts().await.unwrap();
        assert_eq!(counts.running, running, "running mismatch");
        assert_eq!(counts.stopped, stopped, "stopped mismatch");
        assert_eq!(counts.failed, failed, "failed mismatch");
    }

    async fn save_jobs(jm: &mut JobManager, names: &[&str], state: JobState) {
        for name in names {
            jm.job_store
                .save(make_job(name, state.clone()))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn job_state_counts_single_state() {
        let (mut jm, _rx) = test_job_manager();
        save_jobs(&mut jm, &["x", "y"], JobState::Running).await;
        assert_state_counts(&jm, 2, 0, 0).await;

        let (mut jm, _rx) = test_job_manager();
        save_jobs(&mut jm, &["f1", "f2"], JobState::Failed).await;
        assert_state_counts(&jm, 0, 0, 2).await;
    }

    #[test]
    fn metrics_counter_starts_at_zero() {
        let m = JobMetricsCounter::new();
        assert_eq!(m.events_processed.load(Ordering::Relaxed), 0);
        assert_eq!(m.bytes_processed.load(Ordering::Relaxed), 0);
        assert_eq!(m.errors.load(Ordering::Relaxed), 0);
        assert_eq!(m.last_processed_at.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn metrics_counter_record_success() {
        let m = JobMetricsCounter::new();
        m.record_success(256);
        m.record_success(512);

        assert_eq!(m.events_processed.load(Ordering::Relaxed), 2);
        assert_eq!(m.bytes_processed.load(Ordering::Relaxed), 768);
        assert_eq!(m.errors.load(Ordering::Relaxed), 0);
        assert!(m.last_processed_at.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn metrics_counter_record_error() {
        let m = JobMetricsCounter::new();
        m.record_error();
        m.record_error();
        m.record_error();

        assert_eq!(m.errors.load(Ordering::Relaxed), 3);
        assert_eq!(m.events_processed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn metrics_counter_mixed_success_and_errors() {
        let m = JobMetricsCounter::new();
        m.record_success(100);
        m.record_error();
        m.record_success(200);
        m.record_error();

        assert_eq!(m.events_processed.load(Ordering::Relaxed), 2);
        assert_eq!(m.bytes_processed.load(Ordering::Relaxed), 300);
        assert_eq!(m.errors.load(Ordering::Relaxed), 2);
    }
}
