use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::Arc,
};

use anyhow::{anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{RwLock, mpsc::UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub mod in_memory;
pub mod mongodb_store;

use crate::{
    config::{Connector, Service},
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

    pub async fn stop_job(&mut self, job_name: &str) -> anyhow::Result<()> {
        self.set_desired_job_state(job_name, JobState::Stopped)
            .await?;

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

    pub async fn restart_job(&mut self, job_name: &str) -> anyhow::Result<JobMetadata> {
        let job_metadata = self
            .job_store
            .get(job_name)
            .await?
            .ok_or_else(|| anyhow!("job not found: {}", job_name))?;

        self.stop_job(job_name).await?;

        if let Some(conn_cfg) = job_metadata.pipeline.clone() {
            self.start_job(conn_cfg).await
        } else {
            bail!(
                "cannot restart job '{}': missing pipeline configuration",
                job_name
            );
        }
    }

    pub async fn start_job(&mut self, conn_cfg: Connector) -> anyhow::Result<JobMetadata> {
        if self.running_jobs.contains_key(&conn_cfg.name) {
            bail!("job already running: {}", conn_cfg.name);
        }

        let exit_tx = self
            .exit_tx
            .as_ref()
            .ok_or_else(|| anyhow!("job manager is shutting down"))?
            .clone();

        let pipeline_builder =
            PipelineBuilder::new(self.service_registry.clone(), conn_cfg.clone());

        let service_deps = pipeline_builder.service_deps();
        let pipeline = pipeline_builder.build().await?;

        let job_cancel = self.cancel_all.child_token();
        let task_cancel = job_cancel.clone();

        let (job_name, work_handle, source_handle) = pipeline.run(task_cancel, exit_tx).await?;

        self.register_job(
            job_name,
            work_handle,
            source_handle,
            job_cancel,
            service_deps,
            Some(conn_cfg),
        )
        .await
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

    pub async fn create_service(&self, service_cfg: Service) -> anyhow::Result<()> {
        let service_name = service_cfg.name().to_string();
        self.service_registry
            .write()
            .await
            .register_service(service_cfg)
            .await?;

        info!("service '{}' created", service_name);
        Ok(())
    }

    pub async fn remove_service(&self, service_name: &str) -> anyhow::Result<()> {
        // acquire a write lock before checking for consumers to avoid race conditions
        let mut registry = self.service_registry.write().await;

        let jobs = self.job_store.get_dependent_jobs(service_name).await?;
        if !jobs.is_empty() {
            bail!(
                "cannot remove service '{}': it is used by running jobs: {}",
                service_name,
                jobs.join(", ")
            );
        }
        registry.remove_service(service_name).await?;

        info!("service '{}' removed", service_name);
        Ok(())
    }

    pub async fn get_service(&self, service_name: &str) -> anyhow::Result<ServiceStatus> {
        let registry = self.service_registry.read().await;
        match registry.service_definition(service_name).await {
            Ok(service) => {
                let used_by = self.job_store.get_dependent_jobs(service_name).await?;
                return Ok(ServiceStatus {
                    service,
                    used_by_jobs: used_by,
                });
            }
            Err(err) => {
                bail!("failed to get service '{}': {}", service_name, err)
            }
        }
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
}

#[derive(Serialize)]
pub struct ServiceStatus {
    #[serde(flatten)]
    pub service: Service,
    pub used_by_jobs: Vec<String>,
}

#[async_trait::async_trait]
pub trait JobLifecycleStorage {
    async fn list_all(&self) -> anyhow::Result<Vec<JobMetadata>>;
    async fn save(&mut self, metadata: JobMetadata) -> anyhow::Result<()>;
    async fn get(&self, name: &str) -> anyhow::Result<Option<JobMetadata>>;
    async fn get_dependent_jobs(&self, service_name: &str) -> anyhow::Result<Vec<String>>;
}
