use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::Arc,
};

use anyhow::bail;
use serde::Serialize;
use tokio::{
    sync::{RwLock, mpsc::UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    config::{Connector, Service},
    provision::{pipeline::builder::PipelineBuilder, registry::ServiceRegistry},
};

pub struct JobManager {
    services_registry: Arc<RwLock<ServiceRegistry>>,
    cancel_all: CancellationToken,
    job_store: in_memory::InMemoryJobStore,
    running_jobs: HashMap<String, JobContainer>,
    exit_tx: UnboundedSender<(String, JobState)>,
}

struct JobContainer {
    work_handle: JoinHandle<()>,
    source_handle: JoinHandle<()>,
    cancel_token: CancellationToken,
}

#[derive(Clone, Serialize)]
pub struct JobMetadata {
    pub name: String,
    started_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    stopped_at: Option<chrono::DateTime<chrono::Utc>>,
    state: JobState,
    #[serde(rename = "linked_services")]
    service_deps: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pipeline: Option<Connector>,
}

#[derive(Clone, Serialize)]
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
        service_registry: ServiceRegistry,
        job_store: in_memory::InMemoryJobStore,
        exit_tx: UnboundedSender<(String, JobState)>,
    ) -> JobManager {
        Self {
            services_registry: Arc::new(RwLock::new(service_registry)),
            cancel_all: CancellationToken::new(),
            running_jobs: HashMap::new(),
            job_store,
            exit_tx,
        }
    }

    fn register_job(
        &mut self,
        name: String,
        work_handle: JoinHandle<()>,
        source_handle: JoinHandle<()>,
        cancel_token: CancellationToken,
        service_deps: Vec<String>,
        pipeline: Option<Connector>,
    ) -> JobMetadata {
        let metadata = JobMetadata {
            name: name.clone(),
            started_at: chrono::Utc::now(),
            stopped_at: None,
            state: JobState::Running,
            service_deps,
            pipeline,
        };

        let job_container = JobContainer {
            work_handle,
            source_handle,
            cancel_token,
        };

        self.job_store.save(metadata.clone());
        self.running_jobs.insert(name, job_container);
        metadata
    }

    pub async fn handle_job_exit(&mut self, job_name: &str, new_state: JobState) {
        if let Some(jc) = self.running_jobs.remove(job_name) {
            warn!("handling exited job: {}", job_name);
            jc.cancel_token.cancel();
        }

        if let Some(mut metadata) = self.job_store.get(job_name).cloned() {
            metadata.state = new_state;
            metadata.stopped_at = Some(chrono::Utc::now());
            metadata.service_deps.clear();
            self.job_store.save(metadata);
        } else {
            warn!("failed to find metadata for exited job: {}", job_name);
        }
    }

    pub async fn stop_job(&mut self, job_name: &str) -> anyhow::Result<()> {
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

    pub fn list_jobs(&self) -> Vec<JobMetadata> {
        self.job_store.list_all()
    }

    pub async fn start_job(&mut self, conn_cfg: Connector) -> anyhow::Result<JobMetadata> {
        if self.running_jobs.contains_key(&conn_cfg.name) {
            bail!("job already running: {}", conn_cfg.name);
        }

        let pipeline_builder =
            PipelineBuilder::new(self.services_registry.clone(), conn_cfg.clone());

        let service_deps = pipeline_builder.service_deps();
        let pipeline = pipeline_builder.build().await?;

        let job_cancel = self.cancel_all.child_token();
        let task_cancel = job_cancel.clone();

        let (job_name, work_handle, source_handle) =
            pipeline.run(task_cancel, self.exit_tx.clone()).await?;

        Ok(self.register_job(
            job_name,
            work_handle,
            source_handle,
            job_cancel,
            service_deps,
            Some(conn_cfg),
        ))
    }

    pub async fn list_services(&self) -> Vec<ServiceStatus> {
        let all_conf_services = self
            .services_registry
            .read()
            .await
            .all_service_definitions()
            .await;

        all_conf_services
            .into_iter()
            .map(|service| {
                let name = service.name();
                let used_by = self.job_store.get_dependent_jobs(&name);

                ServiceStatus {
                    service,
                    used_by_jobs: used_by,
                }
            })
            .collect()
    }

    pub async fn create_service(&self, service_cfg: Service) -> anyhow::Result<()> {
        let service_name = service_cfg.name().to_string();
        self.services_registry
            .write()
            .await
            .add_service(service_cfg)
            .await?;

        info!("service '{}' created", service_name);
        Ok(())
    }

    pub async fn remove_service(&self, service_name: &str) -> anyhow::Result<()> {
        // acquire a write lock before checking for consumers to avoid race conditions
        let mut registry = self.services_registry.write().await;

        let jobs = self.job_store.get_dependent_jobs(service_name);
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
}

#[derive(Serialize)]
pub struct ServiceStatus {
    #[serde(flatten)]
    pub service: Service,
    pub used_by_jobs: Vec<String>,
}

pub mod in_memory {
    use std::collections::{HashMap, HashSet};

    use crate::job_manager::JobMetadata;

    pub struct InMemoryJobStore {
        jobs: HashMap<String, JobMetadata>,
        jobs_by_service: HashMap<String, HashSet<String>>,
    }

    impl InMemoryJobStore {
        pub fn new() -> Self {
            Self {
                jobs: HashMap::new(),
                jobs_by_service: HashMap::new(),
            }
        }

        pub fn list_all(&self) -> Vec<JobMetadata> {
            self.jobs.values().cloned().collect()
        }

        pub fn save(&mut self, metadata: JobMetadata) {
            if let Some(existing) = self.jobs.get(&metadata.name) {
                if existing.service_deps != metadata.service_deps {
                    self.clear_deps(&metadata.name);
                }
            }

            if !metadata.service_deps.is_empty() {
                self.add_deps(&metadata.name, &metadata.service_deps);
            }

            let name = metadata.name.clone();
            self.jobs.insert(name, metadata);
        }

        pub fn get(&self, name: &str) -> Option<&JobMetadata> {
            self.jobs.get(name)
        }

        pub fn get_dependent_jobs(&self, service_name: &str) -> Vec<String> {
            if let Some(job_names) = self.jobs_by_service.get(service_name) {
                job_names.iter().cloned().collect()
            } else {
                Vec::new()
            }
        }

        fn add_deps(&mut self, job_name: &str, deps: &[String]) {
            for service_name in deps {
                self.jobs_by_service
                    .entry(service_name.clone())
                    .or_insert_with(HashSet::new)
                    .insert(job_name.to_string());
            }
        }

        fn clear_deps(&mut self, job_name: &str) {
            let job_deps = self
                .jobs
                .get(job_name)
                .map(|metadata| metadata.service_deps.clone())
                .unwrap_or_default();

            for service_name in job_deps {
                if let Some(jobs) = self.jobs_by_service.get_mut(&service_name) {
                    jobs.remove(job_name);

                    if jobs.is_empty() {
                        self.jobs_by_service.remove(&service_name);
                    }
                }
            }
        }
    }
}
