use std::{
    collections::{HashMap, HashSet},
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
    running_jobs: HashMap<String, JobContainer>,
    stopped_jobs: Vec<JobMetadata>,
    service_consumers: HashMap<String, HashSet<String>>,
    exit_tx: UnboundedSender<String>,
}

struct JobContainer {
    work_handle: JoinHandle<()>,
    source_handle: JoinHandle<()>,
    cancel_token: CancellationToken,
    metadata: JobMetadata,
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

impl JobManager {
    pub fn new(service_registry: ServiceRegistry, exit_tx: UnboundedSender<String>) -> JobManager {
        Self {
            services_registry: Arc::new(RwLock::new(service_registry)),
            cancel_all: CancellationToken::new(),
            running_jobs: HashMap::new(),
            stopped_jobs: Vec::new(),
            service_consumers: HashMap::new(),
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
        self.add_deps(&name, &service_deps);

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
            metadata: metadata.clone(),
        };

        self.running_jobs.insert(name, job_container);
        metadata
    }

    pub async fn handle_job_exit(&mut self, job_name: &str, new_state: JobState) {
        if let Some(jc) = self.running_jobs.remove(job_name) {
            warn!("handling failed job: {}", job_name);
            jc.cancel_token.cancel();

            let mut metadata = jc.metadata;
            metadata.state = new_state;
            metadata.stopped_at = Some(chrono::Utc::now());

            self.remove_deps(job_name, &metadata.service_deps);
            self.stopped_jobs.push(metadata.clone());
            warn!("job marked as stopped: {}", job_name);
        }
    }

    pub async fn stop_job(&mut self, job_name: &str) -> anyhow::Result<JobMetadata> {
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

            let mut metadata = jc.metadata;
            // todo: persist this to a storage
            metadata.state = JobState::Stopped;
            metadata.stopped_at = Some(chrono::Utc::now());
            self.remove_deps(job_name, &metadata.service_deps);

            self.stopped_jobs.push(metadata.clone());

            Ok(metadata)
        } else {
            bail!("job not found: {}", job_name);
        }
    }

    pub fn list_jobs(&self) -> Vec<JobMetadata> {
        let mut jobs: Vec<JobMetadata> = self
            .running_jobs
            .values()
            .map(|jc| jc.metadata.clone())
            .collect();

        jobs.extend(self.stopped_jobs.clone());
        jobs
    }

    pub async fn start_job(&mut self, conn_cfg: Connector) -> anyhow::Result<JobMetadata> {
        if self.running_jobs.contains_key(&conn_cfg.name) {
            bail!("job already running: {}", conn_cfg.name);
        }

        self.stopped_jobs.retain(|job| job.name != conn_cfg.name);

        let pipeline_builder =
            PipelineBuilder::new(self.services_registry.clone(), conn_cfg.clone());

        let service_deps = pipeline_builder.service_deps();
        let pipeline = pipeline_builder.build().await?;
        let config = pipeline.config.clone();

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
            Some(config),
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
                let used_by = self
                    .service_consumers
                    .get(name)
                    .map(|jobs| jobs.iter().cloned().collect())
                    .unwrap_or_else(Vec::new);

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

        if let Some(jobs) = self.service_consumers.get(service_name) {
            bail!(
                "cannot delete service '{}': it is used by running jobs: {}",
                service_name,
                jobs.iter().cloned().collect::<Vec<_>>().join(", ")
            );
        }
        registry.remove_service(service_name).await?;

        info!("service '{}' removed", service_name);
        Ok(())
    }

    fn add_deps(&mut self, job_name: &str, deps: &[String]) {
        for service_name in deps {
            self.service_consumers
                .entry(service_name.clone())
                .or_insert_with(HashSet::new)
                .insert(job_name.to_string());
        }
    }

    fn remove_deps(&mut self, job_name: &str, deps: &[String]) {
        for service_name in deps {
            if let Some(jobs) = self.service_consumers.get_mut(service_name) {
                jobs.remove(job_name);

                if jobs.is_empty() {
                    self.service_consumers.remove(service_name);
                }
            }
        }
    }
}

#[derive(Serialize)]
pub struct ServiceStatus {
    #[serde(flatten)]
    pub service: Service,
    pub used_by_jobs: Vec<String>,
}
