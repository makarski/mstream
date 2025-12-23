use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::bail;
use serde::Serialize;
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    config::{Connector, Service},
    provision::{pipeline::builder::PipelineBuilder, registry::ServiceRegistry},
};

pub struct JobManager {
    services_registry: Arc<ServiceRegistry>,
    cancel_all: CancellationToken,
    running_jobs: HashMap<String, JobContainer>,
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

    // todo: consider changing to a struct with service type
    service_deps: Vec<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Running,
    Stopped,
}

impl JobManager {
    pub fn new(service_registry: ServiceRegistry, exit_tx: UnboundedSender<String>) -> JobManager {
        Self {
            services_registry: Arc::new(service_registry),
            cancel_all: CancellationToken::new(),
            running_jobs: HashMap::new(),
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
    ) -> JobMetadata {
        self.add_deps(&name, &service_deps);

        let metadata = JobMetadata {
            name: name.clone(),
            started_at: chrono::Utc::now(),
            stopped_at: None,
            state: JobState::Running,
            service_deps,
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

            Ok(metadata)
        } else {
            bail!("job not found: {}", job_name);
        }
    }

    pub fn list_jobs(&self) -> Vec<JobMetadata> {
        self.running_jobs
            .values()
            .map(|jc| jc.metadata.clone())
            .collect()
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
        ))
    }

    pub async fn list_services(&self) -> Vec<ServiceStatus> {
        let all_conf_services = self.services_registry.all_service_definitions().await;

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

    fn add_deps(&mut self, job_name: &str, services: &[String]) {
        for service_name in services {
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
