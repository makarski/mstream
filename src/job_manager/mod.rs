use std::{collections::HashMap, sync::Arc};

use anyhow::bail;
use serde::Serialize;
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    config::Connector,
    provision::{pipeline::builder::PipelineBuilder, registry::ServiceRegistry},
};

pub struct JobManager {
    services_registry: Arc<ServiceRegistry>,
    cancel_all: CancellationToken,
    running_jobs: HashMap<String, JobContainer>,
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
    pub id: String,
    started_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    stopped_at: Option<chrono::DateTime<chrono::Utc>>,
    state: JobState,
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
            exit_tx,
        }
    }

    fn register_job(
        &mut self,
        id: String,
        work_handle: JoinHandle<()>,
        source_handle: JoinHandle<()>,
        cancel_token: CancellationToken,
    ) -> JobMetadata {
        let metadata = JobMetadata {
            id: id.clone(),
            started_at: chrono::Utc::now(),
            stopped_at: None,
            state: JobState::Running,
        };

        let job_container = JobContainer {
            work_handle,
            source_handle,
            cancel_token,
            metadata: metadata.clone(),
        };

        self.running_jobs.insert(id, job_container);
        metadata
    }

    pub async fn stop_job(&mut self, id: &str) -> anyhow::Result<JobMetadata> {
        if let Some(jc) = self.running_jobs.remove(id) {
            info!("stopping job: {}", id);
            jc.cancel_token.cancel();

            // wait for the job to finish
            let (work_res, source_res) = tokio::join!(jc.work_handle, jc.source_handle);
            if let Err(err) = work_res {
                error!("job work task panicked: {}: {}", err, id);
            }

            if let Err(err) = source_res {
                error!("job source task panicked: {}: {}", err, id);
            }

            info!("job stopped: {}", id);

            let mut metadata = jc.metadata;
            // todo: persist this to a storage
            metadata.state = JobState::Stopped;
            metadata.stopped_at = Some(chrono::Utc::now());

            Ok(metadata)
        } else {
            bail!("job not found: {}", id);
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

        let pipeline = pipeline_builder.build().await?;

        let job_cancel = self.cancel_all.child_token();
        let task_cancel = job_cancel.clone();

        let (job_name, work_handle, source_handle) =
            pipeline.run(task_cancel, self.exit_tx.clone()).await?;

        Ok(self.register_job(job_name, work_handle, source_handle, job_cancel))
    }
}
