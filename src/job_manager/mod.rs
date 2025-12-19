use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, bail};
use serde::Serialize;
use tokio::{
    select,
    sync::mpsc::{Sender, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    cmd::event_handler::EventHandler,
    config::Connector,
    provision::{pipeline::PipelineBuilder, registry::ServiceRegistry},
    source::{EventSource, SourceEvent, SourceProvider},
};

pub struct JobManager {
    services_registry: Arc<ServiceRegistry>,
    cancel_all: CancellationToken,
    running_jobs: HashMap<String, JobContainer>,
}

struct JobContainer {
    join_handle: JoinHandle<()>,
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
    pub fn new(service_registry: ServiceRegistry) -> JobManager {
        Self {
            services_registry: Arc::new(service_registry),
            cancel_all: CancellationToken::new(),
            running_jobs: HashMap::new(),
        }
    }

    fn register_job(
        &mut self,
        id: String,
        join_handle: JoinHandle<()>,
        cancel_token: CancellationToken,
    ) -> JobMetadata {
        let metadata = JobMetadata {
            id: id.clone(),
            started_at: chrono::Utc::now(),
            stopped_at: None,
            state: JobState::Running,
        };

        let job_container = JobContainer {
            join_handle,
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
            jc.join_handle.await?;
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

    pub async fn start_job(
        &mut self,
        conn_cfg: Connector,
        done_ch: UnboundedSender<String>,
    ) -> anyhow::Result<JobMetadata> {
        if self.running_jobs.contains_key(&conn_cfg.name) {
            bail!("job already running: {}", conn_cfg.name);
        }

        let pipeline_builder =
            PipelineBuilder::new(self.services_registry.clone(), conn_cfg.clone());
        let pipeline = pipeline_builder.build().await?;

        let source_def = pipeline.source.ok_or_else(|| {
            anyhow!(
                "no source provider initialized for connector: {}",
                conn_cfg.name
            )
        })?;

        // channel buffer size is the same as the batch size
        // if the buffer is full, the source listener will block
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(pipeline.batch_size);
        if let Err(err) =
            spawn_source_listener(conn_cfg.name.clone(), source_def.source_provider, events_tx)
                .await
        {
            bail!("failed to spawn source listener: {}", err);
        }

        let job_name = conn_cfg.name.clone();
        let job_cancel = self.cancel_all.child_token();
        let task_cancel = job_cancel.clone();

        let join_handle = tokio::spawn(async move {
            let cnt_name = conn_cfg.name.clone();

            // todo: consider reusing pipeline entity instead of event handler
            let mut event_handler = EventHandler {
                connector_name: conn_cfg.name.clone(),
                source_schema: pipeline.source_schema,
                source_output_encoding: conn_cfg.source.output_encoding,
                sinks: pipeline.sinks,
                middlewares: pipeline.middlewares,
            };

            let work = async {
                if pipeline.is_batching_enabled {
                    event_handler
                        .listen_batch(events_rx, pipeline.batch_size)
                        .await
                } else {
                    event_handler.listen(events_rx).await
                }
            };

            select! {
                _ = task_cancel.cancelled() => {
                    info!("cancelling job: {}", cnt_name);
                }
                res = work => {
                    if let Err(err) = res {
                        error!("job {} failed: {}", cnt_name, err);
                    }
                }
            }

            // send done signal
            if let Err(err) = done_ch.send(cnt_name.clone()) {
                error!(
                    "failed to send done signal: {}: connector: {}",
                    err, cnt_name
                );
            }
        });

        Ok(self.register_job(job_name, join_handle, job_cancel))
    }
}

async fn spawn_source_listener(
    cnt_name: String,
    mut source_provider: SourceProvider,
    events_tx: Sender<SourceEvent>,
) -> anyhow::Result<()> {
    tokio::spawn(async move {
        info!("spawning a listener for connector: {}", cnt_name);
        if let Err(err) = source_provider.listen(events_tx).await {
            error!("source listener failed. connector: {}:{}", cnt_name, err)
        }
    });
    Ok(())
}
