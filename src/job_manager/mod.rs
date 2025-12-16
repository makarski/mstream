use std::collections::HashMap;

use anyhow::bail;
use serde::Serialize;
use tokio::{
    select,
    sync::mpsc::{Sender, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    cmd::{event_handler::EventHandler, services::ServiceFactory},
    config::{BatchConfig, Connector, SchemaServiceConfigReference, SourceServiceConfigReference},
    schema::{Schema, SchemaRegistry},
    source::{EventSource, SourceEvent},
};

pub struct JobManager {
    service_factory: ServiceFactory,
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
    pub fn new(sf: ServiceFactory) -> JobManager {
        Self {
            service_factory: sf,
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

        let schemas = init_schemas(conn_cfg.schemas.as_ref(), &self.service_factory).await?;
        let source_schema = find_schema(conn_cfg.source.schema_id.clone(), schemas.as_ref());

        let mut publishers = Vec::new();
        for topic_cfg in conn_cfg.sinks.into_iter() {
            let publisher = self.service_factory.publisher_service(&topic_cfg).await?;
            let schema = find_schema(topic_cfg.schema_id.clone(), schemas.as_ref());
            publishers.push((topic_cfg, publisher, schema));
        }

        let middlewares = match conn_cfg.middlewares {
            Some(middlewares) => {
                let mut result = Vec::new();
                for middleware_cfg in middlewares.into_iter() {
                    let middleware = self
                        .service_factory
                        .middleware_service(&middleware_cfg)
                        .await?;

                    let schema = find_schema(middleware_cfg.schema_id.clone(), schemas.as_ref());
                    result.push((middleware_cfg, middleware, schema));
                }
                Some(result)
            }
            None => None,
        };

        let (channel_size, is_batch) = if let Some(batch_cfg) = conn_cfg.batch {
            match batch_cfg {
                BatchConfig::Count { size } => (size, true),
            }
        } else {
            (1, false)
        };

        // channel buffer size is the same as the batch size
        // if the buffer is full, the source listener will block
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(channel_size);
        if let Err(err) = spawn_source_listener(
            conn_cfg.name.clone(),
            &self.service_factory,
            &conn_cfg.source,
            events_tx,
        )
        .await
        {
            bail!("failed to spawn source listener: {}", err);
        }

        let job_name = conn_cfg.name.clone();
        let job_cancel = self.cancel_all.child_token();
        let task_cancel = job_cancel.clone();

        let join_handle = tokio::spawn(async move {
            let cnt_name = conn_cfg.name.clone();
            let mut event_handler = EventHandler {
                connector_name: conn_cfg.name.clone(),
                source_schema,
                source_output_encoding: conn_cfg.source.output_encoding,
                publishers,
                middlewares,
            };

            let work = async {
                if is_batch {
                    event_handler.listen_batch(events_rx, channel_size).await
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
    service_container: &ServiceFactory,
    source_cfg: &SourceServiceConfigReference,
    events_tx: Sender<SourceEvent>,
) -> anyhow::Result<()> {
    let mut source_provider = service_container.source_provider(source_cfg).await?;

    tokio::spawn(async move {
        info!("spawning a listener for connector: {}", cnt_name);
        if let Err(err) = source_provider.listen(events_tx).await {
            error!("source listener failed. connector: {}:{}", cnt_name, err)
        }
    });
    Ok(())
}

struct SchemaDefinition {
    schema_id: String,
    schema: Schema,
}

async fn init_schemas(
    schemas_config: Option<&Vec<SchemaServiceConfigReference>>,
    service_container: &ServiceFactory,
) -> anyhow::Result<Option<Vec<SchemaDefinition>>> {
    match schemas_config {
        Some(cfg) => {
            let mut result = Vec::with_capacity(cfg.len());
            for schema_cfg in cfg.into_iter() {
                let mut schema_service = service_container.schema_provider(&schema_cfg).await?;

                // wait for the schema to be ready
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                let schema = schema_service
                    .get_schema(schema_cfg.resource.clone())
                    .await?;

                result.push(SchemaDefinition {
                    schema_id: schema_cfg.id.clone(),
                    schema,
                });
            }
            Ok(Some(result))
        }
        None => Ok(None),
    }
}

fn find_schema(
    schema_id: Option<String>,
    schema_definitions: Option<&Vec<SchemaDefinition>>,
) -> Schema {
    match (schema_id, schema_definitions) {
        (Some(id), Some(schemas)) => {
            if let Some(schema) = schemas.into_iter().find(|schema| schema.schema_id == id) {
                schema.schema.clone()
            } else {
                Schema::Undefined
            }
        }
        _ => Schema::Undefined,
    }
}
