use anyhow::anyhow;
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    cmd::event_handler::EventHandler,
    config::Encoding,
    provision::pipeline::{
        middleware::MiddlewareDefinition, schema::SchemaDefinition, sink::SinkDefinition,
        source::SourceDefinition,
    },
    schema::Schema,
    source::{EventSource, SourceEvent, SourceProvider},
};

pub mod builder;
pub mod middleware;
pub mod schema;
pub mod sink;
pub mod source;

pub struct Pipeline {
    pub name: String,
    pub source: Option<SourceDefinition>,
    pub source_schema: Schema,
    pub middlewares: Vec<MiddlewareDefinition>,
    pub schemas: Vec<SchemaDefinition>,
    pub sinks: Vec<SinkDefinition>,
    pub batch_size: usize,
    pub is_batching_enabled: bool,
}

impl Pipeline {
    pub async fn run(
        mut self,
        cancel_token: CancellationToken,
        exit_tx: UnboundedSender<String>,
    ) -> anyhow::Result<(String, JoinHandle<()>, JoinHandle<()>)> {
        let source_def = self.source.take().ok_or_else(|| {
            anyhow!(
                "no source provider initialized for connector: {}",
                self.name
            )
        })?;

        let job_name = self.name.clone();
        let runtime = PipelineRuntime::new(self.batch_size, cancel_token.clone());

        let source_handle = runtime.listen_source(self.name.clone(), source_def.source_provider);
        let work_handle = runtime.do_work(self, exit_tx, source_def.out_encoding);

        Ok((job_name, work_handle, source_handle))
    }
}

struct PipelineRuntime {
    events_tx: Sender<SourceEvent>,
    events_rx: Receiver<SourceEvent>,
    cancel_token: CancellationToken,
}

impl PipelineRuntime {
    pub fn new(buffer_size: usize, cancel_token: CancellationToken) -> Self {
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(buffer_size);
        Self {
            events_tx,
            events_rx,
            cancel_token,
        }
    }

    fn listen_source(&self, name: String, mut source_provider: SourceProvider) -> JoinHandle<()> {
        let events_tx = self.events_tx.clone();
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            info!("spawning a listener for connector: {}", name);

            select! {
                _ = cancel_token.cancelled() => {
                    warn!("cancelling source listener for connector: {}", name);
                }
                res = source_provider.listen(events_tx) => {
                    if let Err(err) = res {
                        error!("source listener failed. connector: {}: {}", name, err)
                    }
                }
            }
        })
    }

    fn do_work(
        self,
        pipeline: Pipeline,
        exit_tx: UnboundedSender<String>,
        out_encoding: Encoding,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let cnt_name = pipeline.name;

            // todo: consider reusing pipeline entity instead of event handler
            let mut event_handler = EventHandler {
                connector_name: cnt_name.clone(),
                source_schema: pipeline.source_schema,
                source_output_encoding: out_encoding,
                sinks: pipeline.sinks,
                middlewares: pipeline.middlewares,
            };

            let work = async {
                if pipeline.is_batching_enabled {
                    event_handler
                        .listen_batch(self.events_rx, pipeline.batch_size)
                        .await
                } else {
                    event_handler.listen(self.events_rx).await
                }
            };

            select! {
                _ = self.cancel_token.cancelled() => {
                    info!("cancelling job: {}", cnt_name);
                }
                res = work => {
                    if let Err(err) = res {
                        error!("job {} failed: {}", cnt_name, err);
                    }
                }
            }

            // send done signal
            if let Err(err) = exit_tx.send(cnt_name.clone()) {
                error!(
                    "failed to send done signal: {}: connector: {}",
                    err, cnt_name
                );
            }
        })
    }
}

pub fn find_schema(schema_id: Option<String>, schema_definitions: &[SchemaDefinition]) -> Schema {
    schema_id
        .as_ref()
        .and_then(|id| {
            schema_definitions
                .iter()
                .find(|&schema| &schema.schema_id == id)
                .map(|s| s.schema.clone())
        })
        .unwrap_or_else(|| Schema::Undefined)
}
