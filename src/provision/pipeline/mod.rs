use anyhow::anyhow;
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    config::Encoding,
    job_manager::JobState,
    provision::pipeline::{
        middleware::MiddlewareDefinition, processor::EventHandler, schema::SchemaDefinition,
        sink::SinkDefinition,
    },
    schema::Schema,
    source::{EventSource, SourceEvent, SourceProvider},
};

pub mod builder;
pub mod middleware;
mod processor;
pub mod schema;
pub mod sink;
pub mod source;

pub struct Pipeline {
    pub name: String,
    pub source_provider: Option<SourceProvider>,
    pub source_out_encoding: Encoding,
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
        exit_tx: UnboundedSender<(String, JobState)>,
    ) -> anyhow::Result<(String, JoinHandle<()>, JoinHandle<()>)> {
        let source_provider = self
            .source_provider
            .take()
            .ok_or_else(|| anyhow!("source provider is not defined for pipeline: {}", self.name))?;

        let job_name = self.name.clone();
        let runtime = PipelineRuntime::new(self.batch_size, cancel_token.clone());

        let source_handle = runtime.listen_source(self.name.clone(), source_provider);
        let work_handle = runtime.do_work(self, exit_tx);

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
        exit_tx: UnboundedSender<(String, JobState)>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let cnt_name = pipeline.name.clone();
            let mut eh = EventHandler::new(pipeline);

            let job_state = select! {
                _ = self.cancel_token.cancelled() => {
                    info!("cancelling job: {}", cnt_name);
                    JobState::Stopped
                }
                res = eh.handle(self.events_rx) => {
                    if let Err(err) = res {
                        error!("job {} failed: {}", cnt_name, err);
                    }
                    JobState::Failed
                }
            };

            // send done signal
            if let Err(err) = exit_tx.send((cnt_name.clone(), job_state)) {
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
