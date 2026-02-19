use std::sync::Arc;

use anyhow::anyhow;
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, info_span, warn};

use crate::{
    checkpoint::DynCheckpointer,
    config::Encoding,
    job_manager::{JobMetricsCounter, JobState, JobStateChange},
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
    pub checkpointer: DynCheckpointer,
    pub with_checkpoints: bool,
    pub metrics: Option<Arc<JobMetricsCounter>>,
}

impl Pipeline {
    pub async fn run(
        mut self,
        cancel_token: CancellationToken,
        exit_tx: UnboundedSender<JobStateChange>,
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

        let span = info_span!("source_listener", job_name = %name);
        tokio::spawn(
            async move {
                info!("spawning source listener");

                select! {
                    _ = cancel_token.cancelled() => {
                        warn!("cancelling source listener");
                    }
                    res = source_provider.listen(events_tx) => {
                        if let Err(err) = res {
                            error!("source listener failed: {}", err)
                        }
                    }
                }
            }
            .instrument(span),
        )
    }

    fn do_work(
        self,
        pipeline: Pipeline,
        exit_tx: UnboundedSender<JobStateChange>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let cnt_name = pipeline.name.clone();
            let metrics = pipeline.metrics.clone();
            let mut eh = EventHandler::new(pipeline, metrics);

            let job_state = select! {
                _ = self.cancel_token.cancelled() => {
                    info!(job_name = %cnt_name, "cancelling job");
                    JobState::Stopped
                }
                res = eh.handle(self.events_rx) => {
                    if let Err(err) = res {
                        error!(job_name = %cnt_name, "job failed: {}", err);
                    }
                    JobState::Failed
                }
            };

            let state_change = JobStateChange {
                job_name: cnt_name.clone(),
                new_state: job_state,
            };

            // send done signal
            if let Err(err) = exit_tx.send(state_change) {
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
