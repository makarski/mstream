use anyhow::{anyhow, bail};
use tokio::{
    select,
    sync::mpsc::{Sender, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    cmd::event_handler::EventHandler,
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

#[derive(Default)]
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
        self,
        cancel_token: CancellationToken,
        exit_tx: UnboundedSender<String>,
    ) -> anyhow::Result<(String, JoinHandle<()>)> {
        let source_def = self.source.ok_or_else(|| {
            anyhow!(
                "no source provider initialized for connector: {}",
                self.name
            )
        })?;

        // channel buffer size is the same as the batch size
        // if the buffer is full, the source listener will block
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(self.batch_size);
        if let Err(err) =
            listen_source(self.name.clone(), source_def.source_provider, events_tx).await
        {
            bail!("failed to spawn source listener: {}", err);
        }

        let job_name = self.name.clone();

        let join_handle = tokio::spawn(async move {
            let cnt_name = self.name.clone();

            // todo: consider reusing pipeline entity instead of event handler
            let mut event_handler = EventHandler {
                connector_name: self.name.clone(),
                source_schema: self.source_schema,
                source_output_encoding: source_def.out_encoding,
                sinks: self.sinks,
                middlewares: self.middlewares,
            };

            let work = async {
                if self.is_batching_enabled {
                    event_handler.listen_batch(events_rx, self.batch_size).await
                } else {
                    event_handler.listen(events_rx).await
                }
            };

            select! {
                _ = cancel_token.cancelled() => {
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
        });
        Ok((job_name, join_handle))
    }
}

async fn listen_source(
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

pub fn find_schema(schema_id: Option<String>, schema_definitions: &[SchemaDefinition]) -> Schema {
    let has_schemas = !schema_definitions.is_empty();
    if !has_schemas || schema_id.is_none() {
        return Schema::Undefined;
    }
    let schema_id = schema_id.unwrap();

    schema_definitions
        .into_iter()
        .find(|&schema| schema.schema_id == schema_id)
        .map(|s| s.schema.clone())
        .unwrap_or_else(|| Schema::Undefined)
}
