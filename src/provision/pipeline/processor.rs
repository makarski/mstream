use core::iter::Iterator;
use std::sync::Arc;

use anyhow::{anyhow, bail};
use chrono::Utc;
use tokio::{sync::mpsc::Receiver, task::block_in_place};
use tracing::{debug, error, info, warn};

use crate::{
    checkpoint::Checkpoint,
    job_manager::JobMetricsCounter,
    provision::pipeline::Pipeline,
    schema::encoding::SchemaEncoder,
    sink::{EventSink, encoding::SinkEvent},
    source::SourceEvent,
};

pub(super) struct EventHandler {
    pipeline: Pipeline,
    metrics: Option<Arc<JobMetricsCounter>>,
}

impl EventHandler {
    pub fn new(pipeline: Pipeline, metrics: Option<Arc<JobMetricsCounter>>) -> Self {
        Self { pipeline, metrics }
    }

    /// Entry point for the pipeline's event processing logic.
    ///
    /// Determines the appropriate processing strategy (streaming or batching) based on
    /// the pipeline configuration and begins consuming the event stream.
    pub async fn handle(&mut self, events_rx: Receiver<SourceEvent>) -> anyhow::Result<()> {
        if self.pipeline.is_batching_enabled {
            let batch_size = self.pipeline.batch_size;
            info!(
                job_name = %self.pipeline.name,
                batch_size = batch_size,
                "starting batch EventHandler listener"
            );
            self.run_batch_loop(events_rx, batch_size).await
        } else {
            info!(
                job_name = %self.pipeline.name,
                "starting EventHandler listener"
            );
            self.run_event_loop(events_rx).await
        }
    }

    /// Runs the main processing loop for individual events.
    ///
    /// Consumes the stream until exhaustion, applying schema validation and
    /// routing each event through the configured middleware and sinks.
    async fn run_event_loop(&mut self, mut events_rx: Receiver<SourceEvent>) -> anyhow::Result<()> {
        loop {
            match events_rx.recv().await {
                Some(event) => {
                    debug!("processing event: {:?}", event);

                    let event = block_in_place(|| {
                        event
                            .apply_schema(
                                Some(&self.pipeline.source_out_encoding),
                                &self.pipeline.source_schema,
                            )
                            .map_err(|err| anyhow!("failed to apply source schema: {}", err))
                    })?;

                    if let Err(err) = self.process_event(event).await {
                        error!(job_name = %self.pipeline.name, "failed to process event: {}", err)
                    }
                }
                None => {
                    info!(job_name = %self.pipeline.name, "source listener exited");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Runs the main processing loop for batched events.
    ///
    /// Aggregates incoming events into batches of the configured size before
    /// processing them as a single unit. This improves throughput by reducing
    /// overhead per event.
    async fn run_batch_loop(
        &mut self,
        mut events_rx: Receiver<SourceEvent>,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        // pre-allocate batch vector once to avoid repeated allocations
        let mut batch: Vec<SourceEvent> = Vec::with_capacity(batch_size);
        loop {
            batch.clear();
            while batch.len() < batch_size {
                let limit = batch_size - batch.len();
                let event_count = events_rx.recv_many(&mut batch, limit).await;

                if event_count == 0 {
                    bail!(
                        "batch EventHandler listener exited. connector: {}",
                        self.pipeline.name
                    );
                }

                debug!("collected events for batch: {}", batch.len());
            }

            debug!("processing batch of size: {}", batch.len());

            if let Err(err) = self.process_event_batch(&mut batch).await {
                error!(
                    job_name = %self.pipeline.name,
                    "failed to process batch event: {}", err
                )
            }
        }
    }

    async fn process_event_batch(&mut self, batch: &mut Vec<SourceEvent>) -> anyhow::Result<()> {
        if batch.is_empty() {
            bail!("empty event batch");
        }

        let source_encoding = batch[0].encoding.clone();
        let attributes = batch[0].attributes.clone();
        let last_cursor = batch.last().and_then(|event| event.cursor.clone());
        let payloads: Vec<Vec<u8>> = batch.drain(..).map(|event| event.raw_bytes).collect();

        let batch_event = block_in_place(|| {
            SchemaEncoder::new(
                &source_encoding,
                &self.pipeline.source_out_encoding,
                &self.pipeline.source_schema,
            )
            .apply_to_items(payloads)
            .map(|payload| SourceEvent {
                raw_bytes: payload,
                attributes: attributes,
                encoding: self.pipeline.source_out_encoding.clone(),
                is_framed_batch: true,
                cursor: last_cursor,
            })
        })?;

        debug!(
            "generated SourceEvent for batch. size: {} bytes",
            batch_event.raw_bytes.len()
        );

        if let Err(err) = self.process_event(batch_event).await {
            error!(job_name = %self.pipeline.name, "failed to process event: {}", err)
        }

        Ok(())
    }

    async fn process_event(&mut self, source_event: SourceEvent) -> anyhow::Result<()> {
        let event_bytes = source_event.raw_bytes.len() as u64;
        let event_holder = self.apply_middlewares(source_event).await?;
        let mut all_sinks_succeeded = true;

        // Extract cursor before the loop for checkpointing
        let cursor = event_holder.cursor.clone();

        let mut event_option = Some(event_holder);
        let sinks_len = self.pipeline.sinks.len();

        for (i, sink_def) in self.pipeline.sinks.iter_mut().enumerate() {
            let sink_event_result = block_in_place(|| {
                let event = if i == sinks_len - 1 {
                    // Move the event on the last iteration to avoid clone
                    event_option.take().expect("event should be present")
                } else {
                    // Clone for intermediate sinks
                    event_option
                        .as_ref()
                        .expect("event should be present")
                        .clone()
                };
                // apply sink schema
                event.apply_schema(Some(&sink_def.config.output_encoding), &sink_def.schema)
            });

            let sink_event: SinkEvent = match sink_event_result {
                Ok(source_event) => {
                    debug!("transforming into sink event: {:?}", source_event);
                    source_event.into()
                }
                Err(err) => {
                    error!(
                        job_name = %self.pipeline.name,
                        "failed to encode for sink {}/{}: {}",
                        sink_def.config.service_name,
                        sink_def.config.resource,
                        err
                    );
                    all_sinks_succeeded = false;
                    continue;
                }
            };

            // maybe we need a config in publisher, ie explode batch
            match sink_def
                .sink
                .publish(sink_event, sink_def.config.resource.clone(), None)
                .await
            {
                Ok(message) => {
                    info!(
                        job_name = %self.pipeline.name,
                        "published to {}/{}: {}",
                        sink_def.config.service_name,
                        sink_def.config.resource,
                        message
                    );
                }
                Err(err) => {
                    error!(
                        job_name = %self.pipeline.name,
                        "failed to publish to {}/{}: {:#}",
                        sink_def.config.service_name,
                        sink_def.config.resource,
                        err
                    );
                    all_sinks_succeeded = false;
                    continue;
                }
            };
        }

        self.record_event_outcome(all_sinks_succeeded, event_bytes);

        if self.pipeline.with_checkpoints && all_sinks_succeeded {
            self.save_checkpoint(cursor).await?;
        }

        Ok(())
    }

    fn record_event_outcome(&self, success: bool, bytes: u64) {
        if let Some(ref m) = self.metrics {
            if success {
                m.record_success(bytes);
            } else {
                m.record_error();
            }
        }
    }

    async fn save_checkpoint(&self, cursor: Option<Vec<u8>>) -> anyhow::Result<()> {
        match cursor {
            Some(c) => {
                let cp = Checkpoint {
                    job_name: self.pipeline.name.clone(),
                    cursor: c,
                    updated_at: Utc::now().timestamp_millis(),
                };

                self.pipeline.checkpointer.save(&cp).await?;
                Ok(())
            }
            None => {
                warn!(
                    job_name = %self.pipeline.name,
                    "checkpointing: missing cursor in source event"
                );
                Ok(())
            }
        }
    }

    async fn apply_middlewares(
        &mut self,
        source_event: SourceEvent,
    ) -> anyhow::Result<SourceEvent> {
        let mut transformed_event = source_event;

        for middleware_def in self.pipeline.middlewares.iter_mut() {
            transformed_event = middleware_def.provider.transform(transformed_event).await?;

            transformed_event = block_in_place(|| {
                transformed_event.apply_schema(
                    Some(&middleware_def.config.output_encoding),
                    &middleware_def.schema,
                )
            })
            .map_err(|err| {
                anyhow!(
                    "middleware: {}:{}. schema: {:?}: {}",
                    middleware_def.config.service_name,
                    middleware_def.config.resource,
                    middleware_def.config.schema_id,
                    err
                )
            })?;
        }

        Ok(transformed_event)
    }
}
