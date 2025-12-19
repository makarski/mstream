use core::iter::Iterator;

use anyhow::{anyhow, bail};
use log::{debug, error, info};
use tokio::{sync::mpsc::Receiver, task::block_in_place};

use crate::{
    provision::pipeline::Pipeline,
    schema::encoding::SchemaEncoder,
    sink::{encoding::SinkEvent, EventSink},
    source::SourceEvent,
};

pub(super) struct EventHandler {
    pipeline: Pipeline,
}

impl EventHandler {
    pub fn new(pipeline: Pipeline) -> Self {
        Self { pipeline }
    }

    /// Entry point for the pipeline's event processing logic.
    ///
    /// Determines the appropriate processing strategy (streaming or batching) based on
    /// the pipeline configuration and begins consuming the event stream.
    pub async fn handle(&mut self, events_rx: Receiver<SourceEvent>) -> anyhow::Result<()> {
        if self.pipeline.is_batching_enabled {
            let batch_size = self.pipeline.batch_size;
            info!(
                "starting batch EventHandler listener. connector: {}, batch size: {}",
                self.pipeline.name, batch_size
            );
            self.run_batch_loop(events_rx, batch_size).await
        } else {
            info!(
                "starting EventHandler listener. connector: {}",
                self.pipeline.name
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
                        error!("{}: failed to process event: {}", &self.pipeline.name, err)
                    }
                }
                None => {
                    info!("source listener exited. connector: {}", self.pipeline.name);
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
                    "{}: failed to process batch event: {}",
                    &self.pipeline.name, err
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
            })
        })?;

        debug!(
            "generated SourceEvent for batch. size: {} bytes",
            batch_event.raw_bytes.len()
        );

        if let Err(err) = self.process_event(batch_event).await {
            error!("{}: failed to process event: {}", &self.pipeline.name, err)
        }

        Ok(())
    }

    async fn process_event(&mut self, source_event: SourceEvent) -> anyhow::Result<()> {
        let mut transformed_source_event = source_event;

        for middleware_def in self.pipeline.middlewares.iter_mut() {
            transformed_source_event = middleware_def
                .provider
                .transform(transformed_source_event)
                .await?;

            transformed_source_event = block_in_place(|| {
                transformed_source_event.apply_schema(
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

        let mut event_holder = Some(transformed_source_event);
        let publishers_len = self.pipeline.sinks.len();

        for (i, sink_def) in self.pipeline.sinks.iter_mut().enumerate() {
            let sink_event_result = block_in_place(|| {
                let event = if i == publishers_len - 1 {
                    // move the last event to avoid clone
                    event_holder.take().expect("publishers: last event move")
                } else {
                    // clone the event for other publishers
                    event_holder
                        .as_ref()
                        .expect("publishers: event ref")
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
                        "sink: {}, resource: {}, schema: {:?}, {}",
                        &sink_def.config.service_name,
                        &sink_def.config.resource,
                        &sink_def.config.schema_id,
                        err
                    );
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
                        "published a message to: {}:{}. stream: {}. resource: {}",
                        sink_def.config.service_name,
                        message,
                        &self.pipeline.name,
                        sink_def.config.resource,
                    );
                }
                Err(err) => {
                    error!(
                        "failed to publish message to sink: {}:{}, {}",
                        &sink_def.config.service_name, &sink_def.config.resource, err
                    );
                    continue;
                }
            };
        }

        Ok(())
    }
}
