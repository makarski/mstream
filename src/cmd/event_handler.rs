use core::iter::Iterator;

use anyhow::{anyhow, bail};
use log::{debug, error, info};
use tokio::{sync::mpsc::Receiver, task::block_in_place};

use crate::{
    config::{Encoding, ServiceConfigReference},
    middleware::MiddlewareProvider,
    schema::{encoding::SchemaEncoder, Schema},
    sink::{encoding::SinkEvent, EventSink, SinkProvider},
    source::SourceEvent,
};

pub struct EventHandler {
    pub connector_name: String,
    pub source_schema: Schema,
    pub source_output_encoding: Encoding,
    pub publishers: Vec<(ServiceConfigReference, SinkProvider, Schema)>,
    pub middlewares: Option<Vec<(ServiceConfigReference, MiddlewareProvider, Schema)>>,
}

impl EventHandler {
    /// Listen to source streams and publish the events to the configured sinks
    pub(super) async fn listen(
        &mut self,
        mut events_rx: Receiver<SourceEvent>,
    ) -> anyhow::Result<()> {
        loop {
            match events_rx.recv().await {
                Some(event) => {
                    debug!("processing event: {:?}", event);

                    let event = block_in_place(|| {
                        event
                            .apply_schema(Some(&self.source_output_encoding), &self.source_schema)
                            .map_err(|err| anyhow!("failed to apply source schema: {}", err))
                    })?;

                    if let Err(err) = self.process_event(event).await {
                        error!("{}: failed to process event: {}", &self.connector_name, err)
                    }
                }
                None => {
                    info!("source listener exited. connector: {}", self.connector_name);
                    break;
                }
            }
        }

        Ok(())
    }

    pub(super) async fn listen_batch(
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
                        self.connector_name
                    );
                }

                debug!("collected events for batch: {}", batch.len());
            }

            debug!("processing batch of size: {}", batch.len());

            if let Err(err) = self.process_event_batch(&mut batch).await {
                error!(
                    "{}: failed to process batch event: {}",
                    &self.connector_name, err
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

        let schema_encoder = SchemaEncoder::new_batch(payloads);

        let batch_event = block_in_place(|| {
            schema_encoder
                .apply_schema(
                    &source_encoding,
                    &self.source_output_encoding,
                    &self.source_schema,
                )
                .map(|payload| SourceEvent {
                    raw_bytes: payload,
                    attributes: attributes,
                    encoding: self.source_output_encoding.clone(),
                    is_framed_batch: true,
                })
        })?;

        debug!(
            "generated SourceEvent for batch. size: {} bytes",
            batch_event.raw_bytes.len()
        );

        if let Err(err) = self.process_event(batch_event).await {
            error!("{}: failed to process event: {}", &self.connector_name, err)
        }

        Ok(())
    }

    async fn process_event(&mut self, source_event: SourceEvent) -> anyhow::Result<()> {
        let mut transformed_source_event = source_event;

        if let Some(middlewares) = &mut self.middlewares {
            for (mdlw_cfg, middleware, schema) in middlewares.iter_mut() {
                transformed_source_event = middleware.transform(transformed_source_event).await?;

                transformed_source_event = block_in_place(|| {
                    transformed_source_event.apply_schema(Some(&mdlw_cfg.output_encoding), &schema)
                })
                .map_err(|err| {
                    anyhow!(
                        "middleware: {}:{}. schema: {:?}: {}",
                        mdlw_cfg.service_name,
                        mdlw_cfg.resource,
                        mdlw_cfg.schema_id,
                        err
                    )
                })?;
            }
        }

        let mut event_holder = Some(transformed_source_event);
        let publishers_len = self.publishers.len();

        for (i, (sink_cfg, publisher, schema)) in self.publishers.iter_mut().enumerate() {
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
                event.apply_schema(Some(&sink_cfg.output_encoding), &schema)
            });

            let sink_event: SinkEvent = match sink_event_result {
                Ok(source_event) => {
                    debug!("transforming into sink event: {:?}", source_event);
                    source_event.into()
                }
                Err(err) => {
                    error!(
                        "sink: {}, resource: {}, schema: {:?}, {}",
                        &sink_cfg.service_name, &sink_cfg.resource, &sink_cfg.schema_id, err
                    );
                    continue;
                }
            };

            // maybe we need a config in publisher, ie explode batch
            match publisher
                .publish(sink_event, sink_cfg.resource.clone(), None)
                .await
            {
                Ok(message) => {
                    info!(
                        "published a message to: {}:{}. stream: {}. resource: {}",
                        sink_cfg.service_name, message, &self.connector_name, sink_cfg.resource,
                    );
                }
                Err(err) => {
                    error!(
                        "failed to publish message to sink: {}:{}, {}",
                        &sink_cfg.service_name, &sink_cfg.resource, err
                    );
                    continue;
                }
            };
        }

        Ok(())
    }
}
