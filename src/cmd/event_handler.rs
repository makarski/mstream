use core::iter::Iterator;

use anyhow::{anyhow, bail};
use log::{debug, error, info};
use tokio::{sync::mpsc::Receiver, task::block_in_place};

use crate::{
    config::{Encoding, ServiceConfigReference},
    middleware::MiddlewareProvider,
    schema::{encoding::SchemaEncoder, Schema},
    sink::{encoding::SinkEvent, EventSink, SinkProvider},
    source::{SourceBatch, SourceEvent},
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
        loop {
            let mut batch: Vec<SourceEvent> = Vec::with_capacity(batch_size);

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

            let source_batch = SourceBatch::new(batch);
            debug!("processing batch: {:?}", source_batch);

            if let Err(err) = self.process_event_batch(source_batch).await {
                error!(
                    "{}: failed to process batch event: {}",
                    &self.connector_name, err
                )
            }
        }
    }

    async fn process_event_batch(&mut self, event_batch: SourceBatch) -> anyhow::Result<()> {
        let source_encoding = event_batch.encoding()?;
        let attributes = event_batch.attributes().cloned();
        let payloads = event_batch.into_iter().map(|event| event.raw_bytes);

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

        debug!("generated SourceEvent for batch: {:?}", batch_event);

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

        for (sink_cfg, publisher, schema) in self.publishers.iter_mut() {
            let sink_event_result = block_in_place(|| {
                transformed_source_event
                    .clone()
                    .apply_schema(Some(&sink_cfg.output_encoding), &schema)
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
