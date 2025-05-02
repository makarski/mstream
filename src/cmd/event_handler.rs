use anyhow::anyhow;
use log::{debug, error, info};
use tokio::sync::mpsc::Receiver;

use crate::{
    config::{Encoding, ServiceConfigReference},
    middleware::MiddlewareProvider,
    schema::Schema,
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

    async fn process_event(&mut self, source_event: SourceEvent) -> anyhow::Result<()> {
        let mut transformed_source_event = source_event;

        // apply schema for source event if any
        if !matches!(&self.source_schema, Schema::Undefined) {
            transformed_source_event = transformed_source_event
                .apply_schema(Some(&self.source_output_encoding), &self.source_schema)?;
        }

        if let Some(middlewares) = &mut self.middlewares {
            for (mdlw_cfg, middleware, schema) in middlewares.iter_mut() {
                transformed_source_event = middleware
                    .transform(transformed_source_event)
                    .await
                    .and_then(|event| event.apply_schema(Some(&mdlw_cfg.output_encoding), &schema))
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
            let sink_event: SinkEvent = match transformed_source_event
                .clone()
                .apply_schema(Some(&sink_cfg.output_encoding), &schema)
            {
                Ok(sink_event) => {
                    debug!("transformed sink event: {:?}", sink_event);
                    sink_event.into()
                }
                Err(err) => {
                    error!(
                        "sink: {}, resource: {}, schema: {:?}, {}",
                        &sink_cfg.service_name, &sink_cfg.resource, &sink_cfg.schema_id, err
                    );
                    continue;
                }
            };

            match publisher
                .publish(sink_event, sink_cfg.resource.clone(), None)
                .await
            {
                Ok(message) => {
                    info!(
                        "published a message to: {}:{:?}. stream: {}. resource: {}",
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
