use anyhow::Ok;
use log::{debug, error, info};
use tokio::sync::mpsc::Receiver;

use crate::{
    config::ServiceConfigReference,
    middleware::MiddlewareProvider,
    schema::{Schema, SchemaProvider, SchemaRegistry},
    sink::{encoding::SinkEvent, EventSink, SinkProvider},
    source::SourceEvent,
};

pub struct EventHandler {
    pub connector_name: String,
    pub schema_provider: Option<(String, SchemaProvider)>,
    pub publishers: Vec<(ServiceConfigReference, SinkProvider)>,
    pub middlewares: Option<Vec<(ServiceConfigReference, MiddlewareProvider)>>,
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
                        error!("failed to process event: {}", err)
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

    async fn get_schema(&mut self) -> anyhow::Result<Schema> {
        if let Some((name, schema_provider)) = self.schema_provider.as_mut() {
            schema_provider.get_schema(name.clone()).await
        } else {
            Ok(Schema::Undefined)
        }
    }

    async fn process_event(&mut self, source_event: SourceEvent) -> anyhow::Result<()> {
        let schema = self.get_schema().await?;
        let mut modified_source_event = source_event;

        if let Some(middlewares) = &mut self.middlewares {
            for (_, middleware) in middlewares.iter_mut() {
                modified_source_event = middleware.transform(modified_source_event).await?;
            }
        }

        for (sink_cfg, publisher) in self.publishers.iter_mut() {
            let sink_event =
                SinkEvent::from_source_event(modified_source_event.clone(), sink_cfg, &schema)?;

            let message = publisher
                .publish(sink_event, sink_cfg.resource.clone(), None)
                .await?;

            info!(
                "published a message to: {}:{:?}. stream: {}. id: {}",
                sink_cfg.service_name, message, &self.connector_name, sink_cfg.resource,
            );
        }

        Ok(())
    }
}
