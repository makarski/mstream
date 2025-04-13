use anyhow::Ok;
use log::{debug, error, info};
use tokio::sync::mpsc::Receiver;

use crate::{
    config::ServiceConfigReference,
    schema::{Schema, SchemaProvider, SchemaRegistry},
    sink::{encoding::SinkEvent, EventSink, SinkProvider},
    source::SourceEvent,
};

pub struct EventHandler {
    pub connector_name: String,
    pub schema_provider: Option<(String, SchemaProvider)>,
    pub publishers: Vec<(ServiceConfigReference, SinkProvider)>,
}

impl EventHandler {
    /// Listen to source streams and publish the events to the configured sinks
    pub async fn listen(&mut self, mut events_rx: Receiver<SourceEvent>) -> anyhow::Result<()> {
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

        for (sink_cfg, publisher) in self.publishers.iter_mut() {
            let sink_event = SinkEvent::from_source_event(source_event.clone(), sink_cfg, &schema)?;

            let message = publisher
                .publish(sink_event, sink_cfg.id.clone(), None)
                .await?;

            info!(
                "published a message to: {}:{:?}. stream: {}. id: {}",
                sink_cfg.service_name, message, &self.connector_name, sink_cfg.id,
            );
        }

        Ok(())
    }
}
