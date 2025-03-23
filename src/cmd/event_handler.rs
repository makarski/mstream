use log::{debug, error, info};
use tokio::sync::mpsc::Receiver;

use crate::{
    config::ServiceConfigReference,
    schema::{SchemaProvider, SchemaRegistry},
    sink::{encoding::SinkEvent, EventSink, SinkProvider},
    source::SourceEvent,
};

// rename to event handler - do event processing here
pub struct EventHandler<'a> {
    pub connector_name: String,
    pub schema_name: String,
    pub schema_provider: &'a mut SchemaProvider,
    pub publishers: Vec<(ServiceConfigReference, SinkProvider)>,
}

impl EventHandler<'_> {
    /// Listen to a mongodb change stream and publish the events to the configured sinks
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

    async fn process_event(&mut self, source_event: SourceEvent) -> anyhow::Result<()> {
        let schema = self
            .schema_provider
            .get_schema(self.schema_name.clone())
            .await?;

        for (sink_cfg, publisher) in self.publishers.iter_mut() {
            let sink_event = SinkEvent::from_source_event(source_event.clone(), sink_cfg, &schema)?;

            let message = publisher
                .publish(sink_event, sink_cfg.id.clone(), None)
                .await?;

            info!(
                "successfully published a message: {:?}. stream: {}. schema: {}. id: {}",
                message, &self.connector_name, &self.schema_name, sink_cfg.id,
            );
        }

        Ok(())
    }
}
