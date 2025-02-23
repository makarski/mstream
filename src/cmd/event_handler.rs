use std::collections::HashMap;

use log::{debug, error, info};
use mongodb::bson::Document;
use tokio::sync::mpsc::Receiver;

use crate::{
    config::ServiceConfigReference,
    encoding::avro::encode,
    schema::{SchemaProvider, SchemaRegistry},
    sink::{EventSink, SinkProvider},
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
                    debug!("processing event: {}", event.document);

                    if let Err(err) = self.process_event(event.document, event.attributes).await {
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

    async fn process_event(
        &mut self,
        mongo_doc: Document,
        attributes: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let schema = self
            .schema_provider
            .get_schema(self.schema_name.clone())
            .await?;
        let avro_encoded = encode(mongo_doc, schema)?;

        for (sink_cfg, publisher) in self.publishers.iter_mut() {
            let message = publisher
                .publish(
                    sink_cfg.id.clone(),
                    avro_encoded.clone(),
                    None,
                    Some(attributes.clone()),
                )
                .await?;

            info!(
                "successfully published a message: {:?}. stream: {}. schema: {}. id: {}",
                message, &self.connector_name, &self.schema_name, sink_cfg.id,
            );
        }

        Ok(())
    }
}
