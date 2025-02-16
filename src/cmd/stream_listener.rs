use std::collections::HashMap;

use anyhow::{anyhow, bail};
use log::{debug, error, info};
use mongodb::{
    bson::{doc, Document},
    change_stream::{
        event::{ChangeStreamEvent, OperationType, ResumeToken},
        ChangeStream,
    },
    options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType},
    Database,
};

use crate::{
    config::ServiceConfigReference,
    encoding::avro::encode,
    schema::{SchemaProvider, SchemaRegistry},
    sink::{EventSink, SinkProvider},
};

type CStream = ChangeStream<ChangeStreamEvent<Document>>;

pub struct StreamListener<'a> {
    pub connector_name: String,
    pub schema_name: String,
    pub db: Database,
    pub db_name: String,
    pub db_collection: String,
    pub schema_provider: &'a mut SchemaProvider,
    pub publishers: Vec<(ServiceConfigReference, SinkProvider)>,
    pub resume_token: Option<ResumeToken>,
}

impl StreamListener<'_> {
    /// Listen to a mongodb change stream and publish the events to the configured sinks
    pub async fn listen(&mut self) -> anyhow::Result<()> {
        let mut cs = self.change_stream().await?;

        while cs.is_alive() {
            let Some(event) = cs.next_if_any().await? else {
                continue;
            };
            let attributes = self.event_metadata(&event);
            // self.resume_token = cs.resume_token();

            let mongo_doc = match event.operation_type {
                OperationType::Insert | OperationType::Update => {
                    debug!("got insert/update event: {:?}", event);
                    event.full_document
                }
                OperationType::Delete => {
                    debug!("got delete event: {:?}", event);
                    event.full_document_before_change
                }
                OperationType::Invalidate => {
                    bail!("got invalidate event: {:?}", event);
                }
                OperationType::Drop => {
                    bail!("got drop event: {:?}", event);
                }
                OperationType::DropDatabase => {
                    bail!("got drop database event: {:?}", event);
                }

                // currently not handling other operation types
                _ => None,
            };

            if let Some(mongo_doc) = mongo_doc {
                _ = &self
                    .process_event(mongo_doc, attributes)
                    .await
                    .map_err(|err| error!("{err}"));
            }
        }

        Ok(())
    }

    fn event_metadata(&self, event: &ChangeStreamEvent<Document>) -> HashMap<String, String> {
        HashMap::from([
            ("stream_name".to_owned(), self.connector_name.clone()),
            (
                "operation_type".to_owned(),
                format!("{:?}", event.operation_type).to_lowercase(),
            ),
            ("database".to_owned(), self.db_name.clone()),
            ("collection".to_owned(), self.db_collection.clone()),
        ])
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

    async fn change_stream(&self) -> anyhow::Result<CStream> {
        // enable support for full document before and after change
        // used to obtain the document for delete events
        // https://docs.mongodb.com/manual/reference/command/collMod/#dbcmd.collMod
        self.db
            .run_command(
                doc! {
                    "collMod": self.db_collection.clone(),
                    "changeStreamPreAndPostImages": doc! {
                        "enabled": true,
                    }
                },
                None,
            )
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to enable full document support for stream: {}, {}",
                    &self.connector_name,
                    err
                )
            })?;

        let coll = self.db.collection::<Document>(self.db.name());

        let opts = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .full_document_before_change(Some(FullDocumentBeforeChangeType::WhenAvailable))
            .start_after(self.resume_token.clone())
            .build();

        Ok(coll.watch(None, Some(opts)).await?)
    }
}
