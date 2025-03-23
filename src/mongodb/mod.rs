use std::collections::HashMap;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use log::debug;
use mongodb::{
    bson::{doc, Document},
    change_stream::{
        event::{ChangeStreamEvent, OperationType, ResumeToken},
        ChangeStream,
    },
    options::{ChangeStreamOptions, ClientOptions, FullDocumentBeforeChangeType, FullDocumentType},
    Client, Database,
};
use tokio::sync::mpsc::Sender;

use crate::config::Encoding;
use crate::source::{EventSource, SourceEvent};

pub mod persister;

pub async fn db_client(name: String, conn_str: &str) -> anyhow::Result<Client> {
    let mut opts = ClientOptions::parse(conn_str).await?;
    opts.app_name = Some(name);

    Ok(Client::with_options(opts)?)
}

type CStream = ChangeStream<ChangeStreamEvent<Document>>;

pub struct MongoDbChangeStreamListener {
    db: Database,
    db_name: String,
    db_collection: String,
    resume_token: Option<ResumeToken>,
}

impl MongoDbChangeStreamListener {
    pub fn new(db: Database, db_name: String, db_collection: String) -> Self {
        Self {
            db,
            db_name,
            db_collection,
            resume_token: None,
        }
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
                    &self.db_collection,
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

    fn event_metadata(
        &self,
        event: &ChangeStreamEvent<Document>,
    ) -> Option<HashMap<String, String>> {
        Some(HashMap::from([
            (
                "operation_type".to_owned(),
                format!("{:?}", event.operation_type).to_lowercase(),
            ),
            ("database".to_owned(), self.db_name.clone()),
            ("collection".to_owned(), self.db_collection.clone()),
        ]))
    }
}

#[async_trait]
impl EventSource for MongoDbChangeStreamListener {
    async fn listen(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()> {
        let mut cs = self.change_stream().await?;

        while cs.is_alive() {
            let Some(event) = cs.next_if_any().await? else {
                continue;
            };
            let attributes = self.event_metadata(&event);
            // self.resume_token = cs.resume_token();

            let bson_doc = match event.operation_type {
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

            if let Some(document) = bson_doc {
                events
                    .send(SourceEvent {
                        raw_bytes: None,
                        document: Some(document),
                        attributes,
                        encoding: Encoding::Bson,
                    })
                    .await?;
            }
        }

        Ok(())
    }
}
