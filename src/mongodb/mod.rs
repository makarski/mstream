use std::collections::HashMap;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mongodb::{
    Client, Database,
    bson::{self, RawDocumentBuf, doc},
    change_stream::{
        ChangeStream,
        event::{ChangeStreamEvent, OperationType, ResumeToken},
    },
    options::{ChangeStreamOptions, ClientOptions, FullDocumentBeforeChangeType, FullDocumentType},
};
use tokio::sync::mpsc::Sender;
use tracing::{debug, info};

use crate::source::{EventSource, SourceEvent};
use crate::{checkpoint::Checkpoint, config::Encoding};

pub mod checkpoint;
pub mod persister;
pub mod test_suite;

pub async fn db_client(name: String, conn_str: &str) -> anyhow::Result<Client> {
    let mut opts = ClientOptions::parse(conn_str).await?;
    opts.app_name = Some(name);

    Ok(Client::with_options(opts)?)
}

type CStream = ChangeStream<ChangeStreamEvent<RawDocumentBuf>>;

pub struct MongoDbChangeStreamListener {
    db: Database,
    db_name: String,
    db_collection: String,
    resume_token: Option<ResumeToken>,
}

impl MongoDbChangeStreamListener {
    pub fn new(
        db: Database,
        db_name: String,
        db_collection: String,
        checkpoint: Option<Checkpoint>,
    ) -> anyhow::Result<Self> {
        let resume_token = resume_token_from_checkpoint(&db_name, &db_collection, checkpoint)?;

        Ok(Self {
            db,
            db_name,
            db_collection,
            resume_token,
        })
    }

    async fn change_stream(&self) -> anyhow::Result<CStream> {
        // enable support for full document before and after change
        // used to obtain the document for delete events
        // https://docs.mongodb.com/manual/reference/command/collMod/#dbcmd.collMod
        self.db
            .run_command(doc! {
                "collMod": self.db_collection.clone(),
                "changeStreamPreAndPostImages": doc! {
                    "enabled": true,
                }
            })
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to enable full document support for stream: {}, {}",
                    &self.db_collection,
                    err
                )
            })?;

        let coll = self.db.collection::<RawDocumentBuf>(&self.db_collection);
        let opts = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .full_document_before_change(Some(FullDocumentBeforeChangeType::WhenAvailable))
            .start_after(self.resume_token.clone())
            .build();

        Ok(coll.watch().with_options(opts).await?)
    }

    fn event_metadata(
        &self,
        event: &ChangeStreamEvent<RawDocumentBuf>,
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

            if let Some(raw_document) = bson_doc {
                let bson_raw_bytes = raw_document.as_bytes().to_vec();
                let cursor = resume_token_to_slice(cs.resume_token())?;

                events
                    .send(SourceEvent {
                        raw_bytes: bson_raw_bytes,
                        attributes,
                        encoding: Encoding::Bson,
                        is_framed_batch: false,
                        cursor,
                    })
                    .await?;
            }
        }

        Ok(())
    }
}

impl TryFrom<Checkpoint> for ResumeToken {
    type Error = anyhow::Error;

    fn try_from(cp: Checkpoint) -> Result<Self, Self::Error> {
        let cursor = bson::from_slice(&cp.cursor)?;
        Ok(ResumeToken::from(cursor))
    }
}

fn resume_token_from_checkpoint(
    db: &str,
    coll: &str,
    checkpoint: Option<Checkpoint>,
) -> anyhow::Result<Option<ResumeToken>> {
    match checkpoint {
        Some(cp) => {
            info!(
                "loaded checkpoint for MongoDB change stream on {}.{} with resume token: {}",
                db,
                coll,
                DateTime::<Utc>::from_timestamp_millis(cp.updated_at).unwrap_or_default(),
            );
            Ok(Some(ResumeToken::try_from(cp)?))
        }
        None => Ok(None),
    }
}

fn resume_token_to_slice(rt: Option<ResumeToken>) -> anyhow::Result<Option<Vec<u8>>> {
    let cursor: Option<Vec<u8>> = match rt {
        Some(t) => Some(bson::to_vec(&t)?),
        None => None,
    };

    Ok(cursor)
}
