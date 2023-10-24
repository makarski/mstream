use std::collections::HashMap;

use anyhow::{anyhow, bail};
use gauth::serv_account::ServiceAccount;
use log::{debug, error, info};
use mongodb::bson::{doc, Document};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::change_stream::ChangeStream;
use mongodb::options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType};
use mongodb::Database;
use tokio::sync::mpsc::Sender;

use crate::config::{Config, Connector, SchemaProviderName};
use crate::db::db_client;
use crate::encoding::avro::encode;
use crate::pubsub;
use crate::schema::{MongoDbSchemaProvider, SchemaProvider};
use pubsub::api::{PublishRequest, PubsubMessage};
use pubsub::srvc::{publisher, PublisherService, SchemaService};
use pubsub::{GCPTokenProvider, ServiceAccountAuth};

/// Listen to mongodb change streams and publish the events to a pubsub topic
pub async fn listen_streams(done_ch: Sender<String>, cfg: Config) -> anyhow::Result<()> {
    let mut token_provider =
        ServiceAccount::from_file(&cfg.gcp_serv_acc_key_path, pubsub::SCOPES.to_vec());

    // token provider is lazily initialized - warm it up
    let _ = token_provider.access_token()?;

    for connector_cfg in cfg.connectors {
        info!(
            "listening to: {}:{}",
            connector_cfg.db_name, connector_cfg.db_collection
        );

        // token_provider is Arc and can be cloned without performance penalty
        let gcp_auth_inteceptor = ServiceAccountAuth::new(token_provider.clone());
        let done_ch = done_ch.clone();
        tokio::spawn(async move {
            let cnt_name = connector_cfg.name.clone();
            let stream_listener = StreamListener::new(connector_cfg, gcp_auth_inteceptor).await;

            match stream_listener {
                Ok(mut stream_listener) => {
                    if let Err(err) = stream_listener.listen().await {
                        error!("{err}")
                    }
                }
                Err(err) => error!("{err}"),
            }

            // send done signal
            if let Err(err) = done_ch.send(cnt_name.clone()).await {
                error!(
                    "failed to send done signal: {}: connector: {}",
                    err, cnt_name
                );
            }
        });
    }

    Ok(())
}

/// ChangeStream is a mongodb change stream
type CStream = ChangeStream<ChangeStreamEvent<Document>>;

/// StreamListener listens to a mongodb change stream and publishes the events to a pubsub topic
struct StreamListener<'a, P: GCPTokenProvider + Clone> {
    connector_name: String,
    schema_name: String,
    topic: String,
    db: Database,
    db_name: String,
    db_collection: String,
    schema_srvc: Box<dyn SchemaProvider + 'a + Send + Sync>,
    publisher: PublisherService<ServiceAccountAuth<P>>,
    resume_token: Option<ResumeToken>,
}

impl<'a, P> StreamListener<'a, P>
where
    P: GCPTokenProvider + Clone + 'static + Send + Sync,
{
    async fn new(
        connector: Connector,
        auth_interceptor: ServiceAccountAuth<P>,
    ) -> anyhow::Result<StreamListener<'a, P>> {
        let publisher = publisher(auth_interceptor.clone()).await?;
        let db = db_client(connector.name.clone(), &connector.db_connection)
            .await?
            .database(&connector.db_name);

        let schema_srvc =
            get_schema_service(connector.schema.provider, auth_interceptor, db.clone()).await?;

        Ok(StreamListener {
            connector_name: connector.name,
            schema_name: connector.schema.id,
            topic: connector.topic,
            db_name: connector.db_name,
            db_collection: connector.db_collection,
            publisher,
            db,
            resume_token: None,
            schema_srvc,
        })
    }

    /// Listen to a mongodb change stream and publish the events to a pubsub topic
    async fn listen(&mut self) -> anyhow::Result<()> {
        let mut cs = self.change_stream().await?;

        while cs.is_alive() {
            let Some(event) = cs.next_if_any().await? else { continue };
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
            .schema_srvc
            .get_schema(self.schema_name.clone())
            .await?;
        let avro_encoded = encode(mongo_doc, schema)?;

        let message = self
            .publisher
            .publish(PublishRequest {
                topic: self.topic.clone(),
                messages: vec![PubsubMessage {
                    data: avro_encoded,
                    attributes,
                    ..Default::default()
                }],
            })
            .await
            .map_err(|err| {
                anyhow!(
                    "{}. stream: {}. schema: {}. topic: {}",
                    err.message(),
                    &self.connector_name,
                    &self.schema_name,
                    &self.topic
                )
            })?;

        info!(
            "successfully published a message: {:?}. stream: {}. schema: {}. topic: {}",
            message.into_inner(),
            &self.connector_name,
            &self.schema_name,
            &self.topic,
        );

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

        let coll = self.db.collection::<Document>(&self.db_collection);

        let opts = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .full_document_before_change(Some(FullDocumentBeforeChangeType::WhenAvailable))
            .start_after(self.resume_token.clone())
            .build();

        Ok(coll.watch(None, Some(opts)).await?)
    }
}

async fn get_schema_service<P>(
    provider_name: SchemaProviderName,
    auth_interceptor: ServiceAccountAuth<P>,
    db: Database,
) -> anyhow::Result<Box<dyn SchemaProvider + Send + Sync>>
where
    P: GCPTokenProvider + Clone + 'static + Send + Sync,
{
    Ok(match provider_name {
        SchemaProviderName::Gcp => {
            Box::new(SchemaService::with_interceptor(auth_interceptor).await?)
        }
        SchemaProviderName::MongoDB => Box::new(MongoDbSchemaProvider::new(db).await),
    })
}
