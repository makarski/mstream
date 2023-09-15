use std::collections::HashMap;

use anyhow::anyhow;
use log::{debug, error, info};
use mongodb::bson::Document;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::change_stream::ChangeStream;
use mongodb::options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType};
use mongodb::Client;

use crate::config::{Config, Connector};
use crate::db::db_client;
use crate::encoding::avro::encode;
use crate::pubsub;
use pubsub::api::{PublishRequest, PubsubMessage};
use pubsub::srvc::{publisher, PublisherService, SchemaService};
use pubsub::{GCPTokenProvider, ServiceAccountAuth};

/// Listen to mongodb change streams and publish the events to a pubsub topic
pub async fn listen_streams<P>(cfg: Config, token_provider: P) -> anyhow::Result<()>
where
    P: GCPTokenProvider + Clone + Send + Sync + 'static,
{
    for connector_cfg in cfg.connectors {
        debug!(
            "listening to: {}:{}",
            connector_cfg.db_name, connector_cfg.db_collection
        );

        let token_provider = token_provider.clone();
        tokio::spawn(async move {
            let stream_listener = StreamListener::new(connector_cfg, token_provider).await;

            match stream_listener {
                Ok(mut stream_listener) => {
                    if let Err(err) = stream_listener.listen().await {
                        error!("{err}")
                    }
                }
                Err(err) => error!("{err}"),
            }
        });
    }

    Ok(())
}

/// ChangeStream is a mongodb change stream
type CStream = ChangeStream<ChangeStreamEvent<Document>>;

/// StreamListener listens to a mongodb change stream and publishes the events to a pubsub topic
struct StreamListener<P: GCPTokenProvider + Clone> {
    connector_name: String,
    schema_name: String,
    topic: String,
    db_name: String,
    db_collection: String,
    schema_srvc: SchemaService<ServiceAccountAuth<P>>,
    publisher: PublisherService<ServiceAccountAuth<P>>,
    db_client: Client,
}

impl<P> StreamListener<P>
where
    P: GCPTokenProvider + Clone,
{
    async fn new(connector: Connector, token_provider: P) -> anyhow::Result<Self> {
        let auth_interceptor = ServiceAccountAuth::new(token_provider.clone());

        let schema_srvc = SchemaService::with_interceptor(auth_interceptor.clone()).await?;
        let publisher = publisher(auth_interceptor).await?;
        let db_client = db_client(connector.name.clone(), &connector.db_connection).await?;

        Ok(Self {
            connector_name: connector.name,
            schema_name: connector.schema,
            topic: connector.topic,
            db_name: connector.db_name,
            db_collection: connector.db_collection,
            schema_srvc,
            publisher,
            db_client,
        })
    }

    /// Listen to a mongodb change stream and publish the events to a pubsub topic
    async fn listen(&mut self) -> anyhow::Result<()> {
        let mut cs = self.change_stream().await?;

        while cs.is_alive() {
            let Some(event) = cs.next_if_any().await? else { continue };
            let headers = self.event_metadata(&event);

            let mongo_doc = match event.operation_type {
                OperationType::Insert | OperationType::Update => {
                    debug!("got insert/update event: {:?}", event);
                    event.full_document
                }
                OperationType::Delete => {
                    debug!("got delete event: {:?}", event);
                    event.full_document_before_change
                }

                // currently not handling other operation types
                _ => None,
            };

            if let Some(mongo_doc) = mongo_doc {
                _ = &self
                    .process_event(mongo_doc, headers)
                    .await
                    .map_err(|err| error!("{err}"));
            }
        }

        Ok(())
    }

    fn event_metadata(&self, event: &ChangeStreamEvent<Document>) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "operation_type".to_string(),
            format!("{:?}", event.operation_type),
        );
        metadata
    }

    async fn process_event(
        &mut self,
        mongo_doc: Document,
        headers: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let schema = self
            .schema_srvc
            .get_schema(self.schema_name.clone())
            .await?;

        // todo: publish operation type to attributes field
        // check if routing can be done based on attributes on pubsub side

        let avro_encoded = encode(mongo_doc, &schema.definition)?;

        let message = self
            .publisher
            .publish(PublishRequest {
                topic: self.topic.clone(),
                messages: vec![PubsubMessage {
                    data: avro_encoded,
                    attributes: headers,
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
        let db = self.db_client.database(&self.db_name);
        let coll = db.collection::<Document>(&self.db_collection);

        let opts = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .full_document_before_change(Some(FullDocumentBeforeChangeType::WhenAvailable))
            .build();

        Ok(coll.watch(None, Some(opts)).await?)
    }
}
