use crate::config::{Config, Connector};
use crate::db::db_client;
use crate::encoding::avro::encode2;
use crate::pubsub::{
    api::{PublishRequest, PubsubMessage},
    publ::{publisher, PublisherService},
};
use crate::registry::Registry;

use anyhow::anyhow;
use log::{error, info};
use mongodb::{
    bson::Document,
    change_stream::{event::ChangeStreamEvent, ChangeStream},
    options::{ChangeStreamOptions, FullDocumentType},
    Client,
};

pub async fn listen(cfg: Config, access_token: String) -> anyhow::Result<()> {
    for connector in cfg.connectors {
        let access_token = access_token.clone();

        info!(
            "listening to: {}:{}",
            connector.db_name, connector.db_collection
        );

        tokio::spawn(async move {
            if let Err(err) = listen_handle(connector, access_token.as_str()).await {
                error!("{err}")
            }
        });
    }

    Ok(())
}

async fn listen_handle(connector: Connector, access_token: &str) -> anyhow::Result<()> {
    // todo: check whether gcp and db clients could be shared across threads
    // instead of re-initiating them for each connector
    let schema_registry = Registry::with_token(access_token).await?;
    let publisher = publisher(access_token).await?;
    let mut event_handler = CdcEventHandler::new(schema_registry, publisher);
    let db_client = db_client(connector.name, &connector.db_connection).await?;

    // todo: move the loop to main thread - switch the task processing to a spawn
    let mut cs = change_stream(db_client, &connector.db_name, &connector.db_collection).await?;

    while cs.is_alive() {
        if let Some(event) = cs.next_if_any().await? {
            // todo: think whether we should process each event
            // as a separate concurrent task
            if let Some(mongo_doc) = event.full_document {
                _ = &event_handler
                    .handle_cdc_event(mongo_doc, &connector.schema, &connector.topic)
                    .await
                    .map_err(|err| error!("{err}"));
            }
        }

        // resume_token = cs.resume_token();
    }

    Ok(())
}

type CdcStream = ChangeStream<ChangeStreamEvent<Document>>;

async fn change_stream(
    db_client: Client,
    db_name: &str,
    coll_name: &str,
) -> anyhow::Result<CdcStream> {
    let db = db_client.database(db_name);
    let coll = db.collection::<Document>(coll_name);

    let opts = ChangeStreamOptions::builder()
        .full_document(Some(FullDocumentType::UpdateLookup))
        .build();

    Ok(coll.watch(None, Some(opts)).await?)
}

struct CdcEventHandler {
    schema_registry: Registry,
    publisher: PublisherService,
}

impl CdcEventHandler {
    fn new(schema_registry: Registry, publisher: PublisherService) -> Self {
        CdcEventHandler {
            schema_registry,
            publisher,
        }
    }

    async fn handle_cdc_event(
        &mut self,
        mongo_doc: Document,
        schema_name: &str,
        topic: &str,
    ) -> anyhow::Result<()> {
        let schema = self
            .schema_registry
            .get_schema(schema_name.to_owned())
            .await?;

        // todo: publish operation type to attributes field
        // check if routing can be done based on attributes on pubsub side

        let avro_encoded = encode2(mongo_doc, &schema.definition)?;

        let message = self
            .publisher
            .publish(PublishRequest {
                topic: topic.to_owned(),
                messages: vec![PubsubMessage {
                    data: avro_encoded,
                    ..Default::default()
                }],
            })
            .await
            .map_err(|err| {
                anyhow!(
                    "{} schema: {}. topic: {}",
                    err.message(),
                    schema_name,
                    topic
                )
            })?;

        info!(
            "successfully published a message: {:?}. schema: {}. topic: {}",
            message.into_inner().message_ids,
            schema_name,
            topic
        );

        Ok(())
    }
}
