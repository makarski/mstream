use crate::config::Config;
use crate::db::db_client;
use crate::encoding::avro::encode2;
use crate::pubsub::api::{PublishRequest, PubsubMessage};
use crate::pubsub::publ::{publisher, PublisherService};
use crate::registry::Registry;

use mongodb::bson::Document;
use mongodb::change_stream::event::ChangeStreamEvent;
use mongodb::change_stream::ChangeStream;
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use mongodb::Client;

pub async fn listen(cfg: Config, access_token: &str) -> anyhow::Result<()> {
    // listening
    println!("{}", "listen...");

    let schema_registry = Registry::with_token(access_token).await.unwrap();
    let publisher = publisher(access_token).await.unwrap();
    let mut event_handler = CdcEventHandler::new(schema_registry, publisher);

    for connector in cfg.connectors {
        // need to spawn this scope in a separate (green) thread
        // todo: check synchronization
        // tokio::spawn(async move {
        let db_client = db_client(connector.name, &connector.db_connection).await?;
        println!("created main db client");

        // todo: move the loop to main thread - switch the task processing to a spawn
        let mut cs = change_stream(db_client, &connector.db_name, &connector.db_collection)
            .await
            .unwrap();

        while cs.is_alive() {
            if let Some(event) = cs.next_if_any().await.unwrap() {
                // tokio::spawn(async move {
                // println!("{:?}", event.full_document.unwrap().get("title"));

                if let Some(mongo_doc) = event.full_document {
                    event_handler
                        .handle_cdc_event(mongo_doc, &connector.schema, &connector.topic)
                        .await;
                }
                // });
            }

            // resume_token = cs.resume_token();
        }
        // });
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

    async fn handle_cdc_event(&mut self, mongo_doc: Document, schema_name: &str, topic: &str) {
        // todo: handle the error message and write to log

        let schema = self
            .schema_registry
            .get_schema(schema_name.to_owned())
            .await
            .unwrap();

        let encoded = encode2(mongo_doc, &schema.definition).unwrap();

        // todo: publish operation type to attributes field
        // check if routing can be done based on attributes on pubsub side

        let hex_encoded = hex::encode(encoded.clone());

        println!("> hex: {}", hex_encoded);

        let hex_vec = hex_encoded.as_bytes().to_vec();
        let message = self
            .publisher
            .publish(PublishRequest {
                topic: topic.to_owned(),
                messages: vec![PubsubMessage {
                    data: hex_vec,
                    ..Default::default()
                }],
            })
            .await
            .expect("failed to publish a message");

        println!(
            "successfully published a message: {:?}",
            message.into_inner().message_ids
        );
    }
}
