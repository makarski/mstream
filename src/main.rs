#![allow(dead_code)]

use std::vec;

use anyhow::anyhow;
use config::Config;
use mongodb::bson::{doc, Document};

mod cmd;
mod config;
mod db;
mod encoding;
mod pubsub;
mod registry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = config::Config::load("config.toml")?;
    println!("{:?}", config);

    if cfg!(feature = "listen") || cfg!(feature = "subscribe") {
        let agrs: Vec<String> = std::env::args().collect();
        println!("{:?}", agrs);

        let access_token = &agrs[1];

        #[cfg(feature = "listen")]
        cmd::listener::listen(config.clone(), access_token).await?;

        #[cfg(feature = "subscribe")]
        pull_from_pubsub(config, access_token).await?;
    }

    #[cfg(feature = "persist")]
    {
        for connector in config.connectors {
            let client = db::db_client(connector.name, &connector.db_connection).await?;
            println!("created main db client");

            for dbname in client.list_database_names(None, None).await? {
                println!("{}", dbname);
            }

            println!("persisting to: {}", &connector.db_name);

            // init db and collection
            let db = client.database(&connector.db_name);
            let coll = db.collection::<Document>(&connector.db_collection);

            persist_data(&coll).await?;
        }
    }

    Ok(())
}

async fn pull_from_pubsub(cfg: Config, access_token: &str) -> anyhow::Result<()> {
    use pubsub::api::PullRequest;

    for connector in cfg.connectors {
        // config name
        let schema_name = connector.schema;

        let mut schema_registry = registry::Registry::with_token(access_token).await?;
        let schema = schema_registry.get_schema(schema_name).await?;

        println!("> obtaining messages from subscription");

        let mut subscriber = pubsub::sub::subscriber(access_token).await?;
        let response = subscriber
            .pull(PullRequest {
                subscription: connector.subscription,
                max_messages: 5,
                ..Default::default()
            })
            .await?;

        println!("> successfully obtained a subscribe response");

        for message in response.into_inner().received_messages {
            let msg = message
                .message
                .ok_or_else(|| anyhow!("failed to unwrap response message"))?;

            let payload = hex::decode(msg.data)?;
            let avr_schema = avro_rs::Schema::parse_str(&schema.definition)?;
            let reader = avro_rs::Reader::with_schema(&avr_schema, payload.as_slice())?;

            for value in reader {
                println!("> obtained message: {:?}", value?);
            }
        }
    }

    Ok(())
}

async fn schemas(access_token: &str) {
    // todo: use the proper one
    let project_name = "my-project".to_owned();

    let mut schema_registry = registry::Registry::with_token(access_token).await.unwrap();
    let schemas = schema_registry.list_schemas(project_name).await.unwrap();
    for schema in schemas.schemas {
        println!("> obtaining schema: {}...", &schema.name);

        let schema = schema_registry.get_schema(schema.name).await.unwrap();

        println!("{:?}", schema);
    }
}

async fn persist_data(coll: &mongodb::Collection<Document>) -> Result<(), mongodb::error::Error> {
    let docs = vec![
        doc! {"uuid": uuid::Uuid::new_v4().to_string(), "customerId": "Customer 1", "classification": "NAC", "number": 1, "yes_no": true},
        doc! {"uuid": uuid::Uuid::new_v4().to_string(), "customerId": "Customer 2", "classification": "FAK", "number": 2, "yes_no": true},
        doc! {"uuid": uuid::Uuid::new_v4().to_string(), "customerId": "Customer 3", "classification": "BASKET", "number": 3, "yes_no": false},
    ];

    match coll.insert_many(docs, None).await {
        Ok(_) => Ok(()),
        Err(err) => return Err(err),
    }
}
