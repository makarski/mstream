#![allow(dead_code)]

use std::vec;

use anyhow::anyhow;
use config::Config;
use log::info;
use mongodb::bson::{doc, Document};

mod cmd;
mod config;
mod db;
mod encoding;
mod pubsub;
mod registry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::try_init()?;

    let config = config::Config::load("config.toml")?;
    info!("config: {:?}", config);

    if cfg!(feature = "listen") || cfg!(feature = "subscribe") {
        let agrs: Vec<String> = std::env::args().collect();
        info!("cli args: {:?}", agrs);

        let access_token = agrs
            .get(1)
            .ok_or_else(|| anyhow!("access token not provided"))?;

        #[cfg(feature = "listen")]
        {
            cmd::listener::listen(config.clone(), access_token.to_string()).await?;
            match tokio::signal::ctrl_c().await {
                Ok(()) => {}
                Err(err) => log::error!("unable to listen to shutdown signal: {}", err),
            }
        }

        #[cfg(feature = "subscribe")]
        pull_from_pubsub(config, access_token).await?;
    }

    #[cfg(feature = "persist")]
    {
        for connector in config.connectors {
            let client = db::db_client(connector.name, &connector.db_connection).await?;
            info!("created main db client");

            for dbname in client.list_database_names(None, None).await? {
                info!("{}", dbname);
            }

            info!("persisting to: {}", &connector.db_name);

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

        info!(
            "obtaining messages from subscription: {}",
            connector.subscription,
        );

        let mut subscriber = pubsub::sub::subscriber(access_token).await?;
        let response = subscriber
            .pull(PullRequest {
                subscription: connector.subscription,
                max_messages: 5,
                ..Default::default()
            })
            .await?;

        info!("successfully obtained a subscribe response");

        for message in response.into_inner().received_messages {
            let msg = message
                .message
                .ok_or_else(|| anyhow!("failed to unwrap response message"))?;

            let payload = hex::decode(msg.data)?;
            let avr_schema = avro_rs::Schema::parse_str(&schema.definition)?;
            let reader = avro_rs::Reader::with_schema(&avr_schema, payload.as_slice())?;

            for value in reader {
                info!("> obtained message: {:?}", value?);
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
        info!("obtaining schema: {}", &schema.name);

        let schema = schema_registry.get_schema(schema.name).await.unwrap();

        info!("schema obtained: {:?}", schema);
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
        Err(err) => Err(err),
    }
}
