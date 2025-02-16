mod services;
mod stream_listener;

use log::error;
use services::ServiceFactory;
use tokio::sync::mpsc::Sender;

use crate::cmd::stream_listener::StreamListener;
use crate::config::Config;

/// Listen to mongodb change streams and publish the events to a pubsub or a kafka topic
pub async fn listen_streams(done_ch: Sender<String>, cfg: Config) -> anyhow::Result<()> {
    let service_container = ServiceFactory::new(&cfg);

    for connector_cfg in cfg.connectors.iter().cloned() {
        let done_ch = done_ch.clone();

        let db = service_container.mongo_db(&connector_cfg.source).await?;

        // todo: avoid passing db, as it is only needed for mongo schema provider
        let mut schema_service = service_container
            .schema_provider(&connector_cfg.schema)
            .await?;

        let mut publishers = Vec::new();
        for topic_cfg in connector_cfg.sinks.into_iter() {
            let publisher = service_container.publisher_service(&topic_cfg).await?;
            publishers.push((topic_cfg, publisher));
        }

        tokio::spawn(async move {
            let cnt_name = connector_cfg.name.clone();
            // todo: change singnature to future?

            let mut stream_listener = StreamListener {
                connector_name: connector_cfg.name.clone(),
                schema_name: connector_cfg.schema.id.clone(),
                db_name: db.name().to_owned(),
                db_collection: connector_cfg.source.id.clone(),
                publishers,
                db,
                resume_token: None,
                schema_provider: &mut schema_service,
            };

            if let Err(err) = stream_listener.listen().await {
                error!("{err}")
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
