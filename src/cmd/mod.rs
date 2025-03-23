mod event_handler;
mod services;

use anyhow::bail;
use log::{error, info};
use services::ServiceFactory;
use tokio::sync::mpsc::Sender;

use crate::cmd::event_handler::EventHandler;
use crate::config::{Config, ServiceConfigReference};
use crate::source::{EventSource, SourceEvent};

/// Initializes and starts the event listeners for all the connectors
pub async fn listen_streams(done_ch: Sender<String>, cfg: Config) -> anyhow::Result<()> {
    let service_container = ServiceFactory::new(&cfg).await?;

    for connector_cfg in cfg.connectors.iter().cloned() {
        let done_ch = done_ch.clone();

        // todo: avoid passing db, as it is only needed for mongo schema provider
        let mut schema_service = service_container
            .schema_provider(&connector_cfg.schema)
            .await?;

        let mut publishers = Vec::new();
        for topic_cfg in connector_cfg.sinks.into_iter() {
            let publisher = service_container.publisher_service(&topic_cfg).await?;
            publishers.push((topic_cfg, publisher));
        }

        let (events_tx, events_rx) = tokio::sync::mpsc::channel(1);
        if let Err(err) = spawn_source_listener(
            connector_cfg.name.clone(),
            &service_container,
            &connector_cfg.source,
            events_tx,
        )
        .await
        {
            bail!("failed to spawn source listener: {}", err);
        }

        tokio::spawn(async move {
            let cnt_name = connector_cfg.name.clone();
            // todo: change singnature to future?

            let mut event_handler = EventHandler {
                connector_name: connector_cfg.name.clone(),
                schema_name: connector_cfg.schema.id.clone(),
                publishers,
                schema_provider: &mut schema_service,
            };

            if let Err(err) = event_handler.listen(events_rx).await {
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

async fn spawn_source_listener(
    cnt_name: String,
    service_container: &ServiceFactory<'_>,
    source_cfg: &ServiceConfigReference,
    events_tx: Sender<SourceEvent>,
) -> anyhow::Result<()> {
    let mut source_provider = service_container.source_provider(source_cfg).await?;

    tokio::spawn(async move {
        info!("spawning a listener for connector: {}", cnt_name);
        if let Err(err) = source_provider.listen(events_tx).await {
            error!("source listener failed. connector: {}:{}", cnt_name, err)
        }
    });
    Ok(())
}
