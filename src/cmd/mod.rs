mod event_handler;
mod services;

use anyhow::{bail, Ok};
use log::{error, info};
use services::ServiceFactory;
use tokio::sync::mpsc::Sender;

use crate::cmd::event_handler::EventHandler;
use crate::config::{Config, SourceServiceConfigReference};
use crate::schema::{Schema, SchemaRegistry};
use crate::source::{EventSource, SourceEvent};

struct SchemaDefinition {
    schema_id: String,
    schema: Schema,
}

/// Initializes and starts the event listeners for all the connectors
pub async fn listen_streams(done_ch: Sender<String>, cfg: Config) -> anyhow::Result<()> {
    let service_container = ServiceFactory::new(&cfg).await?;

    for connector_cfg in cfg.connectors.iter().cloned() {
        let done_ch = done_ch.clone();

        let schemas = match connector_cfg.schemas {
            Some(cfg) => {
                let mut result = Vec::with_capacity(cfg.len());
                for schema_cfg in cfg.into_iter() {
                    let mut schema_service = service_container.schema_provider(&schema_cfg).await?;

                    // wait for the schema to be ready
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                    let schema = schema_service
                        .get_schema(schema_cfg.resource.clone())
                        .await?;

                    result.push(SchemaDefinition {
                        schema_id: schema_cfg.id,
                        schema,
                    });
                }
                Some(result)
            }
            None => None,
        };

        let source_schema = find_schema(connector_cfg.source.schema_id.clone(), schemas.as_ref());

        let mut publishers = Vec::new();
        for topic_cfg in connector_cfg.sinks.into_iter() {
            let publisher = service_container.publisher_service(&topic_cfg).await?;
            let schema = find_schema(topic_cfg.schema_id.clone(), schemas.as_ref());
            publishers.push((topic_cfg, publisher, schema));
        }

        let middlewares = match connector_cfg.middlewares {
            Some(middlewares) => {
                let mut result = Vec::new();
                for middleware_cfg in middlewares.into_iter() {
                    let middleware = service_container
                        .middleware_service(&middleware_cfg)
                        .await?;

                    let schema = find_schema(middleware_cfg.schema_id.clone(), schemas.as_ref());
                    result.push((middleware_cfg, middleware, schema));
                }
                Some(result)
            }
            None => None,
        };

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
            let mut event_handler = EventHandler {
                connector_name: connector_cfg.name.clone(),
                source_schema,
                source_output_encoding: connector_cfg.source.output_encoding,
                publishers,
                middlewares,
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

fn find_schema(
    schema_id: Option<String>,
    schema_definitions: Option<&Vec<SchemaDefinition>>,
) -> Schema {
    match (schema_id, schema_definitions) {
        (Some(id), Some(schemas)) => {
            if let Some(schema) = schemas.into_iter().find(|schema| schema.schema_id == id) {
                schema.schema.clone()
            } else {
                Schema::Undefined
            }
        }
        _ => Schema::Undefined,
    }
}

async fn spawn_source_listener(
    cnt_name: String,
    service_container: &ServiceFactory<'_>,
    source_cfg: &SourceServiceConfigReference,
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
