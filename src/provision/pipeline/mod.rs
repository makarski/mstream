use std::sync::Arc;

use crate::{
    config::Connector,
    provision::{
        pipeline::{
            middleware::{MiddlewareBuilder, MiddlewareDefinition},
            schema::{SchemaBuilder, SchemaDefinition},
            sink::{SinkBuilder, SinkDefinition},
            source::{SourceBuilder, SourceDefinition},
        },
        registry::ServiceRegistry,
    },
    schema::Schema,
};

pub mod middleware;
pub mod schema;
pub mod sink;
pub mod source;

#[derive(Default)]
pub struct Pipeline {
    pub source: Option<SourceDefinition>,
    pub source_schema: Schema,
    pub middlewares: Vec<MiddlewareDefinition>,
    pub schemas: Vec<SchemaDefinition>,
    pub sinks: Vec<SinkDefinition>,
    pub batch_size: usize,
    pub is_batching_enabled: bool,
}

pub struct PipelineBuilder {
    source_builder: SourceBuilder,
    schema_builder: SchemaBuilder,
    middleware_builder: MiddlewareBuilder,
    sink_builder: SinkBuilder,
    pipeline: Pipeline,
}

impl PipelineBuilder {
    pub fn new(registry: Arc<ServiceRegistry>, connector: Connector) -> Self {
        let (batch_size, is_batching_enabled) = connector.batch_config();
        let pipeline = Pipeline {
            batch_size,
            is_batching_enabled,
            ..Default::default()
        };

        Self {
            pipeline: pipeline,
            source_builder: SourceBuilder::new(registry.clone(), connector.source.clone()),
            schema_builder: SchemaBuilder::new(registry.clone(), &connector.schemas),
            middleware_builder: MiddlewareBuilder::new(registry.clone(), &connector.middlewares),
            sink_builder: SinkBuilder::new(registry.clone(), connector.sinks.clone()),
        }
    }

    pub async fn build(mut self) -> anyhow::Result<Pipeline> {
        self.init_schemas().await?;
        self.init_middlewares().await?;
        self.init_source().await?;
        self.init_sinks().await?;

        Ok(self.pipeline)
    }

    async fn init_schemas(&mut self) -> anyhow::Result<()> {
        let schemas = self.schema_builder.build().await?;
        self.pipeline.schemas = schemas;
        Ok(())
    }

    async fn init_source(&mut self) -> anyhow::Result<()> {
        let source = self.source_builder.build(&self.pipeline.schemas).await?;
        self.pipeline.source = Some(source);
        Ok(())
    }

    async fn init_middlewares(&mut self) -> anyhow::Result<()> {
        let middlewares = self
            .middleware_builder
            .build(&self.pipeline.schemas)
            .await?;

        self.pipeline.middlewares = middlewares;
        Ok(())
    }

    async fn init_sinks(&mut self) -> anyhow::Result<()> {
        let sinks = self.sink_builder.build(&self.pipeline.schemas).await?;
        self.pipeline.sinks = sinks;
        Ok(())
    }
}

pub fn find_schema(schema_id: Option<String>, schema_definitions: &[SchemaDefinition]) -> Schema {
    let has_schemas = !schema_definitions.is_empty();
    if !has_schemas || schema_id.is_none() {
        return Schema::Undefined;
    }
    let schema_id = schema_id.unwrap();

    schema_definitions
        .into_iter()
        .find(|&schema| schema.schema_id == schema_id)
        .map(|s| s.schema.clone())
        .unwrap_or_else(|| Schema::Undefined)
}
