use std::sync::Arc;

use crate::{
    config::Connector,
    provision::{
        pipeline::{
            middleware::MiddlewareBuilder, schema::SchemaBuilder, sink::SinkBuilder,
            source::SourceBuilder, Pipeline,
        },
        registry::ServiceRegistry,
    },
    schema::Schema,
};

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
            name: connector.name.clone(),
            batch_size,
            is_batching_enabled,
            source: None,
            source_schema: Schema::default(),
            middlewares: Vec::new(),
            schemas: Vec::new(),
            sinks: Vec::new(),
        };

        Self {
            pipeline,
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
