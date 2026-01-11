use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::{
    checkpoint::{Checkpoint, NoopCheckpointer},
    config::Connector,
    provision::{
        pipeline::{
            Pipeline,
            middleware::{MiddlewareBuilder, MiddlewareDefinition},
            schema::{SchemaBuilder, SchemaDefinition},
            sink::{SinkBuilder, SinkDefinition},
            source::{SourceBuilder, SourceDefinition},
        },
        registry::ServiceRegistry,
    },
    schema::Schema,
};

#[async_trait]
pub(crate) trait ComponentBuilder {
    type Output;

    async fn build(&self, schemas: &[SchemaDefinition]) -> anyhow::Result<Self::Output>;
    fn service_deps(&self) -> Vec<String>;
}

type ComponentBuilderImpl<T> = Box<dyn ComponentBuilder<Output = T> + Send + Sync>;

pub struct PipelineBuilder {
    registry: Arc<RwLock<ServiceRegistry>>,
    source_builder: ComponentBuilderImpl<SourceDefinition>,
    schema_builder: ComponentBuilderImpl<Vec<SchemaDefinition>>,
    middleware_builder: ComponentBuilderImpl<Vec<MiddlewareDefinition>>,
    sink_builder: ComponentBuilderImpl<Vec<SinkDefinition>>,
    pipeline: Pipeline,
}

impl PipelineBuilder {
    pub fn new(
        registry: Arc<RwLock<ServiceRegistry>>,
        connector: Connector,
        checkpoint: Option<Checkpoint>,
    ) -> Self {
        let (batch_size, is_batching_enabled) = connector.batch_config();
        let with_checkpoints = connector.checkpoint.as_ref().map_or(false, |cp| cp.enabled);

        let pipeline = Pipeline {
            name: connector.name.clone(),
            batch_size,
            is_batching_enabled,
            source_provider: None,
            source_out_encoding: connector.source.output_encoding.clone(),
            source_schema: Schema::default(),
            middlewares: Vec::new(),
            schemas: Vec::new(),
            sinks: Vec::new(),
            checkpointer: Arc::new(NoopCheckpointer::new()),
            with_checkpoints,
        };

        Self {
            registry: registry.clone(),
            pipeline,
            source_builder: Box::new(SourceBuilder::new(
                registry.clone(),
                connector.source.clone(),
                checkpoint,
            )),
            schema_builder: Box::new(SchemaBuilder::new(registry.clone(), &connector.schemas)),
            middleware_builder: Box::new(MiddlewareBuilder::new(
                registry.clone(),
                &connector.middlewares,
            )),
            sink_builder: Box::new(SinkBuilder::new(registry.clone(), connector.sinks.clone())),
        }
    }

    pub async fn build(mut self) -> anyhow::Result<Pipeline> {
        self.init_schemas().await?;
        self.init_middlewares().await?;
        self.init_source().await?;
        self.init_sinks().await?;
        self.init_checkpointer().await;

        Ok(self.pipeline)
    }

    pub fn service_deps(&self) -> Vec<String> {
        let mut names = Vec::new();
        names.extend(self.source_builder.service_deps());
        names.extend(self.schema_builder.service_deps());
        names.extend(self.middleware_builder.service_deps());
        names.extend(self.sink_builder.service_deps());
        names.sort_unstable();
        names.dedup();
        names
    }

    async fn init_checkpointer(&mut self) {
        if self.pipeline.with_checkpoints {
            self.pipeline.checkpointer = self.registry.read().await.checkpointer();
        }
    }

    async fn init_schemas(&mut self) -> anyhow::Result<()> {
        let schemas = self.schema_builder.build(&[]).await?;
        self.pipeline.schemas = schemas;
        Ok(())
    }

    async fn init_source(&mut self) -> anyhow::Result<()> {
        let source = self.source_builder.build(&self.pipeline.schemas).await?;
        self.pipeline.source_provider = Some(source.source_provider);
        self.pipeline.source_schema = source.schema;
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
