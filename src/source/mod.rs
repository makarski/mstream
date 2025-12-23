use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use crate::config::Encoding;
use crate::pubsub::ServiceAccountAuth;
use crate::pubsub::srvc::PubSubSubscriber;
use crate::schema::Schema;
use crate::schema::encoding::SchemaEncoder;
use crate::{kafka::consumer::KafkaConsumer, mongodb::MongoDbChangeStreamListener};

#[async_trait]
pub trait EventSource {
    async fn listen(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct SourceEvent {
    pub raw_bytes: Vec<u8>,
    pub attributes: Option<HashMap<String, String>>,
    pub encoding: Encoding,
    pub is_framed_batch: bool,
}

#[cfg(test)]
impl Default for SourceEvent {
    fn default() -> Self {
        Self {
            raw_bytes: Vec::new(),
            attributes: None,
            encoding: Encoding::Json,
            is_framed_batch: false,
        }
    }
}

impl SourceEvent {
    pub fn apply_schema(
        self,
        target_encoding: Option<&Encoding>,
        schema: &Schema,
    ) -> anyhow::Result<Self> {
        let target_encoding = target_encoding.unwrap_or(&self.encoding);

        SchemaEncoder::new(&self.encoding, target_encoding, schema)
            .apply(self.raw_bytes, self.is_framed_batch)
            .map(|b| SourceEvent {
                raw_bytes: b,
                attributes: self.attributes,
                encoding: target_encoding.clone(),
                is_framed_batch: self.is_framed_batch,
            })
    }
}

pub enum SourceProvider {
    MongoDb(MongoDbChangeStreamListener),
    Kafka(KafkaConsumer),
    PubSub(PubSubSubscriber<ServiceAccountAuth>),
}

#[async_trait]
impl EventSource for SourceProvider {
    async fn listen(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()> {
        match self {
            SourceProvider::MongoDb(cs_listener) => cs_listener.listen(events).await,
            SourceProvider::Kafka(consumer) => consumer.listen(events).await,
            SourceProvider::PubSub(subscriber) => subscriber.subscribe(events).await,
        }
    }
}
