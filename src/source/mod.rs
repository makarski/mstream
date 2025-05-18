use std::collections::HashMap;

use async_trait::async_trait;
use mongodb::bson::Document;
use tokio::sync::mpsc::Sender;

use crate::config::Encoding;
use crate::pubsub::srvc::PubSubSubscriber;
use crate::pubsub::ServiceAccountAuth;
use crate::schema::encoding::SchemaEncoder;
use crate::schema::Schema;
use crate::{kafka::consumer::KafkaConsumer, mongodb::MongoDbChangeStreamListener};

#[async_trait]
pub trait EventSource {
    async fn listen(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct SourceEvent {
    pub raw_bytes: Vec<u8>,
    pub document: Option<Document>,
    pub attributes: Option<HashMap<String, String>>,
    pub encoding: Encoding,
}

#[derive(Debug)]
pub struct SourceBatch(Vec<SourceEvent>);

impl SourceBatch {
    pub fn new(events: Vec<SourceEvent>) -> Self {
        Self(events)
    }
}

impl Iterator for SourceBatch {
    type Item = SourceEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop()
    }
}

impl SourceBatch {
    pub fn encoding(&self) -> Encoding {
        self.0
            .first()
            .map(|e| e.encoding.clone())
            .unwrap_or_default()
    }

    pub fn attributes(&self) -> Option<&HashMap<String, String>> {
        self.0.first().and_then(|e| e.attributes.as_ref())
    }
}

impl SourceEvent {
    pub fn apply_schema(
        self,
        target_encoding: Option<&Encoding>,
        schema: &Schema,
    ) -> anyhow::Result<Self> {
        let target_encoding = target_encoding.unwrap_or(&self.encoding);
        SchemaEncoder::new_event(self.raw_bytes)
            .apply_schema(&self.encoding, target_encoding, schema)
            .map(|b| SourceEvent {
                raw_bytes: b,
                document: self.document,
                attributes: self.attributes,
                encoding: target_encoding.clone(),
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
