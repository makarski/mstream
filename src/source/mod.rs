use std::collections::HashMap;

use async_trait::async_trait;
use mongodb::bson::Document;
use tokio::sync::mpsc::Sender;

use crate::config::Encoding;
use crate::{kafka::consumer::KafkaConsumer, mongodb::MongoDbChangeStreamListener};

#[async_trait]
pub trait EventSource {
    async fn listen(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct SourceEvent {
    pub raw_bytes: Option<Vec<u8>>,
    pub document: Option<Document>,
    pub attributes: Option<HashMap<String, String>>,
    pub encoding: Encoding,
}

pub enum SourceProvider {
    MongoDb(MongoDbChangeStreamListener),
    Kafka(KafkaConsumer),
}

#[async_trait]
impl EventSource for SourceProvider {
    async fn listen(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()> {
        match self {
            SourceProvider::MongoDb(cs_listener) => cs_listener.listen(events).await,
            SourceProvider::Kafka(consumer) => consumer.listen(events).await,
        }
    }
}
