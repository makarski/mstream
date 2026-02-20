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
    pub cursor: Option<Vec<u8>>,
    /// Millis since epoch when the event was produced at the source.
    /// Used to compute current_lag_seconds at read time.
    pub source_timestamp: Option<i64>,
}

#[cfg(test)]
impl Default for SourceEvent {
    fn default() -> Self {
        Self {
            raw_bytes: Vec::new(),
            attributes: None,
            encoding: Encoding::Json,
            is_framed_batch: false,
            cursor: None,
            source_timestamp: None,
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
                cursor: self.cursor,
                source_timestamp: self.source_timestamp,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_source_timestamp_is_none() {
        let event = SourceEvent::default();
        assert!(event.source_timestamp.is_none());
    }

    #[test]
    fn apply_schema_preserves_source_timestamp() {
        let ts = Some(1_700_000_000_000i64);
        let event = SourceEvent {
            raw_bytes: b"{}".to_vec(),
            encoding: Encoding::Json,
            source_timestamp: ts,
            ..Default::default()
        };

        let result = event.apply_schema(None, &Schema::Undefined).unwrap();
        assert_eq!(result.source_timestamp, ts);
    }

    #[test]
    fn apply_schema_preserves_none_source_timestamp() {
        let event = SourceEvent {
            raw_bytes: b"{}".to_vec(),
            encoding: Encoding::Json,
            ..Default::default()
        };

        let result = event.apply_schema(None, &Schema::Undefined).unwrap();
        assert!(result.source_timestamp.is_none());
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
