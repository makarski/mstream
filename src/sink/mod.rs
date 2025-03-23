use anyhow::anyhow;
use async_trait::async_trait;
use encoding::SinkEvent;

use crate::{
    kafka::producer::KafkaProducer,
    mongodb::persister::MongoDbPersister,
    pubsub::{srvc::PubSubPublisher, ServiceAccountAuth},
};

pub mod encoding;

#[async_trait]
pub trait EventSink {
    async fn publish(
        &mut self,
        sink_event: SinkEvent,
        topic: String,
        key: Option<&str>,
    ) -> anyhow::Result<String>;
}

pub enum SinkProvider {
    Kafka(KafkaProducer),
    PubSub(PubSubPublisher<ServiceAccountAuth>),
    MongoDb(MongoDbPersister),
}

#[async_trait]
impl EventSink for SinkProvider {
    async fn publish(
        &mut self,
        sink_event: SinkEvent,
        topic: String,
        key: Option<&str>,
    ) -> anyhow::Result<String> {
        match self {
            SinkProvider::Kafka(p) => match sink_event.raw_bytes {
                Some(b) => p.publish(topic, b, key, sink_event.attributes).await,
                None => Err(anyhow::anyhow!(
                    "raw_bytes is missing for kafka producer. topic: {}",
                    topic
                )),
            },
            SinkProvider::PubSub(p) => match sink_event.raw_bytes {
                Some(b) => p.publish(topic, b, key, sink_event.attributes).await,
                None => Err(anyhow::anyhow!(
                    "raw_bytes is missing for pubsub producer. topic: {}",
                    topic
                )),
            },
            SinkProvider::MongoDb(p) => match sink_event.bson_doc {
                Some(doc) => p
                    .persist(doc, &topic)
                    .await
                    .map_err(|err| anyhow!("failed to persist to collection: {}. {}", topic, err)),
                None => Err(anyhow::anyhow!(
                    "bson_doc is missing for mongodb persister. collection: {}",
                    topic
                )),
            },
        }
    }
}
