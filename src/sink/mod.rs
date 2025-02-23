use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    kafka::producer::KafkaProducer,
    pubsub::{srvc::PubSubPublisher, ServiceAccountAuth},
};

#[async_trait]
pub trait EventSink {
    async fn publish(
        &mut self,
        topic: String,
        b: Vec<u8>,
        key: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    ) -> anyhow::Result<String>;
}

pub enum SinkProvider {
    Kafka(KafkaProducer),
    PubSub(PubSubPublisher<ServiceAccountAuth>),
}

#[async_trait]
impl EventSink for SinkProvider {
    async fn publish(
        &mut self,
        topic: String,
        b: Vec<u8>,
        key: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    ) -> anyhow::Result<String> {
        match self {
            SinkProvider::Kafka(p) => p.publish(topic, b, key, attributes).await,
            SinkProvider::PubSub(p) => p.publish(topic, b, key, attributes).await,
        }
    }
}
