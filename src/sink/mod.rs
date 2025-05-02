use anyhow::anyhow;
use async_trait::async_trait;
use encoding::SinkEvent;

use crate::{
    http::HttpService,
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
    Http(HttpService),
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
            SinkProvider::Kafka(p) => {
                p.publish(topic, sink_event.raw_bytes, key, sink_event.attributes)
                    .await
            }
            SinkProvider::PubSub(p) => {
                p.publish(topic, sink_event.raw_bytes, key, sink_event.attributes)
                    .await
            }
            SinkProvider::MongoDb(p) => p
                .persist2(sink_event.raw_bytes, &topic)
                .await
                .map_err(|err| anyhow!("failed to persist to collection: {}. {}", topic, err)),

            // match sink_event.bson_doc {
            //     Some(doc) => p
            //         .persist(doc, &topic)
            //         .await
            //         .map_err(|err| anyhow!("failed to persist to collection: {}. {}", topic, err)),
            //     None => Err(anyhow::anyhow!(
            //         "bson_doc is missing for mongodb persister. collection: {}",
            //         topic
            //     )),
            // },
            SinkProvider::Http(p) => {
                p.post(
                    &topic,
                    sink_event.raw_bytes,
                    sink_event.encoding,
                    sink_event.attributes,
                )
                .await
            }
        }
    }
}
