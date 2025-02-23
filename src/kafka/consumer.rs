use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;
use log::{debug, info};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::{ClientConfig, Message};
use tokio::sync::mpsc::Sender;

use crate::encoding::json_to_bson_doc;
use crate::source::{EventSource, SourceEvent};

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    topic: String,
}

impl KafkaConsumer {
    pub fn new(configs: &HashMap<String, String>, topic: String) -> anyhow::Result<Self> {
        debug!("kafka consumer configs: {:?}", configs);
        let mut cfg = ClientConfig::new();

        configs.iter().for_each(|(k, v)| {
            cfg.set(k, v);
        });

        let consumer: StreamConsumer = cfg.create().context("kafka consumer creation failed")?;
        Ok(Self { consumer, topic })
    }
}

#[async_trait]
impl EventSource for KafkaConsumer {
    async fn listen(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()> {
        self.consumer.subscribe(&[&self.topic])?;
        info!("subscribed to topic: {}", self.topic);

        loop {
            let msg = self.consumer.recv().await?;
            info!("received message from kafka topic: {}", msg.topic());
            let payload = msg.payload().context("failed to get payload")?;
            let doc = json_to_bson_doc(payload)?;

            events
                .send(SourceEvent {
                    document: doc,
                    attributes: HashMap::new(),
                })
                .await?;
        }
    }
}
