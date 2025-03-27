use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;
use log::{debug, info};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::{ClientConfig, Message};
use tokio::sync::mpsc::Sender;

use crate::config::Encoding;
use crate::source::{EventSource, SourceEvent};

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    topic: String,
    encoding: Encoding,
}

impl KafkaConsumer {
    pub fn new(
        configs: &HashMap<String, String>,
        topic: String,
        encoding: Encoding,
    ) -> anyhow::Result<Self> {
        debug!("kafka consumer configs: {:?}", configs);
        let mut cfg = ClientConfig::new();

        configs.iter().for_each(|(k, v)| {
            cfg.set(k, v);
        });

        let consumer: StreamConsumer = cfg.create().context("kafka consumer creation failed")?;
        Ok(Self {
            consumer,
            topic,
            encoding,
        })
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

            events
                .send(SourceEvent {
                    raw_bytes: Some(payload.to_vec()),
                    document: None,
                    attributes: None,
                    encoding: self.encoding.clone(),
                })
                .await?;
        }
    }
}
