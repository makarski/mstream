use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use futures::future::try_join_all;
use log::debug;
use rdkafka::{
    ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};

use crate::encoding::framed;

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(configs: &HashMap<String, String>) -> anyhow::Result<Self> {
        debug!("Kafka configs: {:?}", configs);
        let mut cfg = ClientConfig::new();
        configs.iter().for_each(|(k, v)| {
            cfg.set(k, v);
        });

        let producer: FutureProducer = cfg.create()?;
        Ok(Self { producer })
    }
}

impl KafkaProducer {
    pub async fn publish(
        &mut self,
        topic: String,
        data: Vec<u8>,
        key: Option<&str>,
        attributes: Option<HashMap<String, String>>,
        is_framed_batch: bool,
    ) -> anyhow::Result<String> {
        let headers = Self::create_headers(attributes);

        let items = if is_framed_batch {
            let (items, _) = framed::decode(&data)?;
            items
        } else {
            vec![data]
        };

        log::info!("publishing {} items to topic: {}", items.len(), topic);

        let mut delivery_futures = Vec::with_capacity(items.len());

        for item in items.iter() {
            let mut record: FutureRecord<str, Vec<u8>> = FutureRecord::to(&topic)
                .payload(item)
                .headers(headers.clone());

            if let Some(k) = key {
                record = record.key(k);
            }

            let df = self.producer.send(record, Duration::from_secs(0));
            delivery_futures.push(df);
        }

        try_join_all(delivery_futures)
            .await
            .map_err(|(e, _)| anyhow!("failed to deliver message: {}", e))?;

        Ok("".to_owned())
    }

    fn create_headers(attributes: Option<HashMap<String, String>>) -> OwnedHeaders {
        match attributes {
            Some(attr) => {
                let mut headers = OwnedHeaders::new();
                for (k, v) in attr.iter() {
                    headers = headers.insert(Header {
                        key: k,
                        value: Some(v),
                    });
                }
                headers
            }
            None => OwnedHeaders::new(),
        }
    }
}
