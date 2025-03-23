use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use log::debug;
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};

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
        b: Vec<u8>,
        key: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    ) -> anyhow::Result<String> {
        let headers = match attributes {
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
        };

        log::info!("publishing to topic: {}", topic);

        let mut record: FutureRecord<str, Vec<u8>> =
            FutureRecord::to(&topic).payload(&b).headers(headers);

        if let Some(k) = key {
            record = record.key(k);
        }

        let delivery_status = self.producer.send(record, Duration::from_secs(0)).await;
        delivery_status.map_err(|(err, owned_msg)| {
            anyhow!("Failed to deliver message: {:?}: {:?}", err, owned_msg)
        })?;

        Ok("".to_owned())
    }
}
