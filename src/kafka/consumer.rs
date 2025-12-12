use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use log::{debug, error, info};
use rdkafka::consumer::{stream_consumer::StreamConsumer, Consumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use tokio::sync::mpsc::Sender;

use crate::config::Encoding;
use crate::source::{EventSource, SourceEvent};

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    topic: String,
    encoding: Encoding,
    start_timestamp_ms: Option<u128>,
}

impl KafkaConsumer {
    pub fn new(
        configs: &HashMap<String, String>,
        topic: String,
        encoding: Encoding,
        seek_back_secs: Option<u64>,
    ) -> anyhow::Result<Self> {
        debug!("kafka consumer configs: {:?}", configs);
        let mut cfg = ClientConfig::new();

        configs.iter().for_each(|(k, v)| {
            cfg.set(k, v);
        });

        let consumer: StreamConsumer = cfg.create().context("kafka consumer creation failed")?;

        let start_timestamp = match seek_back_secs {
            Some(seconds) => {
                let duration = Duration::from_secs(seconds);
                let start_time = SystemTime::now() - duration;
                let start_timestamp = start_time
                    .duration_since(UNIX_EPOCH)
                    .context("failed to get duration since epoch")?
                    .as_millis();
                Some(start_timestamp)
            }
            None => None,
        };

        Ok(Self {
            consumer,
            topic,
            encoding,
            start_timestamp_ms: start_timestamp,
        })
    }

    fn seek_to_timestamp(&self) -> anyhow::Result<()> {
        if let Some(start_ts) = self.start_timestamp_ms {
            info!("seeking to timestamp: {}. {}", start_ts, self.topic);

            let timeout = Duration::from_secs(10);
            let metadata = self
                .consumer
                .fetch_metadata(Some(&self.topic), timeout)
                .with_context(|| anyhow!("failed to fetch topic metadata: {}", self.topic))?;

            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|t| t.name() == self.topic)
                .ok_or_else(|| anyhow!("topic not found: {}", self.topic))?;

            let partitions = topic_metadata.partitions();
            let mut topic_partitions = TopicPartitionList::new();

            let offset = Offset::Offset(start_ts as i64);
            for partition in partitions {
                let partition_id = partition.id();
                topic_partitions.add_partition_offset(&self.topic, partition_id, offset)?;
            }

            let offsets_for_times = self.consumer.offsets_for_times(topic_partitions, timeout)?;
            let mut seek_tpl = TopicPartitionList::new();

            for partition in offsets_for_times.elements() {
                if let Offset::Offset(offset) = partition.offset() {
                    if offset > 0 {
                        seek_tpl.add_partition_offset(
                            partition.topic(),
                            partition.partition(),
                            Offset::Offset(offset),
                        )?;

                        debug!(
                            "topic: {}. added offset for partition: {}. offset: {}",
                            partition.topic(),
                            partition.partition(),
                            offset
                        );
                    } else {
                        // handle a case when no valid offset is returned
                        // kafka returns -1001 for no offset
                        // so we need to get the high watermark, ie latest offset
                        let watermarks = self.consumer.fetch_watermarks(
                            partition.topic(),
                            partition.partition(),
                            timeout,
                        )?;

                        seek_tpl.add_partition_offset(
                            partition.topic(),
                            partition.partition(),
                            Offset::Offset(watermarks.1), // high watermark
                        )?;

                        debug!(
                            "topic: {}. added high watermark for partition: {}. offset: {}",
                            partition.topic(),
                            partition.partition(),
                            watermarks.1
                        );
                    }
                } else {
                    bail!("invalid offset type");
                }
            }

            debug!(
                "topic: {}. seek to timestamp: {}. partitions: {:?}",
                self.topic,
                start_ts,
                seek_tpl.elements().len(),
            );

            if !seek_tpl.elements().is_empty() {
                debug!("assigning topic partitions: {:?}", seek_tpl);

                self.consumer
                    .assign(&seek_tpl)
                    .context("failed to assign topic partitions")?;
            }
        } else {
            info!("no start timestamp provided.");
        }

        Ok(())
    }
}

#[async_trait]
impl EventSource for KafkaConsumer {
    async fn listen(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()> {
        self.seek_to_timestamp()
            .context("failed to seek to timestamp")?;

        self.consumer.subscribe(&[&self.topic])?;
        info!("subscribed to topic: {}", self.topic);

        loop {
            match self.consumer.recv().await {
                Ok(msg) => {
                    info!("received message from kafka topic: {}", msg.topic());
                    let payload = msg.payload().context("failed to get payload")?;
                    let payload_vec = payload.to_vec();

                    if let Err(err) = events
                        .send(SourceEvent {
                            raw_bytes: payload_vec,
                            attributes: None,
                            encoding: self.encoding.clone(),
                            is_framed_batch: false,
                        })
                        .await
                    {
                        error!("failed to send event to channel: {}", err);
                    }
                }

                Err(err) => {
                    error!("failed to receive message from kafka topic: {}", err);
                }
            }
        }
    }
}
