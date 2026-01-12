use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use log::{debug, error, info};
use rdkafka::consumer::{Consumer, stream_consumer::StreamConsumer};
use rdkafka::topic_partition_list::TopicPartitionListElem;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use tokio::sync::mpsc::Sender;

use crate::checkpoint::Checkpoint;
use crate::config::Encoding;
use crate::kafka::KafkaOffset;
use crate::source::{EventSource, SourceEvent};

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    topic: String,
    encoding: Encoding,
    start_timestamp_ms: Option<u128>,
    checkpoint: Option<KafkaOffset>,
}

impl KafkaConsumer {
    /// Creates a new Kafka consumer instance.
    ///
    /// # Arguments
    ///
    /// * `configs` - A map of Kafka client configuration properties.
    /// * `topic` - The name of the topic to consume from.
    /// * `encoding` - The expected encoding of the messages.
    /// * `seek_back_secs` - Optional number of seconds to seek back from the current time.
    pub fn new(
        configs: &HashMap<String, String>,
        topic: String,
        encoding: Encoding,
        seek_back_secs: Option<u64>,
        checkpoint: Option<Checkpoint>,
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

        let kafka_checkpoint =
            checkpoint.and_then(|cp| mongodb::bson::from_slice::<KafkaOffset>(&cp.cursor).ok());

        Ok(Self {
            consumer,
            topic,
            encoding,
            start_timestamp_ms: start_timestamp,
            checkpoint: kafka_checkpoint,
        })
    }

    fn seek_to_checkpoint(&self) -> anyhow::Result<bool> {
        let checkpoint = match &self.checkpoint {
            Some(cp) => cp,
            None => return Ok(false),
        };

        if checkpoint.topic != self.topic {
            bail!(
                "checkpoint topic mismatch: expected {}, got {}",
                self.topic,
                checkpoint.topic
            );
        }

        info!(
            "seeking to checkpoint: topic={}, partition={}, offset={}",
            checkpoint.topic, checkpoint.partition, checkpoint.offset
        );

        let mut tpl = TopicPartitionList::new();

        // Seek to offset + 1 to resume after the last processed message
        tpl.add_partition_offset(
            &checkpoint.topic,
            checkpoint.partition,
            Offset::Offset(checkpoint.offset + 1),
        )?;

        self.consumer.assign(&tpl)?;

        info!(
            "assigned to checkpoint offset: topic={}, partition={}, offset={}",
            checkpoint.topic,
            checkpoint.partition,
            checkpoint.offset + 1
        );

        Ok(true)
    }

    fn seek_to_timestamp(&self) -> anyhow::Result<()> {
        let start_ts = match self.start_timestamp_ms {
            Some(ts) => ts,
            None => {
                info!("no start timestamp provided.");
                return Ok(());
            }
        };

        info!("seeking to timestamp: {}. {}", start_ts, self.topic);

        let timeout = Duration::from_secs(10);
        let offsets_for_times = self.fetch_offsets_for_timestamp(start_ts, timeout)?;
        let seek_tpl = self.build_seek_partition_list(&offsets_for_times, timeout)?;

        if !seek_tpl.elements().is_empty() {
            debug!("assigning topic partitions: {:?}", seek_tpl);
            self.consumer
                .assign(&seek_tpl)
                .context("failed to assign topic partitions")?;
        }

        debug!(
            "topic: {}. seek to timestamp: {}. partitions: {:?}",
            self.topic,
            start_ts,
            seek_tpl.elements().len(),
        );

        Ok(())
    }

    fn fetch_offsets_for_timestamp(
        &self,
        start_ts: u128,
        timeout: Duration,
    ) -> anyhow::Result<TopicPartitionList> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), timeout)
            .with_context(|| anyhow!("failed to fetch topic metadata: {}", self.topic))?;

        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic)
            .ok_or_else(|| anyhow!("topic not found: {}", self.topic))?;

        let mut topic_partitions = TopicPartitionList::new();
        let offset = Offset::Offset(start_ts as i64);

        for partition in topic_metadata.partitions() {
            topic_partitions.add_partition_offset(&self.topic, partition.id(), offset)?;
        }

        self.consumer
            .offsets_for_times(topic_partitions, timeout)
            .context("failed to get offsets for times")
    }

    fn build_seek_partition_list(
        &self,
        offsets_for_times: &TopicPartitionList,
        timeout: Duration,
    ) -> anyhow::Result<TopicPartitionList> {
        let mut seek_tpl = TopicPartitionList::new();

        for partition in offsets_for_times.elements() {
            let resolved_offset = self.resolve_partition_offset(&partition, timeout)?;
            seek_tpl.add_partition_offset(
                partition.topic(),
                partition.partition(),
                Offset::Offset(resolved_offset),
            )?;

            debug!(
                "topic: {}. partition: {}. resolved offset: {}",
                partition.topic(),
                partition.partition(),
                resolved_offset
            );
        }

        Ok(seek_tpl)
    }

    fn resolve_partition_offset(
        &self,
        partition: &TopicPartitionListElem<'_>,
        timeout: Duration,
    ) -> anyhow::Result<i64> {
        match partition.offset() {
            Offset::Offset(offset) if offset > 0 => Ok(offset),
            Offset::Offset(_) => {
                // Kafka returns -1001 for no offset, fall back to high watermark
                let (_, high) = self.consumer.fetch_watermarks(
                    partition.topic(),
                    partition.partition(),
                    timeout,
                )?;
                Ok(high)
            }
            _ => bail!(
                "invalid offset type for partition {}",
                partition.partition()
            ),
        }
    }
}

#[async_trait]
impl EventSource for KafkaConsumer {
    async fn listen(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()> {
        // Priority: timestamp config > checkpoint > default subscribe
        // If user explicitly sets offset_seek_back_seconds, they want to reprocess from that time
        if self.start_timestamp_ms.is_some() {
            // seek_to_timestamp uses assign() - don't call subscribe() as they are mutually exclusive
            self.seek_to_timestamp()
                .context("failed to seek to timestamp")?;
            info!(
                "assigned to topic partitions via timestamp seek: {}",
                self.topic
            );
        } else {
            let used_checkpoint = self
                .seek_to_checkpoint()
                .context("failed to seek to checkpoint")?;

            if !used_checkpoint {
                self.consumer.subscribe(&[&self.topic])?;
                info!(
                    "subscribed to topic: {} (no checkpoint, no timestamp)",
                    self.topic
                );
            }
        }

        loop {
            match self.consumer.recv().await {
                Ok(msg) => {
                    info!("received message from kafka topic: {}", msg.topic());
                    let payload = msg.payload().context("failed to get payload")?;

                    let sender = events.clone();
                    let payload_vec = payload.to_vec();
                    let encoding = self.encoding.clone();

                    let offset = KafkaOffset {
                        topic: msg.topic().to_string(),
                        partition: msg.partition(),
                        offset: msg.offset(),
                    };

                    let cursor = offset.try_into()?;

                    let attr = HashMap::from([
                        ("origin".to_string(), "kafka".to_string()),
                        ("topic".to_string(), msg.topic().to_string()),
                        ("partition".to_string(), msg.partition().to_string()),
                        ("offset".to_string(), msg.offset().to_string()),
                    ]);

                    tokio::spawn(async move {
                        if let Err(err) = sender
                            .send(SourceEvent {
                                raw_bytes: payload_vec,
                                attributes: Some(attr),
                                encoding,
                                is_framed_batch: false,
                                cursor: Some(cursor),
                            })
                            .await
                        {
                            error!("failed to send event to channel: {}", err);
                        }
                    });
                }
                Err(err) => {
                    error!("failed to receive message from kafka topic: {}", err);
                }
            }
        }
    }
}
