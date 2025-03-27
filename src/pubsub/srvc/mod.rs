use std::collections::HashMap;

use anyhow::{anyhow, Context};
use apache_avro::Schema;
use async_trait::async_trait;
use log::debug;
use tokio::sync::mpsc::Sender;
use tonic::service::Interceptor;

use super::api::subscriber_client::SubscriberClient;
use super::api::StreamingPullRequest;
use super::{tls_transport, Channel, InterceptedService};
use crate::config::Encoding;
use crate::pubsub::api::publisher_client::PublisherClient;
use crate::pubsub::api::schema_service_client::SchemaServiceClient;
use crate::pubsub::api::{GetSchemaRequest, ListSchemasRequest, ListSchemasResponse};
use crate::pubsub::api::{PublishRequest, PubsubMessage};
use crate::schema::SchemaRegistry;
use crate::source::SourceEvent;

pub struct PubSubPublisher<I> {
    client: PublisherClient<InterceptedService<Channel, I>>,
}

impl<I: Interceptor> PubSubPublisher<I> {
    pub async fn with_interceptor(interceptor: I) -> anyhow::Result<Self> {
        let channel = tls_transport()
            .await
            .with_context(|| "failed to create tls transport for pubsub publisher")?;
        Ok(Self {
            client: PublisherClient::with_interceptor(channel, interceptor),
        })
    }

    pub async fn publish(
        &mut self,
        topic: String,
        b: Vec<u8>,
        key: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    ) -> anyhow::Result<String> {
        let mut msg = PubsubMessage {
            data: b,
            ..Default::default()
        };

        if let Some(k) = key {
            k.clone_into(&mut msg.ordering_key)
        }

        if let Some(attr) = attributes {
            msg.attributes = attr;
        }

        let req = PublishRequest {
            topic: topic.clone(),
            messages: vec![msg],
        };

        let msg = self
            .client
            .publish(req)
            .await
            .map_err(|err| anyhow!("{}. topic: {}", err.message(), &topic))?;

        let msg = msg.into_inner().message_ids[0].clone();
        Ok(msg)
    }
}

pub struct SchemaService<I> {
    client: SchemaServiceClient<InterceptedService<Channel, I>>,
    cache: HashMap<String, Schema>,
}

impl<I: Interceptor> SchemaService<I> {
    pub async fn with_interceptor(interceptor: I) -> anyhow::Result<Self> {
        let channel = tls_transport()
            .await
            .with_context(|| "failed to create tls transport for pubsub schema service")?;

        let client = SchemaServiceClient::with_interceptor(channel, interceptor);

        Ok(Self {
            client,
            cache: HashMap::new(),
        })
    }

    pub async fn list_schemas(&mut self, parent: String) -> anyhow::Result<ListSchemasResponse> {
        let schema_list_response = self
            .client
            .list_schemas(ListSchemasRequest {
                parent,
                ..Default::default()
            })
            .await?;

        Ok(schema_list_response.into_inner())
    }
}

#[async_trait]
impl<I: Interceptor + Send> SchemaRegistry for SchemaService<I> {
    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema> {
        if !self.cache.contains_key(&id) {
            let schema_response = self.client.get_schema(GetSchemaRequest {
                name: id.clone(),
                ..Default::default()
            });

            let pubsub_schema = schema_response.await?.into_inner();
            let avro_schema = Schema::parse_str(&pubsub_schema.definition)?;
            self.cache.insert(id.clone(), avro_schema);

            log::info!("schema {} added to cache", id);
        } else {
            log::info!("schema {} found in cache", id);
        }

        self.cache
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("schema not found"))
    }
}

pub struct PubSubSubscriber<I> {
    subscription: String,
    client: SubscriberClient<InterceptedService<Channel, I>>,
    encoding: Encoding,
}

impl<I: Interceptor> PubSubSubscriber<I> {
    pub async fn new(
        interceptor: I,
        subscription: String,
        encoding: Encoding,
    ) -> anyhow::Result<Self> {
        let channel = tls_transport()
            .await
            .with_context(|| "failed to create tls transport for pubsub subscriber")?;

        let client = SubscriberClient::with_interceptor(channel, interceptor);

        Ok(Self {
            subscription,
            client,
            encoding,
        })
    }

    pub async fn subscribe(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel::<StreamingPullRequest>(1);

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        let tx_clone = tx.clone();
        let subscription = self.subscription.clone();
        let init_task = tokio::spawn(async move {
            let streaming_pr = StreamingPullRequest {
                subscription,
                stream_ack_deadline_seconds: 20,
                ..Default::default()
            };

            match tx_clone.send(streaming_pr).await {
                Ok(_) => log::info!("subscription request sent"),
                Err(err) => log::error!("failed to send subscription request: {}", err),
            }
        });

        // start the streaming pull
        init_task.await?;

        let mut response_stream = self.client.streaming_pull(stream).await?.into_inner();
        while let Some(msg) = response_stream.message().await? {
            debug!("received message: {:?}", msg);

            let mut ack_ids = Vec::new();
            for m in msg.received_messages {
                debug!("message: {:?}", m);
                ack_ids.push(m.ack_id.clone());
                if let Some(payload) = m.message {
                    log::info!("received pubsub message: {:?}", payload);
                    let source_event = SourceEvent {
                        raw_bytes: Some(payload.data),
                        document: None,
                        attributes: Some(payload.attributes),
                        encoding: self.encoding.clone(),
                    };
                    events.send(source_event).await?;
                }
            }

            log::info!("acknowledging messages: {:?}", ack_ids);

            if !ack_ids.is_empty() {
                let count = ack_ids.len();
                let ack_req = StreamingPullRequest {
                    ack_ids,
                    ..Default::default()
                };

                match tx.send(ack_req).await {
                    Ok(_) => log::info!("acknowledged messages. count: {}", count),
                    Err(err) => log::error!("failed to acknowledge messages: {}", err),
                }
            }
        }

        Ok(())
    }
}
