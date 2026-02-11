use std::collections::HashMap;

use anyhow::{Context, anyhow, bail};
use tokio::sync::mpsc::Sender;
use tonic::service::Interceptor;

use super::api::StreamingPullRequest;
use super::api::subscriber_client::SubscriberClient;
use super::{Channel, InterceptedService, tls_transport};
use crate::config::Encoding;
use crate::encoding::framed;
use crate::pubsub::api::publisher_client::PublisherClient;
use crate::pubsub::api::schema::Type as PubSubSchemaType;
use crate::pubsub::api::schema_service_client::SchemaServiceClient;
use crate::pubsub::api::{
    CreateSchemaRequest, DeleteSchemaRequest, GetSchemaRequest, ListSchemasRequest,
    ListSchemasResponse, SchemaView,
};
use crate::pubsub::api::{PublishRequest, PubsubMessage};
use crate::schema::Schema;
use crate::source::SourceEvent;

pub struct CreateSchemaParams {
    pub parent: String,
    pub schema_id: String,
    pub schema_type: PubSubSchemaType,
    pub definition: String,
}

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
        data: Vec<u8>,
        key: Option<&str>,
        attributes: Option<HashMap<String, String>>,
        is_batch: bool,
    ) -> anyhow::Result<String> {
        let messages = if is_batch {
            Self::create_ps_batch_message(data, key, attributes)?
        } else {
            vec![Self::create_ps_message(data, key, &attributes)]
        };

        let req = PublishRequest {
            topic: topic.clone(),
            messages,
        };

        let response = self
            .client
            .publish(req)
            .await
            .map_err(|err| anyhow!("{}. topic: {}", err.message(), &topic))?;

        let count = response.into_inner().message_ids.len();
        Ok(format!("{}", count))
    }

    fn create_ps_message(
        b: Vec<u8>,
        key: Option<&str>,
        attributes: &Option<HashMap<String, String>>,
    ) -> PubsubMessage {
        let mut ps_msg = PubsubMessage {
            data: b,
            ..Default::default()
        };

        if let Some(k) = key {
            k.clone_into(&mut ps_msg.ordering_key)
        }

        if let Some(attr) = attributes.as_ref() {
            ps_msg.attributes = attr.clone();
        }

        ps_msg
    }

    fn create_ps_batch_message(
        data: Vec<u8>,
        key: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    ) -> anyhow::Result<Vec<PubsubMessage>> {
        let (msgs, _) = framed::decode(&data)?;
        let mut req_msgs = Vec::with_capacity(msgs.len());

        for item in msgs {
            let ps_msg = Self::create_ps_message(item, key, &attributes);
            req_msgs.push(ps_msg);
        }

        Ok(req_msgs)
    }
}

pub struct SchemaService<I> {
    client: tokio::sync::Mutex<SchemaServiceClient<InterceptedService<Channel, I>>>,
    cache: tokio::sync::Mutex<HashMap<String, Schema>>,
}

impl<I: Interceptor> SchemaService<I> {
    pub async fn with_interceptor(interceptor: I) -> anyhow::Result<Self> {
        let channel = tls_transport()
            .await
            .with_context(|| "failed to create tls transport for pubsub schema service")?;

        let client = SchemaServiceClient::with_interceptor(channel, interceptor);

        Ok(Self {
            client: tokio::sync::Mutex::new(client),
            cache: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    pub async fn list_schemas(&self, parent: String) -> anyhow::Result<ListSchemasResponse> {
        let mut client = self.client.lock().await;
        let schema_list_response = client
            .list_schemas(ListSchemasRequest {
                parent,
                view: SchemaView::Full.into(),
                ..Default::default()
            })
            .await?;

        Ok(schema_list_response.into_inner())
    }

    pub async fn create_schema(
        &self,
        params: CreateSchemaParams,
    ) -> anyhow::Result<super::api::Schema> {
        let mut client = self.client.lock().await;
        let response = client
            .create_schema(CreateSchemaRequest {
                parent: params.parent,
                schema_id: params.schema_id,
                schema: Some(super::api::Schema {
                    name: String::new(),
                    r#type: params.schema_type.into(),
                    definition: params.definition,
                }),
            })
            .await?;

        Ok(response.into_inner())
    }

    pub async fn delete_schema(&self, name: String) -> anyhow::Result<()> {
        let mut client = self.client.lock().await;
        client.delete_schema(DeleteSchemaRequest { name }).await?;

        Ok(())
    }

    pub async fn get_schema(&self, id: &str) -> anyhow::Result<Schema> {
        // Check cache first
        {
            let cache = self.cache.lock().await;
            if let Some(schema) = cache.get(id) {
                log::info!("schema {} found in cache", id);
                return Ok(schema.clone());
            }
        }

        // Fetch from PubSub
        let pubsub_schema = {
            let mut client = self.client.lock().await;
            client
                .get_schema(GetSchemaRequest {
                    name: id.to_string(),
                    ..Default::default()
                })
                .await?
                .into_inner()
        };

        let internal_schema_encoding = match PubSubSchemaType::try_from(pubsub_schema.r#type)? {
            PubSubSchemaType::Avro => Encoding::Avro,
            PubSubSchemaType::ProtocolBuffer => {
                bail!("unsupported pubsub schema type: protobuf")
            }
            PubSubSchemaType::Unspecified => {
                bail!("unsupported pubsub schema type: unspecified")
            }
        };

        let schema = Schema::parse(&pubsub_schema.definition, internal_schema_encoding)?;

        // Cache the result
        {
            let mut cache = self.cache.lock().await;
            cache.insert(id.to_string(), schema.clone());
        }
        log::info!("schema {} added to cache", id);

        Ok(schema)
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
            let mut ack_ids = Vec::new();
            for m in msg.received_messages {
                ack_ids.push(m.ack_id.clone());
                if let Some(payload) = m.message {
                    log::debug!("received pubsub message: {}", payload.message_id);
                    let source_event = SourceEvent {
                        raw_bytes: payload.data,
                        attributes: Some(payload.attributes),
                        encoding: self.encoding.clone(),
                        is_framed_batch: false,
                        // todo: check whether pubsub provides a cursor-like mechanism
                        cursor: None,
                    };
                    events.send(source_event).await?;
                }
            }

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
