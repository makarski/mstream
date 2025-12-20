use std::collections::HashMap;

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;
use tonic::service::Interceptor;

use super::api::schema::Type as PubSubSchemaType;
use super::api::subscriber_client::SubscriberClient;
use super::api::StreamingPullRequest;
use super::{tls_transport, Channel, InterceptedService};
use crate::config::Encoding;
use crate::encoding::framed;
use crate::pubsub::api::publisher_client::PublisherClient;
use crate::pubsub::api::schema_service_client::SchemaServiceClient;
use crate::pubsub::api::{GetSchemaRequest, ListSchemasRequest, ListSchemasResponse};
use crate::pubsub::api::{PublishRequest, PubsubMessage};
use crate::schema::Schema;
use crate::source::SourceEvent;

/// Trait for publishing messages to PubSub
/// Enables mocking and testing without real GCP connection
#[async_trait]
pub trait PubSubPublisherTrait: Send + Sync {
    async fn publish(
        &mut self,
        topic: String,
        data: Vec<u8>,
        key: Option<&str>,
        attributes: Option<HashMap<String, String>>,
        is_batch: bool,
    ) -> anyhow::Result<String>;
}

/// Trait for subscribing to PubSub messages
/// Enables mocking and testing without real GCP connection
#[async_trait]
pub trait PubSubSubscriberTrait: Send + Sync {
    async fn subscribe(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()>;
}

/// Trait for PubSub schema service operations
/// Enables mocking and testing without real GCP connection
#[async_trait]
pub trait SchemaServiceTrait: Send + Sync {
    async fn list_schemas(&mut self, parent: String) -> anyhow::Result<ListSchemasResponse>;
    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema>;
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

// Implement the trait for PubSubPublisher
#[async_trait]
impl<I: Interceptor + Send + Sync> PubSubPublisherTrait for PubSubPublisher<I> {
    async fn publish(
        &mut self,
        topic: String,
        data: Vec<u8>,
        key: Option<&str>,
        attributes: Option<HashMap<String, String>>,
        is_batch: bool,
    ) -> anyhow::Result<String> {
        self.publish(topic, data, key, attributes, is_batch).await
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

    pub async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema> {
        if !self.cache.contains_key(&id) {
            let schema_response = self.client.get_schema(GetSchemaRequest {
                name: id.clone(),
                ..Default::default()
            });

            let pubsub_schema = schema_response.await?.into_inner();
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
            self.cache.insert(id.clone(), schema.clone());
            log::info!("schema {} added to cache", id);

            return Ok(schema);
        } else {
            log::info!("schema {} found in cache", id);
        }

        self.cache
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("schema not found"))
    }
}

// Implement the trait for SchemaService
#[async_trait]
impl<I: Interceptor + Send + Sync> SchemaServiceTrait for SchemaService<I> {
    async fn list_schemas(&mut self, parent: String) -> anyhow::Result<ListSchemasResponse> {
        self.list_schemas(parent).await
    }

    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema> {
        self.get_schema(id).await
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

// Implement the trait for PubSubSubscriber
#[async_trait]
impl<I: Interceptor + Send + Sync> PubSubSubscriberTrait for PubSubSubscriber<I> {
    async fn subscribe(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()> {
        self.subscribe(events).await
    }
}

// ============================================================================
// Mock Implementations for Testing
// ============================================================================

#[cfg(test)]
pub mod mocks {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Mock PubSub publisher for testing
    /// Records all published messages for verification
    #[derive(Clone)]
    pub struct MockPubSubPublisher {
        pub published: Arc<Mutex<Vec<PublishedMessage>>>,
        pub should_fail: bool,
    }

    #[derive(Debug, Clone)]
    pub struct PublishedMessage {
        pub topic: String,
        pub data: Vec<u8>,
        pub key: Option<String>,
        pub attributes: Option<HashMap<String, String>>,
        pub is_batch: bool,
    }

    impl MockPubSubPublisher {
        pub fn new() -> Self {
            Self {
                published: Arc::new(Mutex::new(Vec::new())),
                should_fail: false,
            }
        }

        pub fn with_failure() -> Self {
            Self {
                published: Arc::new(Mutex::new(Vec::new())),
                should_fail: true,
            }
        }

        pub async fn get_published(&self) -> Vec<PublishedMessage> {
            self.published.lock().await.clone()
        }

        pub async fn clear(&self) {
            self.published.lock().await.clear();
        }
    }

    #[async_trait]
    impl PubSubPublisherTrait for MockPubSubPublisher {
        async fn publish(
            &mut self,
            topic: String,
            data: Vec<u8>,
            key: Option<&str>,
            attributes: Option<HashMap<String, String>>,
            is_batch: bool,
        ) -> anyhow::Result<String> {
            if self.should_fail {
                anyhow::bail!("Mock publisher configured to fail");
            }

            let message = PublishedMessage {
                topic: topic.clone(),
                data: data.clone(),
                key: key.map(|s| s.to_string()),
                attributes: attributes.clone(),
                is_batch,
            };

            self.published.lock().await.push(message);

            Ok("1".to_string())
        }
    }

    /// Mock PubSub subscriber for testing
    /// Can be pre-loaded with messages to deliver
    pub struct MockPubSubSubscriber {
        pub messages: Arc<Mutex<Vec<SourceEvent>>>,
        pub should_fail: bool,
    }

    impl MockPubSubSubscriber {
        pub fn new() -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
                should_fail: false,
            }
        }

        pub fn with_failure() -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
                should_fail: true,
            }
        }

        pub async fn add_message(&self, event: SourceEvent) {
            self.messages.lock().await.push(event);
        }

        pub async fn add_messages(&self, events: Vec<SourceEvent>) {
            self.messages.lock().await.extend(events);
        }
    }

    #[async_trait]
    impl PubSubSubscriberTrait for MockPubSubSubscriber {
        async fn subscribe(&mut self, events: Sender<SourceEvent>) -> anyhow::Result<()> {
            if self.should_fail {
                anyhow::bail!("Mock subscriber configured to fail");
            }

            let messages = self.messages.lock().await.clone();
            for message in messages {
                events.send(message).await?;
            }

            Ok(())
        }
    }

    /// Mock schema service for testing
    /// Can be pre-loaded with schemas
    pub struct MockSchemaService {
        pub schemas: Arc<Mutex<HashMap<String, Schema>>>,
        pub should_fail: bool,
    }

    impl MockSchemaService {
        pub fn new() -> Self {
            Self {
                schemas: Arc::new(Mutex::new(HashMap::new())),
                should_fail: false,
            }
        }

        pub fn with_failure() -> Self {
            Self {
                schemas: Arc::new(Mutex::new(HashMap::new())),
                should_fail: true,
            }
        }

        pub async fn add_schema(&self, id: String, schema: Schema) {
            self.schemas.lock().await.insert(id, schema);
        }
    }

    #[async_trait]
    impl SchemaServiceTrait for MockSchemaService {
        async fn list_schemas(&mut self, _parent: String) -> anyhow::Result<ListSchemasResponse> {
            if self.should_fail {
                anyhow::bail!("Mock schema service configured to fail");
            }

            Ok(ListSchemasResponse {
                schemas: vec![],
                next_page_token: String::new(),
            })
        }

        async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema> {
            if self.should_fail {
                anyhow::bail!("Mock schema service configured to fail");
            }

            self.schemas
                .lock()
                .await
                .get(&id)
                .cloned()
                .ok_or_else(|| anyhow!("Schema not found: {}", id))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::config::Encoding;

        #[tokio::test]
        async fn test_mock_publisher_success() {
            let mut mock = MockPubSubPublisher::new();

            let result = mock
                .publish(
                    "test-topic".to_string(),
                    b"test data".to_vec(),
                    Some("key1"),
                    Some(
                        [("attr1".to_string(), "value1".to_string())]
                            .iter()
                            .cloned()
                            .collect(),
                    ),
                    false,
                )
                .await;

            assert!(result.is_ok());
            assert_eq!(result.unwrap(), "1");

            let published = mock.get_published().await;
            assert_eq!(published.len(), 1);
            assert_eq!(published[0].topic, "test-topic");
            assert_eq!(published[0].data, b"test data");
            assert_eq!(published[0].key, Some("key1".to_string()));
            assert!(!published[0].is_batch);
        }

        #[tokio::test]
        async fn test_mock_publisher_failure() {
            let mut mock = MockPubSubPublisher::with_failure();

            let result = mock
                .publish(
                    "test-topic".to_string(),
                    b"test data".to_vec(),
                    None,
                    None,
                    false,
                )
                .await;

            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().to_string(),
                "Mock publisher configured to fail"
            );
        }

        #[tokio::test]
        async fn test_mock_subscriber_success() {
            let mut mock = MockPubSubSubscriber::new();

            // Pre-load some messages
            mock.add_message(SourceEvent {
                raw_bytes: b"message 1".to_vec(),
                attributes: None,
                encoding: Encoding::Json,
                is_framed_batch: false,
            })
            .await;

            mock.add_message(SourceEvent {
                raw_bytes: b"message 2".to_vec(),
                attributes: Some(
                    [("key".to_string(), "value".to_string())]
                        .iter()
                        .cloned()
                        .collect(),
                ),
                encoding: Encoding::Json,
                is_framed_batch: false,
            })
            .await;

            // Create a channel to receive events
            let (tx, mut rx) = tokio::sync::mpsc::channel(10);

            // Subscribe
            let result = mock.subscribe(tx).await;
            assert!(result.is_ok());

            // Verify we received the messages
            let msg1 = rx.recv().await.unwrap();
            assert_eq!(msg1.raw_bytes, b"message 1");

            let msg2 = rx.recv().await.unwrap();
            assert_eq!(msg2.raw_bytes, b"message 2");
            assert!(msg2.attributes.is_some());
        }

        #[tokio::test]
        async fn test_mock_subscriber_failure() {
            let mut mock = MockPubSubSubscriber::with_failure();
            let (tx, _rx) = tokio::sync::mpsc::channel(10);

            let result = mock.subscribe(tx).await;
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().to_string(),
                "Mock subscriber configured to fail"
            );
        }

        #[tokio::test]
        async fn test_mock_schema_service() {
            let mut mock = MockSchemaService::new();

            // Add a test schema
            let schema = Schema::parse(
                r#"{"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]}"#,
                Encoding::Avro,
            )
            .unwrap();

            mock.add_schema("test-schema-id".to_string(), schema.clone())
                .await;

            // Get the schema
            let result = mock.get_schema("test-schema-id".to_string()).await;
            assert!(result.is_ok());

            // Try to get a non-existent schema
            let result = mock.get_schema("non-existent".to_string()).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_sink_provider_with_mock() {
            use crate::sink::{encoding::SinkEvent, EventSink, SinkProvider};

            let mock = MockPubSubPublisher::new();
            let mut sink = SinkProvider::PubSub(Box::new(mock.clone()));

            let sink_event = SinkEvent {
                raw_bytes: b"test payload".to_vec(),
                attributes: Some(
                    [("source".to_string(), "test".to_string())]
                        .iter()
                        .cloned()
                        .collect(),
                ),
                encoding: Encoding::Json,
                is_framed_batch: false,
            };

            let result = sink
                .publish(sink_event, "my-topic".to_string(), Some("key123"))
                .await;

            assert!(result.is_ok());

            // Verify the message was published
            let published = mock.get_published().await;
            assert_eq!(published.len(), 1);
            assert_eq!(published[0].topic, "my-topic");
            assert_eq!(published[0].key, Some("key123".to_string()));
        }
    }
}
