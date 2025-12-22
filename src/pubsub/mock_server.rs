//! Mock gRPC server for PubSub testing
//!
//! Provides a complete mock implementation of Google Cloud PubSub gRPC services
//! for integration-style testing without requiring real GCP infrastructure.
//!
//! # Features
//! - Full Publisher, Subscriber, and SchemaService mock implementations
//! - In-memory message storage and retrieval
//! - Configurable success/failure scenarios
//! - Thread-safe operation with Arc<Mutex<>>
//! - Easy to start/stop for tests
//!
//! # Example
//! ```no_run
//! use mstream::pubsub::mock_server::start_mock_pubsub_server;
//!
//! #[tokio::test]
//! async fn test_with_mock_server() {
//!     let (addr, mock_state) = start_mock_pubsub_server().await.unwrap();
//!     // Use mock server at `addr`
//!     // Inspect state via `mock_state`
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

use super::api::{
    publisher_server::{Publisher, PublisherServer},
    subscriber_server::{Subscriber, SubscriberServer},
    schema_service_server::{SchemaService as SchemaServiceTrait, SchemaServiceServer},
    AcknowledgeRequest, CreateSchemaRequest, DeleteSchemaRequest, GetSchemaRequest,
    ListSchemasRequest, ListSchemasResponse, ModifyAckDeadlineRequest, ModifyPushConfigRequest,
    PublishRequest, PublishResponse, PubsubMessage, PullRequest, PullResponse, ReceivedMessage,
    Schema as PubSubSchema, SeekRequest, SeekResponse, StreamingPullRequest,
    StreamingPullResponse, ValidateMessageRequest, ValidateMessageResponse,
    ValidateSchemaRequest, ValidateSchemaResponse,
};

/// Shared state for the mock PubSub server
#[derive(Debug, Clone, Default)]
pub struct MockPubSubState {
    /// Published messages indexed by topic
    pub published_messages: Arc<Mutex<HashMap<String, Vec<PubsubMessage>>>>,
    /// Messages available for pulling, indexed by subscription
    pub subscription_messages: Arc<Mutex<HashMap<String, Vec<PubsubMessage>>>>,
    /// Schemas indexed by schema ID
    pub schemas: Arc<Mutex<HashMap<String, PubSubSchema>>>,
    /// Whether operations should fail
    pub should_fail: Arc<Mutex<bool>>,
}

impl MockPubSubState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_failure() -> Self {
        Self {
            should_fail: Arc::new(Mutex::new(true)),
            ..Default::default()
        }
    }

    /// Add a message to a subscription for pulling
    pub async fn add_message_to_subscription(&self, subscription: &str, message: PubsubMessage) {
        self.subscription_messages
            .lock()
            .await
            .entry(subscription.to_string())
            .or_insert_with(Vec::new)
            .push(message);
    }

    /// Get all published messages for a topic
    pub async fn get_published(&self, topic: &str) -> Vec<PubsubMessage> {
        self.published_messages
            .lock()
            .await
            .get(topic)
            .cloned()
            .unwrap_or_default()
    }

    /// Clear all published messages
    pub async fn clear_published(&self) {
        self.published_messages.lock().await.clear();
    }

    /// Set failure mode
    pub async fn set_should_fail(&self, should_fail: bool) {
        *self.should_fail.lock().await = should_fail;
    }
}

/// Mock Publisher service implementation
#[derive(Debug, Clone)]
pub struct MockPublisher {
    state: MockPubSubState,
}

impl MockPublisher {
    pub fn new(state: MockPubSubState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Publisher for MockPublisher {
    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        if *self.state.should_fail.lock().await {
            return Err(Status::internal("Mock publisher configured to fail"));
        }

        let req = request.into_inner();
        let message_count = req.messages.len();

        // Store messages
        self.state
            .published_messages
            .lock()
            .await
            .entry(req.topic.clone())
            .or_insert_with(Vec::new)
            .extend(req.messages);

        // Generate message IDs
        let message_ids = (0..message_count)
            .map(|i| format!("mock-msg-{}", i))
            .collect();

        Ok(Response::new(PublishResponse { message_ids }))
    }

    // Implement other required methods with default/unimplemented responses
    async fn create_topic(
        &self,
        _request: Request<super::api::Topic>,
    ) -> Result<Response<super::api::Topic>, Status> {
        Err(Status::unimplemented("CreateTopic not implemented in mock"))
    }

    async fn get_topic(
        &self,
        _request: Request<super::api::GetTopicRequest>,
    ) -> Result<Response<super::api::Topic>, Status> {
        Err(Status::unimplemented("GetTopic not implemented in mock"))
    }

    async fn update_topic(
        &self,
        _request: Request<super::api::UpdateTopicRequest>,
    ) -> Result<Response<super::api::Topic>, Status> {
        Err(Status::unimplemented(
            "UpdateTopic not implemented in mock",
        ))
    }

    async fn list_topics(
        &self,
        _request: Request<super::api::ListTopicsRequest>,
    ) -> Result<Response<super::api::ListTopicsResponse>, Status> {
        Err(Status::unimplemented("ListTopics not implemented in mock"))
    }

    async fn list_topic_subscriptions(
        &self,
        _request: Request<super::api::ListTopicSubscriptionsRequest>,
    ) -> Result<Response<super::api::ListTopicSubscriptionsResponse>, Status> {
        Err(Status::unimplemented(
            "ListTopicSubscriptions not implemented in mock",
        ))
    }

    async fn list_topic_snapshots(
        &self,
        _request: Request<super::api::ListTopicSnapshotsRequest>,
    ) -> Result<Response<super::api::ListTopicSnapshotsResponse>, Status> {
        Err(Status::unimplemented(
            "ListTopicSnapshots not implemented in mock",
        ))
    }

    async fn delete_topic(
        &self,
        _request: Request<super::api::DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "DeleteTopic not implemented in mock",
        ))
    }

    async fn detach_subscription(
        &self,
        _request: Request<super::api::DetachSubscriptionRequest>,
    ) -> Result<Response<super::api::DetachSubscriptionResponse>, Status> {
        Err(Status::unimplemented(
            "DetachSubscription not implemented in mock",
        ))
    }
}

/// Mock Subscriber service implementation
#[derive(Debug, Clone)]
pub struct MockSubscriber {
    state: MockPubSubState,
}

impl MockSubscriber {
    pub fn new(state: MockPubSubState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Subscriber for MockSubscriber {
    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> Result<Response<PullResponse>, Status> {
        if *self.state.should_fail.lock().await {
            return Err(Status::internal("Mock subscriber configured to fail"));
        }

        let req = request.into_inner();
        let mut messages = self.state.subscription_messages.lock().await;

        let subscription_msgs = messages
            .entry(req.subscription.clone())
            .or_insert_with(Vec::new);

        // Take up to max_messages
        let count = std::cmp::min(req.max_messages as usize, subscription_msgs.len());
        let pulled: Vec<PubsubMessage> = subscription_msgs.drain(..count).collect();

        let received_messages: Vec<ReceivedMessage> = pulled
            .into_iter()
            .enumerate()
            .map(|(i, msg)| ReceivedMessage {
                ack_id: format!("mock-ack-{}", i),
                message: Some(msg),
                delivery_attempt: 0,
            })
            .collect();

        Ok(Response::new(PullResponse {
            received_messages,
        }))
    }

    async fn acknowledge(
        &self,
        _request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        if *self.state.should_fail.lock().await {
            return Err(Status::internal("Mock subscriber configured to fail"));
        }
        Ok(Response::new(()))
    }

    async fn modify_ack_deadline(
        &self,
        _request: Request<ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    // Streaming pull - simplified implementation
    type StreamingPullStream =
        tokio_stream::wrappers::ReceiverStream<Result<StreamingPullResponse, Status>>;

    async fn streaming_pull(
        &self,
        _request: Request<tonic::Streaming<StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        Err(Status::unimplemented(
            "StreamingPull not fully implemented in mock",
        ))
    }

    async fn create_subscription(
        &self,
        _request: Request<super::api::Subscription>,
    ) -> Result<Response<super::api::Subscription>, Status> {
        Err(Status::unimplemented(
            "CreateSubscription not implemented in mock",
        ))
    }

    async fn modify_push_config(
        &self,
        _request: Request<ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "ModifyPushConfig not implemented in mock",
        ))
    }

    async fn get_snapshot(
        &self,
        _request: Request<super::api::GetSnapshotRequest>,
    ) -> Result<Response<super::api::Snapshot>, Status> {
        Err(Status::unimplemented(
            "GetSnapshot not implemented in mock",
        ))
    }

    async fn list_snapshots(
        &self,
        _request: Request<super::api::ListSnapshotsRequest>,
    ) -> Result<Response<super::api::ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented(
            "ListSnapshots not implemented in mock",
        ))
    }

    async fn create_snapshot(
        &self,
        _request: Request<super::api::CreateSnapshotRequest>,
    ) -> Result<Response<super::api::Snapshot>, Status> {
        Err(Status::unimplemented(
            "CreateSnapshot not implemented in mock",
        ))
    }

    async fn update_snapshot(
        &self,
        _request: Request<super::api::UpdateSnapshotRequest>,
    ) -> Result<Response<super::api::Snapshot>, Status> {
        Err(Status::unimplemented(
            "UpdateSnapshot not implemented in mock",
        ))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<super::api::DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "DeleteSnapshot not implemented in mock",
        ))
    }

    async fn get_subscription(
        &self,
        _request: Request<super::api::GetSubscriptionRequest>,
    ) -> Result<Response<super::api::Subscription>, Status> {
        Err(Status::unimplemented(
            "GetSubscription not implemented in mock",
        ))
    }

    async fn update_subscription(
        &self,
        _request: Request<super::api::UpdateSubscriptionRequest>,
    ) -> Result<Response<super::api::Subscription>, Status> {
        Err(Status::unimplemented(
            "UpdateSubscription not implemented in mock",
        ))
    }

    async fn list_subscriptions(
        &self,
        _request: Request<super::api::ListSubscriptionsRequest>,
    ) -> Result<Response<super::api::ListSubscriptionsResponse>, Status> {
        Err(Status::unimplemented(
            "ListSubscriptions not implemented in mock",
        ))
    }

    async fn delete_subscription(
        &self,
        _request: Request<super::api::DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "DeleteSubscription not implemented in mock",
        ))
    }

    async fn seek(&self, _request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        Err(Status::unimplemented("Seek not implemented in mock"))
    }
}

/// Mock SchemaService implementation
#[derive(Debug, Clone)]
pub struct MockSchemaService {
    state: MockPubSubState,
}

impl MockSchemaService {
    pub fn new(state: MockPubSubState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl SchemaServiceTrait for MockSchemaService {
    async fn create_schema(
        &self,
        request: Request<CreateSchemaRequest>,
    ) -> Result<Response<PubSubSchema>, Status> {
        if *self.state.should_fail.lock().await {
            return Err(Status::internal("Mock schema service configured to fail"));
        }

        let req = request.into_inner();
        if let Some(schema) = req.schema {
            self.state
                .schemas
                .lock()
                .await
                .insert(schema.name.clone(), schema.clone());
            Ok(Response::new(schema))
        } else {
            Err(Status::invalid_argument("Schema is required"))
        }
    }

    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<PubSubSchema>, Status> {
        if *self.state.should_fail.lock().await {
            return Err(Status::internal("Mock schema service configured to fail"));
        }

        let req = request.into_inner();
        self.state
            .schemas
            .lock()
            .await
            .get(&req.name)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| Status::not_found(format!("Schema not found: {}", req.name)))
    }

    async fn list_schemas(
        &self,
        _request: Request<ListSchemasRequest>,
    ) -> Result<Response<ListSchemasResponse>, Status> {
        if *self.state.should_fail.lock().await {
            return Err(Status::internal("Mock schema service configured to fail"));
        }

        let schemas = self.state.schemas.lock().await;
        let schema_list: Vec<PubSubSchema> = schemas.values().cloned().collect();

        Ok(Response::new(ListSchemasResponse {
            schemas: schema_list,
            next_page_token: String::new(),
        }))
    }

    async fn delete_schema(
        &self,
        request: Request<DeleteSchemaRequest>,
    ) -> Result<Response<()>, Status> {
        if *self.state.should_fail.lock().await {
            return Err(Status::internal("Mock schema service configured to fail"));
        }

        let req = request.into_inner();
        self.state.schemas.lock().await.remove(&req.name);
        Ok(Response::new(()))
    }

    async fn validate_schema(
        &self,
        _request: Request<ValidateSchemaRequest>,
    ) -> Result<Response<ValidateSchemaResponse>, Status> {
        Err(Status::unimplemented(
            "ValidateSchema not implemented in mock",
        ))
    }

    async fn validate_message(
        &self,
        _request: Request<ValidateMessageRequest>,
    ) -> Result<Response<ValidateMessageResponse>, Status> {
        Err(Status::unimplemented(
            "ValidateMessage not implemented in mock",
        ))
    }
}

/// Start a mock PubSub gRPC server
///
/// Returns the server address and shared state for inspection
pub async fn start_mock_pubsub_server(
) -> anyhow::Result<(String, MockPubSubState, tokio::task::JoinHandle<()>)> {
    let state = MockPubSubState::new();
    start_mock_pubsub_server_with_state(state).await
}

/// Start a mock PubSub gRPC server with custom state
///
/// Allows pre-configuring the mock state before starting
pub async fn start_mock_pubsub_server_with_state(
    state: MockPubSubState,
) -> anyhow::Result<(String, MockPubSubState, tokio::task::JoinHandle<()>)> {
    use std::net::SocketAddr;
    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    let publisher = MockPublisher::new(state.clone());
    let subscriber = MockSubscriber::new(state.clone());
    let schema_service = MockSchemaService::new(state.clone());

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(PublisherServer::new(publisher))
            .add_service(SubscriberServer::new(subscriber))
            .add_service(SchemaServiceServer::new(schema_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("Mock server failed");
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok((format!("http://{}", local_addr), state, server_handle))
}
