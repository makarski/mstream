//! Tests for mock gRPC PubSub server
//!
//! Demonstrates using the mock gRPC server for integration-style testing
//! without requiring real GCP infrastructure.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use mstream::pubsub::api::{
        publisher_client::PublisherClient, subscriber_client::SubscriberClient,
        schema_service_client::SchemaServiceClient, PublishRequest, PubsubMessage, PullRequest,
        GetSchemaRequest, ListSchemasRequest, Schema as PubSubSchema,
    };
    use mstream::pubsub::mock_server::{start_mock_pubsub_server, MockPubSubState};
    use mstream::pubsub::NoAuth;
    use tonic::transport::Channel;

    #[tokio::test]
    async fn test_mock_server_publish_and_pull() {
        // Start mock server
        let (addr, state, _handle) = start_mock_pubsub_server().await.unwrap();

        // Connect clients to mock server
        let channel = Channel::from_shared(addr.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();

        let mut publisher = PublisherClient::with_interceptor(channel.clone(), NoAuth);
        let mut subscriber = SubscriberClient::with_interceptor(channel, NoAuth);

        // Publish a message
        let topic = "projects/test-project/topics/test-topic".to_string();
        let test_data = b"Hello from mock server!";

        let publish_request = PublishRequest {
            topic: topic.clone(),
            messages: vec![PubsubMessage {
                data: test_data.to_vec(),
                attributes: HashMap::from([
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ]),
                message_id: String::new(),
                publish_time: None,
                ordering_key: "test-key".to_string(),
            }],
        };

        let publish_response = publisher.publish(publish_request).await.unwrap();
        let message_ids = publish_response.into_inner().message_ids;

        assert_eq!(message_ids.len(), 1);
        assert!(message_ids[0].starts_with("mock-msg-"));

        // Verify message was stored
        let published = state.get_published(&topic).await;
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].data, test_data);
        assert_eq!(published[0].attributes.get("key1"), Some(&"value1".to_string()));

        // Add message to subscription for pulling
        state
            .add_message_to_subscription(
                "projects/test-project/subscriptions/test-sub",
                PubsubMessage {
                    data: test_data.to_vec(),
                    attributes: HashMap::from([("source".to_string(), "test".to_string())]),
                    message_id: "msg-123".to_string(),
                    publish_time: None,
                    ordering_key: String::new(),
                },
            )
            .await;

        // Pull the message
        let pull_request = PullRequest {
            subscription: "projects/test-project/subscriptions/test-sub".to_string(),
            max_messages: 10,
            ..Default::default()
        };

        let pull_response = subscriber.pull(pull_request).await.unwrap();
        let received = pull_response.into_inner().received_messages;

        assert_eq!(received.len(), 1);
        assert_eq!(received[0].message.as_ref().unwrap().data, test_data);
        assert!(received[0].ack_id.starts_with("mock-ack-"));
    }

    #[tokio::test]
    async fn test_mock_server_batch_publish() {
        let (addr, state, _handle) = start_mock_pubsub_server().await.unwrap();

        let channel = Channel::from_shared(addr).unwrap().connect().await.unwrap();
        let mut publisher = PublisherClient::with_interceptor(channel, NoAuth);

        let topic = "projects/test-project/topics/batch-topic".to_string();

        // Publish multiple messages
        let messages: Vec<PubsubMessage> = (0..5)
            .map(|i| PubsubMessage {
                data: format!("message-{}", i).into_bytes(),
                attributes: HashMap::new(),
                message_id: String::new(),
                publish_time: None,
                ordering_key: String::new(),
            })
            .collect();

        let publish_request = PublishRequest {
            topic: topic.clone(),
            messages: messages.clone(),
        };

        let response = publisher.publish(publish_request).await.unwrap();
        assert_eq!(response.into_inner().message_ids.len(), 5);

        // Verify all messages were stored
        let published = state.get_published(&topic).await;
        assert_eq!(published.len(), 5);
        for i in 0..5 {
            assert_eq!(published[i].data, format!("message-{}", i).into_bytes());
        }
    }

    #[tokio::test]
    async fn test_mock_server_acknowledge() {
        let (addr, state, _handle) = start_mock_pubsub_server().await.unwrap();

        let channel = Channel::from_shared(addr).unwrap().connect().await.unwrap();
        let mut subscriber = SubscriberClient::with_interceptor(channel, NoAuth);

        // Add a message
        state
            .add_message_to_subscription(
                "projects/test-project/subscriptions/test-sub",
                PubsubMessage {
                    data: b"test".to_vec(),
                    ..Default::default()
                },
            )
            .await;

        // Pull and acknowledge
        let pull_response = subscriber
            .pull(PullRequest {
                subscription: "projects/test-project/subscriptions/test-sub".to_string(),
                max_messages: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let received = pull_response.into_inner().received_messages;
        assert_eq!(received.len(), 1);

        let ack_id = received[0].ack_id.clone();

        // Acknowledge the message
        let ack_response = subscriber
            .acknowledge(mstream::pubsub::api::AcknowledgeRequest {
                subscription: "projects/test-project/subscriptions/test-sub".to_string(),
                ack_ids: vec![ack_id],
            })
            .await;

        assert!(ack_response.is_ok());
    }

    #[tokio::test]
    async fn test_mock_server_schema_operations() {
        let (addr, state, _handle) = start_mock_pubsub_server().await.unwrap();

        let channel = Channel::from_shared(addr).unwrap().connect().await.unwrap();
        let mut schema_client = SchemaServiceClient::with_interceptor(channel, NoAuth);

        // Create a schema
        let schema = PubSubSchema {
            name: "projects/test-project/schemas/test-schema".to_string(),
            r#type: 1, // Avro
            definition: r#"{"type": "record", "name": "Test", "fields": []}"#.to_string(),
        };

        let create_response = schema_client
            .create_schema(mstream::pubsub::api::CreateSchemaRequest {
                parent: "projects/test-project".to_string(),
                schema: Some(schema.clone()),
                schema_id: "test-schema".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(create_response.into_inner().name, schema.name);

        // Get the schema
        let get_response = schema_client
            .get_schema(GetSchemaRequest {
                name: schema.name.clone(),
                view: 0,
            })
            .await
            .unwrap();

        let retrieved = get_response.into_inner();
        assert_eq!(retrieved.name, schema.name);
        assert_eq!(retrieved.definition, schema.definition);

        // List schemas
        let list_response = schema_client
            .list_schemas(ListSchemasRequest {
                parent: "projects/test-project".to_string(),
                page_size: 100,
                page_token: String::new(),
                view: 0,
            })
            .await
            .unwrap();

        let schemas = list_response.into_inner().schemas;
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, schema.name);

        // Delete the schema
        let delete_response = schema_client
            .delete_schema(mstream::pubsub::api::DeleteSchemaRequest {
                name: schema.name.clone(),
            })
            .await;

        assert!(delete_response.is_ok());

        // Verify deletion
        let schemas_after_delete = state.schemas.lock().await;
        assert!(!schemas_after_delete.contains_key(&schema.name));
    }

    #[tokio::test]
    async fn test_mock_server_failure_mode() {
        let state = MockPubSubState::with_failure();
        let (addr, _state, _handle) =
            mstream::pubsub::mock_server::start_mock_pubsub_server_with_state(state)
                .await
                .unwrap();

        let channel = Channel::from_shared(addr).unwrap().connect().await.unwrap();
        let mut publisher = PublisherClient::with_interceptor(channel.clone(), NoAuth);
        let mut subscriber = SubscriberClient::with_interceptor(channel.clone(), NoAuth);
        let mut schema_client = SchemaServiceClient::with_interceptor(channel, NoAuth);

        // All operations should fail
        let publish_result = publisher
            .publish(PublishRequest {
                topic: "test".to_string(),
                messages: vec![],
            })
            .await;
        assert!(publish_result.is_err());
        assert!(publish_result
            .unwrap_err()
            .message()
            .contains("configured to fail"));

        let pull_result = subscriber
            .pull(PullRequest {
                subscription: "test".to_string(),
                max_messages: 1,
                ..Default::default()
            })
            .await;
        assert!(pull_result.is_err());

        let schema_result = schema_client
            .get_schema(GetSchemaRequest {
                name: "test".to_string(),
                view: 0,
            })
            .await;
        assert!(schema_result.is_err());
    }

    #[tokio::test]
    async fn test_mock_server_multiple_subscriptions() {
        let (addr, state, _handle) = start_mock_pubsub_server().await.unwrap();

        let channel = Channel::from_shared(addr).unwrap().connect().await.unwrap();
        let mut subscriber = SubscriberClient::with_interceptor(channel, NoAuth);

        // Add messages to different subscriptions
        state
            .add_message_to_subscription(
                "projects/test/subscriptions/sub1",
                PubsubMessage {
                    data: b"message for sub1".to_vec(),
                    ..Default::default()
                },
            )
            .await;

        state
            .add_message_to_subscription(
                "projects/test/subscriptions/sub2",
                PubsubMessage {
                    data: b"message for sub2".to_vec(),
                    ..Default::default()
                },
            )
            .await;

        // Pull from sub1
        let response1 = subscriber
            .pull(PullRequest {
                subscription: "projects/test/subscriptions/sub1".to_string(),
                max_messages: 10,
                ..Default::default()
            })
            .await
            .unwrap();

        let msgs1 = response1.into_inner().received_messages;
        assert_eq!(msgs1.len(), 1);
        assert_eq!(msgs1[0].message.as_ref().unwrap().data, b"message for sub1");

        // Pull from sub2
        let response2 = subscriber
            .pull(PullRequest {
                subscription: "projects/test/subscriptions/sub2".to_string(),
                max_messages: 10,
                ..Default::default()
            })
            .await
            .unwrap();

        let msgs2 = response2.into_inner().received_messages;
        assert_eq!(msgs2.len(), 1);
        assert_eq!(msgs2[0].message.as_ref().unwrap().data, b"message for sub2");
    }

    #[tokio::test]
    async fn test_mock_server_clear_published() {
        let (addr, state, _handle) = start_mock_pubsub_server().await.unwrap();

        let channel = Channel::from_shared(addr).unwrap().connect().await.unwrap();
        let mut publisher = PublisherClient::with_interceptor(channel, NoAuth);

        let topic = "projects/test/topics/test".to_string();

        // Publish messages
        publisher
            .publish(PublishRequest {
                topic: topic.clone(),
                messages: vec![PubsubMessage {
                    data: b"test".to_vec(),
                    ..Default::default()
                }],
            })
            .await
            .unwrap();

        assert_eq!(state.get_published(&topic).await.len(), 1);

        // Clear published messages
        state.clear_published().await;

        assert_eq!(state.get_published(&topic).await.len(), 0);
    }
}
