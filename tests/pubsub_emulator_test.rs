// Example test demonstrating PubSub emulator usage
//
// To run this test:
// 1. Start the PubSub emulator:
//    docker run -d -p 8085:8085 --name pubsub-emulator \
//      gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
//      gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
//
// 2. Run the test:
//    USE_PUBSUB_EMULATOR=true cargo test --test pubsub_emulator_test
//
// 3. Cleanup:
//    docker stop pubsub-emulator && docker rm pubsub-emulator

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use mstream::pubsub::api::{
        publisher_client::PublisherClient, subscriber_client::SubscriberClient, PublishRequest,
        PubsubMessage, PullRequest, AcknowledgeRequest,
    };
    use mstream::pubsub::{emulator_transport, NoAuth};

    /// Helper to check if emulator mode is enabled
    fn is_emulator_enabled() -> bool {
        std::env::var("USE_PUBSUB_EMULATOR")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false)
    }

    #[tokio::test]
    #[ignore = "Requires PubSub emulator running on localhost:8085"]
    async fn test_pubsub_emulator_publish_and_pull() {
        if !is_emulator_enabled() {
            println!("Skipping test - USE_PUBSUB_EMULATOR not set to true");
            return;
        }

        // Connect to emulator
        let channel = emulator_transport()
            .await
            .expect("Failed to connect to emulator");

        // Create publisher client (no auth needed for emulator)
        let mut publisher = PublisherClient::with_interceptor(channel.clone(), NoAuth);

        // Create subscriber client (no auth needed for emulator)
        let mut subscriber = SubscriberClient::with_interceptor(channel, NoAuth);

        // Test topic and subscription names
        // Note: You may need to create these in the emulator first using gcloud commands
        // or the emulator's admin API
        let topic = "projects/test-project/topics/test-topic";
        let subscription = "projects/test-project/subscriptions/test-subscription";

        // Publish a test message
        let test_data = b"Hello from emulator test!";
        let mut attributes = HashMap::new();
        attributes.insert("test_key".to_string(), "test_value".to_string());

        let publish_request = PublishRequest {
            topic: topic.to_string(),
            messages: vec![PubsubMessage {
                data: test_data.to_vec(),
                attributes,
                message_id: String::new(),
                publish_time: None,
                ordering_key: "test-key".to_string(),
            }],
        };

        let publish_response = publisher
            .publish(publish_request)
            .await
            .expect("Failed to publish message");

        println!(
            "Published message with IDs: {:?}",
            publish_response.into_inner().message_ids
        );

        // Pull the message back
        let pull_request = PullRequest {
            subscription: subscription.to_string(),
            max_messages: 1,
            ..Default::default()
        };

        let pull_response = subscriber
            .pull(pull_request)
            .await
            .expect("Failed to pull message");

        let received_messages = pull_response.into_inner().received_messages;
        assert!(!received_messages.is_empty(), "Should have received at least one message");

        // Verify message content
        for received_msg in &received_messages {
            let msg = received_msg.message.as_ref().expect("Message should exist");
            assert_eq!(msg.data, test_data);
            assert_eq!(
                msg.attributes.get("test_key"),
                Some(&"test_value".to_string())
            );
            println!("Received message: {:?}", String::from_utf8_lossy(&msg.data));
        }

        // Acknowledge the messages
        let ack_ids: Vec<String> = received_messages
            .iter()
            .map(|m| m.ack_id.clone())
            .collect();

        subscriber
            .acknowledge(AcknowledgeRequest {
                subscription: subscription.to_string(),
                ack_ids,
            })
            .await
            .expect("Failed to acknowledge messages");

        println!("Test completed successfully!");
    }

    #[tokio::test]
    #[ignore = "Requires PubSub emulator running on localhost:8085"]
    async fn test_emulator_connection() {
        if !is_emulator_enabled() {
            println!("Skipping test - USE_PUBSUB_EMULATOR not set to true");
            return;
        }

        let _channel = emulator_transport()
            .await
            .expect("Failed to connect to emulator");

        println!("Successfully connected to PubSub emulator");
    }
}
