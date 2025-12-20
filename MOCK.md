# Mocking Google Cloud Services Without GCloud Account

This guide provides multiple approaches to mock Google Cloud PubSub services for testing without requiring a GCloud account or network connectivity to Google.

---

## Option 1: Google Cloud PubSub Emulator (Recommended)

Google provides an official local emulator that mimics PubSub behavior without requiring credentials.

### Setup

**Install via gcloud CLI (one-time setup, doesn't require account):**
```bash
gcloud components install pubsub-emulator
```

**Or use Docker (no gcloud needed):**
```bash
docker run -d -p 8085:8085 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
```

### Integration in Code

```rust
// In your test setup (tests/setup/mod.rs)
pub async fn create_emulator_channel() -> anyhow::Result<Channel> {
    // Connect to local emulator instead of pubsub.googleapis.com
    let endpoint = Channel::from_static("http://localhost:8085");
    Ok(endpoint.connect().await?)
}

pub async fn create_test_publisher() -> anyhow::Result<PubSubPublisher<NoAuth>> {
    let channel = create_emulator_channel().await?;
    // No auth needed for emulator
    Ok(PubSubPublisher::new(channel, "test-project"))
}

// Create a NoAuth interceptor for emulator
pub struct NoAuth;

impl tonic::service::Interceptor for NoAuth {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        Ok(request) // Pass through without adding auth headers
    }
}
```

### Test Example

```rust
#[cfg(test)]
mod pubsub_tests {
    use super::*;

    #[tokio::test]
    async fn test_publish_to_emulator() {
        // Create topic via emulator admin API or assume it exists
        let mut publisher = create_test_publisher().await.unwrap();

        let result = publisher.publish(
            "test-topic",
            b"test-data".to_vec(),
            Some("key-1".to_string()),
            HashMap::new(),
            false,
        ).await;

        assert!(result.is_ok());
    }
}
```

### Pros
- Official Google implementation - high fidelity
- No credentials or network access needed
- Supports all PubSub operations
- Easy Docker deployment for CI/CD

### Cons
- Requires gcloud CLI or Docker
- Additional process to manage

---

## Option 2: Mock gRPC Server with Tonic

Create a custom mock server implementing the PubSub gRPC service.

### Implementation

**Add to Cargo.toml [dev-dependencies]:**
```toml
[dev-dependencies]
mockito = "1.7.0"  # Already present
tokio = { version = "1.0", features = ["full", "test-util", "sync"] }
```

**Create src/pubsub/mock.rs:**
```rust
use tonic::{transport::Server, Request, Response, Status};
use crate::pubsub::api::google::pubsub::v1::{
    publisher_server::{Publisher, PublisherServer},
    subscriber_server::{Subscriber, SubscriberServer},
    PublishRequest, PublishResponse, PullRequest, PullResponse,
    PubsubMessage, ReceivedMessage, AcknowledgeRequest,
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Default)]
pub struct MockPublisher {
    pub published_messages: Arc<Mutex<Vec<PubsubMessage>>>,
}

#[tonic::async_trait]
impl Publisher for MockPublisher {
    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let req = request.into_inner();

        // Store messages for verification
        let mut messages = self.published_messages.lock().await;
        messages.extend(req.messages);

        // Return success with message IDs
        let message_ids = (0..req.messages.len())
            .map(|i| format!("msg-{}", i))
            .collect();

        Ok(Response::new(PublishResponse { message_ids }))
    }

    // Implement other required methods...
}

#[derive(Debug, Default)]
pub struct MockSubscriber {
    pub messages_to_return: Arc<Mutex<Vec<PubsubMessage>>>,
}

#[tonic::async_trait]
impl Subscriber for MockSubscriber {
    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> Result<Response<PullResponse>, Status> {
        let messages = self.messages_to_return.lock().await;

        let received_messages: Vec<ReceivedMessage> = messages
            .iter()
            .enumerate()
            .map(|(i, msg)| ReceivedMessage {
                ack_id: format!("ack-{}", i),
                message: Some(msg.clone()),
                delivery_attempt: 0,
            })
            .collect();

        Ok(Response::new(PullResponse { received_messages }))
    }

    async fn acknowledge(
        &self,
        _request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    // Implement other required methods...
}

// Helper to start mock server
pub async fn start_mock_pubsub_server() -> anyhow::Result<(String, Arc<MockPublisher>, Arc<MockSubscriber>)> {
    let publisher = Arc::new(MockPublisher::default());
    let subscriber = Arc::new(MockSubscriber::default());

    let addr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    let pub_clone = publisher.clone();
    let sub_clone = subscriber.clone();

    tokio::spawn(async move {
        Server::builder()
            .add_service(PublisherServer::new(pub_clone))
            .add_service(SubscriberServer::new(sub_clone))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });

    Ok((format!("http://{}", local_addr), publisher, subscriber))
}
```

### Test Usage

```rust
#[tokio::test]
async fn test_publish_with_mock_server() {
    let (addr, mock_publisher, _) = start_mock_pubsub_server().await.unwrap();

    // Connect to mock server
    let channel = Channel::from_shared(addr)
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut publisher = PubSubPublisher::new(channel, "test-project");

    publisher.publish(
        "test-topic",
        b"test-data".to_vec(),
        Some("key".to_string()),
        HashMap::new(),
        false,
    ).await.unwrap();

    // Verify
    let messages = mock_publisher.published_messages.lock().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].data, b"test-data");
}
```

### Pros
- No external dependencies
- Full control over responses
- Can simulate error conditions
- Fast execution

### Cons
- Must implement all gRPC methods
- Need to regenerate with `build_server(true)` in build.rs
- More code to maintain

---

## Option 3: Trait-Based Abstraction (Cleanest for Unit Tests)

Extract a trait for PubSub operations and provide mock implementations.

### Implementation

**Add to src/pubsub/srvc/mod.rs:**
```rust
#[async_trait::async_trait]
pub trait PubSubPublisherTrait: Send + Sync {
    async fn publish(
        &mut self,
        topic: &str,
        data: Vec<u8>,
        ordering_key: Option<String>,
        attributes: HashMap<String, String>,
        is_batch: bool,
    ) -> anyhow::Result<String>;
}

// Implement for existing struct
#[async_trait::async_trait]
impl<I: Interceptor + Send> PubSubPublisherTrait for PubSubPublisher<I> {
    async fn publish(
        &mut self,
        topic: &str,
        data: Vec<u8>,
        ordering_key: Option<String>,
        attributes: HashMap<String, String>,
        is_batch: bool,
    ) -> anyhow::Result<String> {
        // Existing implementation
    }
}

// Mock implementation for tests
#[cfg(test)]
pub struct MockPubSubPublisher {
    pub published: Arc<Mutex<Vec<(String, Vec<u8>, Option<String>, HashMap<String, String>)>>>,
    pub should_fail: bool,
}

#[cfg(test)]
#[async_trait::async_trait]
impl PubSubPublisherTrait for MockPubSubPublisher {
    async fn publish(
        &mut self,
        topic: &str,
        data: Vec<u8>,
        ordering_key: Option<String>,
        attributes: HashMap<String, String>,
        _is_batch: bool,
    ) -> anyhow::Result<String> {
        if self.should_fail {
            anyhow::bail!("Mock publish failed");
        }

        self.published.lock().await.push((
            topic.to_string(),
            data,
            ordering_key,
            attributes,
        ));

        Ok("1".to_string())
    }
}
```

### Update Your Sink to Use the Trait

**In src/sink/mod.rs:**
```rust
pub enum SinkProvider {
    PubSub(Box<dyn PubSubPublisherTrait>),
    // ... other variants
}

impl SinkProvider {
    pub async fn send(&mut self, event: SinkEvent) -> anyhow::Result<()> {
        match self {
            SinkProvider::PubSub(publisher) => {
                publisher.publish(
                    &event.topic,
                    event.data,
                    event.ordering_key,
                    event.attributes,
                    false,
                ).await?;
            }
        }
        Ok(())
    }
}
```

### Test Usage

```rust
#[tokio::test]
async fn test_sink_with_mock_publisher() {
    let mock = MockPubSubPublisher {
        published: Arc::new(Mutex::new(Vec::new())),
        should_fail: false,
    };

    let mut sink = SinkProvider::PubSub(Box::new(mock.clone()));

    sink.send(SinkEvent {
        topic: "test-topic".to_string(),
        data: b"test".to_vec(),
        ordering_key: None,
        attributes: HashMap::new(),
    }).await.unwrap();

    let published = mock.published.lock().await;
    assert_eq!(published.len(), 1);
}
```

### Pros
- Clean unit testing
- No external processes
- Fast test execution
- Easy to simulate errors

### Cons
- Requires refactoring existing code
- Adds abstraction layer

---

## Option 4: Testcontainers with PubSub Emulator (Best for CI/CD)

Use the `testcontainers` crate to automatically manage Docker containers.

### Setup

**Add to Cargo.toml [dev-dependencies]:**
```toml
testcontainers = "0.15"
```

### Implementation

```rust
use testcontainers::{clients, images::generic::GenericImage, Docker};

#[tokio::test]
async fn test_with_pubsub_container() {
    let docker = clients::Cli::default();
    let pubsub = GenericImage::new("gcr.io/google.com/cloudsdktool/google-cloud-cli", "emulators")
        .with_exposed_port(8085)
        .with_wait_for(testcontainers::core::WaitFor::message_on_stdout("Server started"))
        .with_cmd(vec!["gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085"]);

    let container = docker.run(pubsub);
    let port = container.get_host_port_ipv4(8085);

    // Connect to emulator
    let endpoint = format!("http://localhost:{}", port);
    let channel = Channel::from_shared(endpoint)?.connect().await?;

    // Run tests...
}
```

### Pros
- Automatic container lifecycle management
- Perfect for CI/CD pipelines
- No manual setup required
- High fidelity with real emulator

### Cons
- Requires Docker
- Slower than pure mocks
- Additional dependency

---

## Recommended Approach

For this project, we recommend a **hybrid approach**:

1. **For unit tests**: Use **Option 3 (Trait-Based Abstraction)**
   - Fast, no external dependencies
   - Perfect for testing business logic

2. **For integration tests**: Use **Option 1 (PubSub Emulator)**
   - High fidelity with real PubSub behavior
   - Can run in Docker for CI/CD
   - Test actual serialization/deserialization

---

## Quick Start Implementations

### Implementation 1: PubSub Emulator Integration

See the integration test updates in `tests/setup/mod.rs` and `tests/integration_test.rs` for a working example using the PubSub emulator.

**Key changes:**
- Added `NoAuth` interceptor that bypasses authentication
- Created `create_emulator_channel()` to connect to local emulator
- Updated test helpers to support both emulator and real GCP modes
- Added environment variable `USE_PUBSUB_EMULATOR` to switch between modes

**Running tests with emulator:**
```bash
# Start emulator
docker run -d -p 8085:8085 --name pubsub-emulator \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085

# Run tests
USE_PUBSUB_EMULATOR=true cargo test --test integration_test

# Cleanup
docker stop pubsub-emulator && docker rm pubsub-emulator
```

### Implementation 2: Trait-Based Abstraction (Future Work)

Refactoring the sink/source modules to use trait-based abstractions would enable clean unit testing without external dependencies.

### Implementation 3: Mock gRPC Server (Future Work)

Creating a full mock gRPC server would provide the most flexibility for testing edge cases and error conditions.

---

## Environment Variables

- `USE_PUBSUB_EMULATOR`: Set to `true` to use local emulator instead of real GCP
- `PUBSUB_EMULATOR_HOST`: Override default emulator endpoint (default: `localhost:8085`)
- `MSTREAM_TEST_AUTH_TOKEN`: Bearer token for real GCP testing (only when not using emulator)
- `PUBSUB_TOPIC`: Topic name for publishing
- `PUBSUB_SUBSCRIPTION`: Subscription name for pulling
- `PUBSUB_SCHEMA`: Schema resource ID

---

## CI/CD Integration

Add the following to your GitHub Actions workflow:

```yaml
- name: Start PubSub Emulator
  run: |
    docker run -d -p 8085:8085 --name pubsub-emulator \
      gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
      gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
    sleep 5  # Wait for emulator to start

- name: Run Integration Tests
  env:
    USE_PUBSUB_EMULATOR: true
  run: cargo test --test integration_test

- name: Stop PubSub Emulator
  if: always()
  run: docker stop pubsub-emulator && docker rm pubsub-emulator
```

---

## References

- [Google Cloud PubSub Emulator Documentation](https://cloud.google.com/pubsub/docs/emulator)
- [Tonic gRPC Framework](https://github.com/hyperium/tonic)
- [Testcontainers Rust](https://docs.rs/testcontainers/latest/testcontainers/)
