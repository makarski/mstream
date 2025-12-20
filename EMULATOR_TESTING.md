# Testing with PubSub Emulator

This guide explains how to run tests using the Google Cloud PubSub emulator instead of requiring real GCP credentials.

## Why Use the Emulator?

- No GCP account or credentials needed
- Faster test execution (local, no network latency)
- Free to use
- Deterministic testing environment
- Can run in CI/CD without GCP setup

## Prerequisites

Choose one of the following:

### Option 1: Docker (Recommended)
- Docker installed and running

### Option 2: gcloud CLI
- gcloud CLI installed
- Run: `gcloud components install pubsub-emulator`

## Quick Start

### 1. Start the Emulator

**Using Docker:**
```bash
docker run -d -p 8085:8085 --name pubsub-emulator \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
```

**Using gcloud CLI:**
```bash
gcloud beta emulators pubsub start --host-port=localhost:8085
```

### 2. Set Environment Variable

```bash
export USE_PUBSUB_EMULATOR=true
```

### 3. Run Tests

**Run emulator-specific tests:**
```bash
cargo test --test pubsub_emulator_test -- --ignored
```

**Run integration tests with emulator:**
```bash
# Note: Still requires MongoDB, but PubSub parts will use emulator
export MONGO_URI="mongodb://localhost:27017/?replicaSet=rs0"
export PUBSUB_SUBSCRIPTION="projects/test-project/subscriptions/test-subscription"
cargo test --test integration_test -- --ignored
```

### 4. Cleanup

**Docker:**
```bash
docker stop pubsub-emulator && docker rm pubsub-emulator
```

**gcloud CLI:**
Press `Ctrl+C` in the terminal where the emulator is running

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `USE_PUBSUB_EMULATOR` | Yes | `false` | Set to `true` or `1` to enable emulator mode |
| `PUBSUB_EMULATOR_HOST` | No | `http://localhost:8085` | Override emulator endpoint |
| `PUBSUB_SUBSCRIPTION` | Yes | N/A | Subscription name (e.g., `projects/test-project/subscriptions/test-sub`) |
| `PUBSUB_TOPIC` | Yes | N/A | Topic name (e.g., `projects/test-project/topics/test-topic`) |
| `MONGO_URI` | Yes* | N/A | MongoDB connection string (*only for integration tests) |

## Setting Up Topics and Subscriptions

The emulator doesn't persist data between restarts. You'll need to create topics and subscriptions each time you start it.

### Using gcloud CLI

```bash
# Set emulator host
export PUBSUB_EMULATOR_HOST=localhost:8085

# Create topic
gcloud pubsub topics create test-topic --project=test-project

# Create subscription
gcloud pubsub subscriptions create test-subscription \
  --topic=test-topic \
  --project=test-project
```

### Using REST API

The emulator exposes an HTTP API on port 8085:

```bash
# Create topic
curl -X PUT "http://localhost:8085/v1/projects/test-project/topics/test-topic"

# Create subscription
curl -X PUT "http://localhost:8085/v1/projects/test-project/subscriptions/test-subscription" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "projects/test-project/topics/test-topic"
  }'
```

## Example Test Workflow

Complete example for running tests with emulator:

```bash
# 1. Start emulator
docker run -d -p 8085:8085 --name pubsub-emulator \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085

# 2. Wait for emulator to be ready (3-5 seconds)
sleep 5

# 3. Create test topics and subscriptions
export PUBSUB_EMULATOR_HOST=localhost:8085
gcloud pubsub topics create test-topic --project=test-project
gcloud pubsub subscriptions create test-subscription \
  --topic=test-topic \
  --project=test-project

# 4. Run tests
export USE_PUBSUB_EMULATOR=true
export PUBSUB_SUBSCRIPTION="projects/test-project/subscriptions/test-subscription"
cargo test --test pubsub_emulator_test -- --ignored

# 5. Cleanup
unset USE_PUBSUB_EMULATOR
unset PUBSUB_EMULATOR_HOST
docker stop pubsub-emulator && docker rm pubsub-emulator
```

## Using in CI/CD

Example GitHub Actions workflow:

```yaml
name: Tests with PubSub Emulator

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      pubsub-emulator:
        image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
        ports:
          - 8085:8085
        options: >-
          --health-cmd "curl -f http://localhost:8085 || exit 1"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        # Start emulator as container command
        entrypoint: gcloud
        command: ["beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085"]

    steps:
      - uses: actions/checkout@v3

      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable

      - name: Setup PubSub resources
        run: |
          # Install gcloud
          curl https://sdk.cloud.google.com | bash
          source $HOME/google-cloud-sdk/path.bash.inc

          export PUBSUB_EMULATOR_HOST=localhost:8085
          gcloud pubsub topics create test-topic --project=test-project
          gcloud pubsub subscriptions create test-subscription \
            --topic=test-topic --project=test-project

      - name: Run tests
        env:
          USE_PUBSUB_EMULATOR: true
          PUBSUB_SUBSCRIPTION: projects/test-project/subscriptions/test-subscription
        run: cargo test --test pubsub_emulator_test
```

## Limitations

### Current Implementation

- **App listener doesn't support emulator**: The `start_app_listener` function in integration tests still connects to real GCP PubSub. Full emulator support would require refactoring the provisioning/registry code.
- **Test utilities only**: Emulator integration is currently only available in the test suite (`#[cfg(test)]`), not in the main application code.

### What Works

- ✅ Pulling messages from PubSub in tests
- ✅ Publishing messages via direct client usage
- ✅ Acknowledgments
- ✅ Basic PubSub operations

### What Doesn't Work Yet

- ❌ Full integration test with app listener (still requires real GCP)
- ❌ Schema service emulation
- ❌ Running the main mstream app against emulator

## Future Improvements

To fully support the emulator in all tests:

1. **Refactor Registry**: Update `src/provision/registry.rs` to support configurable PubSub endpoints
2. **Configuration**: Add emulator mode to main config structure
3. **Schema Service**: Implement local schema management for tests
4. **Test Containers**: Use `testcontainers` crate for automatic lifecycle management

See `MOCK.md` for additional mocking strategies and implementation options.

## Troubleshooting

### Connection Refused
- **Problem**: `failed to connect to PubSub emulator: transport error`
- **Solution**: Ensure emulator is running and accessible on port 8085
  ```bash
  curl http://localhost:8085
  # Should return some JSON response
  ```

### Topic/Subscription Not Found
- **Problem**: `NOT_FOUND: Resource not found`
- **Solution**: Create topics and subscriptions before running tests (see setup section above)

### Wrong Endpoint
- **Problem**: Tests still trying to connect to `pubsub.googleapis.com`
- **Solution**: Ensure `USE_PUBSUB_EMULATOR=true` is set
  ```bash
  echo $USE_PUBSUB_EMULATOR  # Should output: true
  ```

### Port Already in Use
- **Problem**: `Error: Port 8085 already in use`
- **Solution**: Stop existing emulator instance
  ```bash
  docker stop pubsub-emulator && docker rm pubsub-emulator
  # Or find and kill the process using port 8085
  lsof -ti:8085 | xargs kill
  ```

## Resources

- [Google Cloud PubSub Emulator Documentation](https://cloud.google.com/pubsub/docs/emulator)
- [PubSub gRPC API Reference](https://cloud.google.com/pubsub/docs/reference/rpc)
- [Tonic gRPC Framework](https://github.com/hyperium/tonic)

## Related Documentation

- `MOCK.md` - Comprehensive guide to all mocking strategies
- `TestSummary.md` - Current test coverage analysis
- `tests/pubsub_emulator_test.rs` - Example emulator test
