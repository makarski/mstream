# mstream

[![CodeScene Average Code Health](https://codescene.io/projects/45886/status-badges/average-code-health)](https://codescene.io/projects/45886)

A lightweight, high-performance data streaming bridge that connects sources to sinks with powerful transformation capabilities.

**mstream** simplifies building data pipelines by providing a configurable bridge between systems like MongoDB, Kafka, and PubSub. It handles format conversion, schema validation, and data transformation out of the box.

## Web UI

Access the dashboard at `http://localhost:8719/`.

![Dashboard](docs/images/dashboard.png)

![Job Details](docs/images/job-details.png)

- **Dashboard** — Overview of all jobs and services
- **Job Management** — Create, stop, and restart jobs
- **Service Management** — View and manage service configurations
- **Pipeline Visualization** — Visual representation of data flow

## Features

- **Universal Connectivity** — Stream data between MongoDB, Kafka, and Google Cloud PubSub
- **Zero-Code Transformations** — Convert between BSON, JSON, and Avro formats automatically
- **Powerful Middleware** — Transform data in-flight using HTTP services or embedded Rhai scripts
- **Schema Validation** — Enforce data quality with Avro schema validation
- **Schema Introspection** — Discover collection schemas without exposing sensitive data
- **Batch Processing** — Optimized high-throughput batch handling
- **Checkpoint Persistence** — Resume streaming from the last processed position after restarts

## Installation

### Using Docker (recommended)

```bash
cp mstream-config.toml.example mstream-config.toml
# edit config as needed
make docker-up
```

### Building from Source

```bash
cargo build --release
RUST_LOG=info ./target/release/mstream
```

## Quick Start

Create a `mstream-config.toml` file:

```toml
[[services]]
provider = "mongodb"
name = "mongo-local"
connection_string = "mongodb://localhost:27017"
db_name = "app_db"
write_mode = "insert"  # optional: "insert" (default) or "replace"

[[services]]
provider = "kafka"
name = "kafka-local"
"bootstrap.servers" = "localhost:9092"

[[connectors]]
enabled = true
name = "users-sync"
source = { service_name = "mongo-local", resource = "users", output_encoding = "json" }
sinks = [
    { service_name = "kafka-local", resource = "users-topic", output_encoding = "json" }
]
```

Run mstream:

```bash
RUST_LOG=info ./mstream
```

### Examples

- [mongo_to_kafka.toml](examples/mongo_to_kafka.toml) — MongoDB Change Stream → Kafka
- [kafka_to_mongo.toml](examples/kafka_to_mongo.toml) — Kafka → MongoDB
- [rhai/](examples/rhai) — Rhai transformation scripts (includes [append-only MongoDB pattern](examples/rhai/README.md#append-only-mongodb-sink))

## Core Concepts

```mermaid
graph LR
    Source[Source] --> |Event| Middleware[Middleware]
    Middleware --> |Transformed Event| Sink[Sink]
```

- **Source** — Origin of data (MongoDB Change Stream, Kafka topic, PubSub subscription)
- **Middleware** — Optional transformation step (HTTP service or Rhai script)
- **Sink** — Destination for data (Kafka topic, MongoDB collection, PubSub topic, HTTP endpoint)

## Supported Integrations

| Type | Service | Notes |
|------|---------|-------|
| **Source** | MongoDB | Change Streams (v6.0+) |
| **Source** | Kafka | Consumer groups, offset management |
| **Source** | Google PubSub | Subscriptions |
| **Sink** | MongoDB | Insert or Replace (upsert by `_id`) |
| **Sink** | Kafka | Producer |
| **Sink** | Google PubSub | Publisher |
| **Sink** | HTTP | POST requests |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MSTREAM_API_PORT` | Port for the REST API and Web UI | `8719` |
| `MSTREAM_ENC_KEY` | Hex-encoded AES-256 key for service encryption | — |
| `RUST_LOG` | Log level (`error`, `warn`, `info`, `debug`, `trace`) | `info` |

## Logs Configuration

Configure the in-memory log buffer for the Logs API:

```toml
[system.logs]
buffer_capacity = 10000       # Max entries in ring buffer (default: 10000)
stream_preload_count = 100    # Entries sent on SSE connect (default: 100)
```

## Management API

REST API available at port `8719` (configurable via `MSTREAM_API_PORT`).

### Jobs

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/jobs` | List all jobs |
| `POST` | `/jobs` | Create a new job |
| `POST` | `/jobs/{name}/stop` | Stop a job |
| `POST` | `/jobs/{name}/restart` | Restart a job |
| `GET` | `/jobs/{name}/checkpoints` | List checkpoint history for a job |

### Services

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/services` | List all services |
| `POST` | `/services` | Create a new service |
| `GET` | `/services/{name}` | Get service details |
| `DELETE` | `/services/{name}` | Remove a service (if not in use) |
| `GET` | `/services/{name}/resources` | List resources for a service |
| `GET` | `/services/{name}/schema` | Introspect schema for a resource |
| `POST` | `/schema/fill` | Generate synthetic data from a schema |

#### List Service Resources

List available resources (collections, topics) for a service:

```
GET /services/{name}/resources
```

**Response:**
```json
{
  "service_name": "mongo-local",
  "resources": [
    { "name": "users", "resource_type": "collection" },
    { "name": "orders", "resource_type": "collection" }
  ]
}
```

> **Note:** Currently supports MongoDB services only. System collections (prefixed with `system.`) are excluded.

#### Schema Introspection

Introspect the schema of a MongoDB collection by sampling documents:

```
GET /services/{name}/schema?resource=users&sample_size=100
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resource` | Collection name to introspect (required) | — |
| `sample_size` | Number of documents to sample (max: 1000) | 100 |

**Response:** Returns schema variants grouped by document shape. Documents with conflicting field types are grouped separately.

```json
[
  {
    "share_percent": 95.0,
    "sample_count": 95,
    "schema": {
      "type": "object",
      "properties": {
        "name": { "type": "string" },
        "age": { "type": "integer" },
        "email": { "type": "string" }
      },
      "required": ["name", "age"]
    }
  }
]
```

> **Note:** Schema introspection samples documents without exposing actual values, making it safe for compliance-sensitive environments where you need to build transformation pipelines without viewing PII.

#### Schema Fill

Generate synthetic data from a JSON Schema via `POST /schema/fill`. Pass `{"schema": <JsonSchema>}` and receive a document with realistic values based on schema hints (`enum`, `format`, `minimum`/`maximum`) and field name heuristics (e.g., `email` → email address, `_id` → ObjectId, `created_at` → datetime).

Useful for testing transformation scripts in the Playground without exposing real data.

### Logs

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/logs` | Query log entries with optional filters |
| `GET` | `/logs/stream` | SSE stream of live log entries |

#### Query Parameters for `/logs`

| Parameter | Description |
|-----------|-------------|
| `job_name` | Filter by job name |
| `level` | Minimum log level (`trace`, `debug`, `info`, `warn`, `error`) |
| `limit` | Maximum number of entries to return (default: 100) |
| `since` | Only entries after this timestamp (RFC3339 format) |

#### Query Parameters for `/logs/stream`

| Parameter | Description |
|-----------|-------------|
| `job_name` | Filter by job name |
| `level` | Minimum log level |
| `initial` | Number of historical entries to send initially (default: 100) |

### Transform (Playground)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/transform/run` | Test a Rhai script on a sample document |

#### Request Body

```json
{
  "script": "fn transform(data, attributes) { result(data, attributes) }",
  "payload": "{\"email\": \"john@example.com\"}",
  "attributes": {"source": "playground"}
}
```

- `script` — Rhai script with `transform(data, attributes)` function (required)
- `payload` — JSON string of the input document or array (required)
- `attributes` — Optional key-value metadata passed to the script

> **Note:** For batch processing, pass a JSON array as the payload (e.g., `"[{...}, {...}]"`). The script receives the array directly and can iterate over it.

#### Response

```json
{
  "message": "success",
  "item": {
    "document": {"email": "j***@example.com"},
    "attributes": {"source": "playground", "processed": "true"}
  }
}
```

## Checkpoints

Checkpoints allow connectors to resume from their last processed position after a restart, preventing data loss or reprocessing.

### System Configuration

Enable checkpoints globally by configuring a MongoDB backend:

```toml
[system.checkpoints]
service_name = "mongo-local"  # Must reference an existing MongoDB service
resource = "mstream-checkpoints"  # Collection to store checkpoint data
```

### Connector Configuration

Enable checkpointing per connector:

```toml
[[connectors]]
name = "users-sync"
checkpoint = { enabled = true }
source = { service_name = "mongo-local", resource = "users", output_encoding = "json" }
sinks = [
    { service_name = "kafka-local", resource = "users-topic", output_encoding = "json" }
]
```

### Supported Sources

| Source | Checkpoint Data | Notes |
|--------|-----------------|-------|
| MongoDB Change Stream | Resume token | Automatic resume on restart |
| Kafka | Topic, partition, offset | `offset_seek_back_seconds` takes priority over checkpoint when set |
| Google PubSub | — | Not yet supported |

### Behavior

- When a connector restarts with checkpoints enabled, it resumes from the last saved position
- For Kafka: if `offset_seek_back_seconds` is configured, it takes priority over the checkpoint (useful for reprocessing historical data)
- Checkpoints are saved after all sinks successfully process each event

## Documentation

- [Persistence](docs/persistence.md) — Job and service persistence configuration
- [Technical Reference](docs/reference.md) — Encoding, schemas, batch processing, middleware

## Development

```bash
make docker-db-up    # Start MongoDB cluster
make run-debug       # Run with debug logging
make unit-tests      # Run unit tests
make integration-tests  # Run integration tests
```

## License

MIT License — see [LICENSE](LICENSE) for details.
