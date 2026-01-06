# Technical Reference

## Schema and Encoding

mstream uses a clear input/output encoding model:

- **Input Encoding**: The format data is received in (e.g., `avro` from Kafka)
- **Output Encoding**: The format data is transformed to before the next step (e.g., `json` for a sink)

```toml
[[connectors]]
# Read Avro from Kafka -> Convert to JSON -> Write to MongoDB as BSON
source = { service_name = "kafka", resource = "events", input_encoding = "avro", output_encoding = "json" }
sinks = [
    { service_name = "mongo", resource = "events_archive", output_encoding = "bson" }
]
```

### Format Conversion Matrix

| Source Format | Target Format | Notes |
|---------------|---------------|-------|
| BSON | BSON | Direct passthrough |
| BSON | JSON | Serializes BSON to JSON |
| BSON | Avro | Requires `schema_id` with Avro schema |
| JSON | JSON | Passthrough |
| JSON | BSON | Parses JSON to BSON |
| JSON | Avro | Requires `schema_id` with Avro schema |
| Avro | Avro | Validates against schema if provided |
| Avro | JSON | Deserializes Avro to JSON |
| Avro | BSON | Converts Avro records to BSON documents |

### Schema Inheritance Rules

The `schema_id` field is optional in most cases and follows these rules:

1. **Avro encoding**: A schema reference is required whenever Avro encoding is used
2. **Schema inheritance**: If no `schema_id` is specified, the component uses the schema from the most recent previous step
3. **Source schema**: If a schema is defined at the source, it applies to all steps unless overridden

## Batch Processing

For high-throughput scenarios, enable batch processing to group events:

```toml
[[connectors]]
name = "high-volume-sync"
batch = { kind = "count", size = 100 }
source = { service_name = "mongo", resource = "logs", output_encoding = "bson" }
sinks = [
    { service_name = "kafka", resource = "logs-batch", output_encoding = "json" }
]
```

### Considerations

| Aspect | Notes |
|--------|-------|
| **MongoDB sinks** | Output encoding must be `bson` to enable optimized bulk writes |
| **Non-MongoDB sinks** | Batch is emitted as an array of events in the configured encoding |
| **Middleware** | Must be capable of handling batched data (arrays) |
| **Schema validation** | Applied to each event in the batch individually |
| **Memory** | Batch size directly affects memory usage — tune based on available resources |
| **Latency** | Lower batch sizes reduce latency for time-sensitive events |

### MongoDB Batch Format

When batch processing is enabled for a MongoDB sink, events are stored as a single document:

```json
{
  "items": [
    { "id": 1, "name": "Event 1" },
    { "id": 2, "name": "Event 2" }
  ]
}
```

## Middleware

Transform data on the fly using HTTP services or embedded scripts.

### HTTP Middleware

Send events to an external API for processing:

```toml
[[services]]
provider = "http"
name = "transform-api"
host = "http://transform-api:8080"
max_retries = 3
timeout_sec = 20

[[connectors]]
middlewares = [
    { service_name = "transform-api", resource = "enrich-user", output_encoding = "json" }
]
```

### UDF (Rhai) Middleware

Write custom transformation logic in [Rhai](https://rhai.rs) scripts:

```toml
[[services]]
provider = "udf"
name = "script-engine"
engine = { kind = "rhai" }
script_path = "./scripts"

[[connectors]]
middlewares = [
    { service_name = "script-engine", resource = "anonymize.rhai", output_encoding = "json" }
]
```

**Example Script (`anonymize.rhai`):**

```rhai
fn transform(data, attributes) {
    // Mask email address
    if "email" in data {
        data.email = mask_email(data.email);
    }
    
    // Add timestamp
    data.processed_at = timestamp_ms();
    
    result(data, attributes)
}
```

## PubSub Message Attributes

When using Google Cloud PubSub as a sink, mstream adds the following attributes:

| Attribute | Description |
|-----------|-------------|
| `operation_type` | Event type: `insert`, `update`, `delete` |
| `database` | MongoDB database name (for Mongo sources) |
| `collection` | MongoDB collection name (for Mongo sources) |

## HTTP Service Configuration

| Field | Description | Default |
|-------|-------------|---------|
| `host` | Base URL for the HTTP service | — |
| `max_retries` | Number of retry attempts | `5` |
| `base_backoff_ms` | Initial backoff delay between retries | `1000` |
| `connection_timeout_sec` | Connection timeout | `30` |
| `timeout_sec` | Request timeout | `30` |
| `tcp_keepalive_sec` | TCP keepalive interval | `300` |

## Kafka Configuration

Kafka services support all [librdkafka configuration options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) as key-value pairs:

```toml
[[services]]
provider = "kafka"
name = "kafka-cloud"
offset_seek_back_seconds = 3600  # Optional: seek back 1h for source
"bootstrap.servers" = "your-kafka-broker:9092"
"security.protocol" = "SASL_SSL"
"sasl.mechanism" = "PLAIN"
"sasl.username" = "env:MSTREAM_KAFKA_USERNAME"
"sasl.password" = "env:MSTREAM_KAFKA_PASSWORD"
"client.id" = "mstream-client"
"group.id" = "mstream-group"
```

Use the `env:` prefix to read values from environment variables.