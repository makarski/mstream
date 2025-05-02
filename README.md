# mstream

A lightweight, configurable data streaming bridge that connects sources to sinks with powerful transformation capabilities.

## Overview

mstream creates connectors that move data between various systems with flexible transformation options:

1. **Source → Sink Streaming**: Connect data from [MongoDB change streams](https://www.mongodb.com/docs/manual/changeStreams/), Kafka, or PubSub to multiple destinations
2. **Format Conversion**: Seamlessly convert between BSON, JSON, and Avro formats
3. **Schema Filtering**: Apply schema-based field filtering to extract only needed data
4. **Transformation Pipeline**: Process data through configurable middleware chains before delivery

## Key Features

- **Multiple Destination Support**: Stream from one source to multiple sinks simultaneously
- **Middleware Processing**: Transform data through HTTP services before delivery
- **Schema Validation**: Use AVRO schemas to validate and filter document fields
- **Format Flexibility**: Convert between common data formats (BSON, JSON, Avro)

## Supported Integrations

**Sources:**

- MongoDB Change Streams (v6.0+)
- Kafka topics
- Google Cloud PubSub topics

**Sinks:**

- MongoDB collections
- Kafka topics
- Google Cloud PubSub topics
- HTTP endpoints

Configuration is managed through a simple TOML file. See the [example configuration](./mstream-config.toml.example).

_Note: The production configuration file should be named `mstream-config.toml` and placed in the same directory as the binary._

### Components

```mermaid
graph TB
    subgraph mstream[mstream]
        handler@{ shape: procs, label: "Listeners"}
        %% kafka_consumer[Kafka Consumer]
        schema_cache[Schema Cache]
        encoder[["`Format Encoder
        (Json, Avro, Bson)`"]]
        middleware[["`Middlewares
        (HTTP Transform)`"]]
    end

    subgraph sources[Sources]
        kafka_source@{ shape: das, label: "Kafka Source Topic" }
        pubsub_source_topic@{ shape: das, label: "PubSub Source Topic" }
        mongodb_source@{shape: database, label: "MongoDB Change Stream" }
    end


    subgraph sinks[Sinks]
        kafka_sink@{ shape: das, label: "Kafka Sink Topic" }
        mongodb_target[(MongoDB Sink)]
        pubsub_sink_topic@{ shape: das, label: "PubSub Topic" }
        http_sink["Web Server"]
    end


    subgraph schemas[Schemas]
        pubsub_schema_registry@{label: "PubSub Schema Registry (Avro)" }
        mongodb_schema_storage@{label: "Schema definition stored in Mongo"}
    end


    mongodb_source -.-> handler
    kafka_source -...-> handler
    pubsub_source_topic -.-> handler
    pubsub_schema_registry --> schema_cache
    mongodb_schema_storage --> schema_cache

    handler --> middleware
    middleware --> encoder
    schema_cache <-->handler

    encoder -.-> mongodb_target
    encoder -.-> pubsub_sink_topic
    encoder -.-> kafka_sink
    encoder --> |HTTP POST| http_sink
```

### Schema and Encoding Support

mstream provides powerful schema filtering and format conversion capabilities to transform data as it flows through your pipeline.

#### Schema Filtering and Multiple Schema Support

Schemas in mstream serve two important purposes:
1. **Field Filtering**: Extract only the fields you need from source documents
2. **Format Validation**: Ensure data conforms to expected structures

Each connector can define multiple schemas that can be referenced by ID at different points in the pipeline:

```toml
[[connectors]]
name = "multi-schema-example"
# Define all schemas used in this connector
schemas = [
    { id = "basic_schema", service_name = "pubsub-example", resource = "projects/your-project/schemas/basic-user" },
    { id = "extended_schema", service_name = "mongodb-local", resource = "schema_collection" },
]
```

When a schema is applied to a document, only fields defined in the schema will be included in the output. For example:

**Schema definition:**
```json
{
  "type": "record",
  "name": "User",
  "fields": [{ "name": "name", "type": "string" }]
}
```

**Source document:**
```json
{
  "name": "John",
  "age": 30,
  "last_name": "Doe"
}
```

**Result after filtering:**
```json
{
  "name": "John"
}
```

#### Encoding and Format Conversion

mstream v0.16.0+ uses a clear input/output encoding model for data flow:

- **Input Encoding**: The format data is received in
- **Output Encoding**: The format data is transformed to before passing to the next step

Important rules:
- **Input encoding** is only required for Kafka and PubSub sources
- Each step in the pipeline automatically uses the previous step's **output_encoding** as its input
- Format conversions are explicitly defined by the input/output encoding pair at each step

```toml
[[connectors]]
name = "format-conversion-example"
# Source with explicit input and output encodings
# *mind that toml does not support new lines for [inline tables](https://toml.io/en/v1.0.0#inline-table)
source = {
    service_name = "kafka-local",
    resource = "raw_data",
    input_encoding = "json",    # Required for Kafka sources
    output_encoding = "bson",   # Will be converted from JSON to BSON
    schema_id = "raw_schema"    # Optional schema reference
}
```

#### Schema Inheritance

The `schema_id` field is optional in most cases and follows these rules:

1. **Avro encoding**: A schema reference is required whenever Avro encoding is used
2. **Schema inheritance**: If no schema_id is specified, the component will use the schema defined at the most recent previous step
3. **Source schema**: If a schema is defined at the source, it will be applied to all steps unless overridden

This allows for flexible pipelines where data can be filtered differently for different destinations:

```toml
[[connectors]]
name = "schema-inheritance-example"
# Define schemas
schemas = [
    { id = "base_schema", service_name = "pubsub-example", resource = "projects/your-project/schemas/base" },
    { id = "analytics_schema", service_name = "pubsub-example", resource = "projects/your-project/schemas/analytics" },
]

# Source uses base_schema
source = { service_name = "mongodb-local", resource = "users", output_encoding = "bson", schema_id = "base_schema"  # This schema will be inherited by default
}

# Middlewares and sinks can override the schema
middlewares = [
    # Uses base_schema by inheritance (no schema_id specified)
    { service_name = "http-local", resource = "normalize", output_encoding = "json" },

    # Overrides with a different schema
    { service_name = "http-local", resource = "analyze", schema_id = "analytics_schema", output_encoding = "json" },
]

sinks = [
    # Uses analytics_schema from the last middleware
    { service_name = "kafka-local", resource = "analytics_topic", output_encoding = "json" },

    # Explicitly uses base_schema
    { service_name = "mongodb-local", resource = "users_copy", output_encoding = "bson", schema_id = "base_schema" },
]
```

#### Supported Format Conversions

mstream supports these format conversions throughout the pipeline:

| Source Format | Target Format | Notes                                       |
|---------------|---------------|---------------------------------------------|
| BSON          | BSON          | Direct passthrough                          |
| BSON          | JSON          | Serializes BSON to JSON                     |
| BSON          | Avro          | Requires schema_id with Avro schema         |
| JSON          | JSON          | Passthrough                                 |
| JSON          | BSON          | Parses JSON to BSON                         |
| JSON          | Avro          | Requires schema_id with Avro schema         |
| Avro          | Avro          | Validates against schema if provided        |
| Avro          | JSON          | Deserializes Avro to JSON                   |
| Avro          | BSON          | Converts Avro records to BSON documents     |
| Other         | Other         | Binary passthrough for custom formats       |

Conversions between "Other" encoding and structured formats (JSON/BSON/Avro) are not supported directly but can be implemented using middleware services.

### Middleware Support

The mstream application supports middleware processing for data transformation before sending to sinks. This powerful feature allows you to modify, enrich, or transform data as it passes through the pipeline.

#### Middleware Functionality

Middleware components sit between the source and sink, allowing for data transformation or enrichment. Each middleware:

- Receives a source event with its payload
- Processes the data (via HTTP POST to external services)
- Returns a modified version of the event for further processing

Multiple middlewares can be chained together to create complex transformation pipelines.

```mermaid
graph LR
    Source --> Middleware1
    Middleware1 --> Middleware2
    Middleware2 --> Middleware3
    Middleware3 --> Sink1
    Middleware3 --> Sink2
    Middleware3 --> Sink3
```

#### Configuring Middlewares

In your connector configuration, you can specify one or more middlewares:

```toml
[[connectors]]
name = "employee-connector"
source = { service_name = "mongodb-local", resource = "employees", output_encoding = "bson" }
# Add middleware transformations
middlewares = [
    # resource = "transform" means that the middleware will execute a http post request to /transform endpoint
    { service_name = "http-local", resource = "transform", output_encoding = "json" },
    { service_name = "http-local", resource = "enrich", output_encoding = "json" },
]
sinks = [
    { service_name = "kafka-local", resource = "test", output_encoding = "json" },
    { service_name = "mongodb-local", resource = "employees-copy", output_encoding = "bson" },
]
```

#### HTTP Middleware

The currently implemented middleware type is HTTP, which:

1. Sends the event data to an external HTTP endpoint
2. Receives the transformed data as the response
3. Passes the transformed data to the next middleware or sink

Each middleware can specify its own encoding format, allowing for flexible transformation chains that convert between different data formats as needed.

#### Data Flow with Middleware

1. Source produces an event
2. Event optionally passes through schema filtering
3. Event sequentially passes through each configured middleware
4. The final transformed event is sent to all configured sinks

This middleware capability is especially useful for:

- Data enrichment from external services
- Complex transformations beyond simple field filtering
- Normalization of data across different sources
- Custom business logic implementation
- Encoding conversions that are not natively supported by mstream, i.e. **protobuf to json**

### Mongo Event Processing

**Supported Sources**

- [MongoDB Change Stream Events](https://www.mongodb.com/docs/v6.0/reference/change-events/)
  - Insert document
  - Update document
  - Delete document
- Kafka Messages

**The worker will report an error and stop execution for MongoDB events**

- Invalidate stream
- Drop collection
- Drop database

#### Message Structure

A processed change stream is transformed into a pubsub message with the following structure:

**[Attributes](https://cloud.google.com/pubsub/docs/publisher#using-attributes)**

| attribute name | attribute value                          |
| -------------- | ---------------------------------------- |
| operation_type | event type: `insert`, `update`, `delete` |
| database       | mongodb database name                    |
| collection     | mongodb collection name                  |

Attributes can be used to configure fine-grained subscriptions. For more details see [documentation](https://cloud.google.com/pubsub/docs/subscription-message-filter#filtering_syntax)

### Running

```sh
# Spawn mongo cluster in docker
$ make db-up
$ make db-check

# This will run the app with 'cargo run' and debug log level
$ make run-debug
```

### Testing

**Unit tests**

```sh
$ make unit-tests
```

**Integration tests** _(to be run locally)_

Install [gcloud](https://cloud.google.com/sdk/docs/install) - google access token will be retrieved through gcloud cli tool, unlike production case scenario where the application relies on service account configuration.

In order to run integration tests, it is required to have locally spawned mongodb cluster
and a configured GCP pubsub topic, schema and subscription.

It is planned to automate creating GCP resources in the future. For now check `tests/setup/mod.rs`

```sh
$ make integration-tests
```

### Configuring Docker Mongo Cluster

https://www.mongodb.com/compatibility/deploying-a-mongodb-cluster-with-docker

## License

License under either or:

- [MIT](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)
