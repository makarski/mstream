mstream
===

The application subscribes to [mongodb change streams](https://www.mongodb.com/docs/manual/changeStreams/) and Kafka topics specified in config.
MongoDB create and update events, as well as messages from Kafka, are picked up and can be encoded in various formats (Avro, JSON, BSON) before being sent to GCP PubSub, Kafka, or MongoDB.
One connector can be configured to consume from **one source** and publish to **multiple sink topics** from various providers.

`Minimum tested MongoDB version: 6.0`

```mermaid
graph TB
    subgraph mstream[mstream]
        handler@{ shape: procs, label: "Listeners"}
        %% kafka_consumer[Kafka Consumer]
        schema_cache[Schema Cache]
        encoder[["`Format Encoder
        (Json, Avro, Bson)`"]]
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
    end


    subgraph schemas[Schemas]
        schema_registry@{label: "PubSub Schema Registry (Avro)" }
    end


    mongodb_source e1@-.-> handler
    kafka_source e2@-..-> handler
    pubsub_source_topic e3@-.-> handler
    schema_registry --> schema_cache

    handler --> encoder
    schema_cache <-->handler

    encoder e4@-.-> mongodb_target
    encoder e5@-.-> pubsub_sink_topic
    encoder e6@-.-> kafka_sink

    e1@{animate: true}
    e2@{animate: true}
    e3@{animate: true}
    e4@{animate: true}
    e5@{animate: true}
    e6@{animate: true}
```

### Supported Format Conversions

mstream supports multiple encoding formats for both sources and sinks:

#### BSON Source:
- BSON → Avro: Converts MongoDB BSON documents to Avro records
- BSON → JSON: Serializes BSON documents to JSON format
- BSON → BSON: Direct passthrough (for MongoDB to MongoDB replication)

#### Avro Source:
- Avro → Avro: Passthrough or schema validation
- Avro → JSON: Deserializes Avro records to JSON format
- Avro → BSON: Converts Avro records to MongoDB BSON documents

#### JSON Source:
- JSON → JSON: Passthrough or format validation
- JSON → Avro: Parses JSON and encodes as Avro records
- JSON → BSON: Parses JSON and converts to BSON documents

JSON source operations are processed by first converting to BSON internally and then
applying the same transformation logic as BSON sources, unless the target is JSON.

### Event Processing

**Supported Sources**
* [MongoDB Change Stream Events](https://www.mongodb.com/docs/v6.0/reference/change-events/)
  * Insert document
  * Update document
  * Delete document
* Kafka Messages

**The worker will report an error and stop execution for MongoDB events**
* Invalidate stream
* Drop collection
* Drop database

#### Message Structure

A processed change stream is transformed into a pubsub message with the following structure:

**[Attributes](https://cloud.google.com/pubsub/docs/publisher#using-attributes)**

attribute name | attribute value
---------------| ----------------
operation_type | event type: `insert`, `update`, `delete`
database       | mongodb database name
collection     | mongodb collection name

Attributes can be used to configure fine-grained subscriptions. For more details see [documentation](https://cloud.google.com/pubsub/docs/subscription-message-filter#filtering_syntax)

**Payload**

Payload represents a mongo db document encoded in avro format

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

* [MIT](LICENSE-MIT)
* [Apache License, Version 2.0](LICENSE-APACHE)
