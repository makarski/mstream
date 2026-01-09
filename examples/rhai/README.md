# Rhai Middleware Examples

This directory contains example Rhai scripts for transforming events using the UDF middleware.

## TOML Configuration Example

Here's a complete TOML configuration example showing how to use these Rhai scripts in a connector:

```toml
[[services]]
provider = "udf"
name = "udf-rhai"
engine = { kind = "rhai" }
script_path = "examples/rhai/scripts/"

[[connectors]]
enabled = true
name = "user-data-transform"

# Define source - MongoDB collection
source = { service_name = "mongodb-local", resource = "users", output_encoding = "json" }

# Use Rhai scripts as middleware transformations
middlewares = [
    # First apply loyalty tier calculation
    { service_name = "udf-rhai", resource = "json_transform.rhai", output_encoding = "json" },

    # Then enrich with metadata attributes
    { service_name = "udf-rhai", resource = "attribute_enrichment.rhai", output_encoding = "json" },
]

# Define multiple sinks for the transformed data
sinks = [
    # Send to MongoDB
    { service_name = "mongodb-local", resource = "users_processed", output_encoding = "bson" },

    # Also send to Kafka
    { service_name = "kafka-local", resource = "users_topic", output_encoding = "json" }
]
```

## Available Scripts

* [json_transform.rhai](./scripts/json_transform.rhai)
Transforms JSON events by adding fields, calculating derived values, and cleaning data.

* [attribute_enrichment.rhai](./scripts/attribute_enrichment.rhai)
Enriches event attributes with processing metadata and derived information.

* [data_filter.rhai](./scripts/data_filter.rhai)
Filters and reshapes data based on business rules.

## Sample Data

The `sample_data/` directory contains example JSON events that can be processed by these scripts.

## Writing Rhai Scripts

The `transform` function receives the event data automatically decoded based on the source encoding (e.g., as a Map for JSON/BSON). It should return the transformed data, which will be encoded back to the output format.

## Built-in Rhai Helper Functions

The embedded Rhai engine exposes several helper functions you can call from your scripts:

- `result(data, attributes?)`: wraps the transformed payload (and optional updated attributes) for the pipeline.
- `timestamp_ms()`: returns the current UNIX timestamp in milliseconds.
- `hash_sha256(value)`: computes a SHA-256 hex digest for anonymization.
- `mask_email(email)`: obfuscates the local-part of an email (e.g., `alice@example.com` → `a***@example.com`).
- `mask_phone(phone)`: masks all but the last four digits of a phone number.
- `mask_year_only(date_or_iso_date)`: truncates a date to the first day of its year, handling both plain strings and MongoDB EJSON timestamps.

Use these helpers to keep your Rhai scripts concise and consistent across connectors.

## Append-Only MongoDB Sink

By default, MongoDB Change Streams include an `_id` field in each document. When sinking back to MongoDB with `write_mode = "insert"` (the default), duplicate `_id` values will cause insert failures.

To enable **append-only** behavior (every event creates a new document), use a Rhai middleware to strip the `_id` field before the sink:

```rhai
// strip_id.rhai - Remove _id to allow append-only inserts
fn transform(doc, attributes) {
    doc.remove("_id");
    result(doc, attributes)
}
```

Configuration example:

```toml
[[services]]
provider = "mongodb"
name = "mongo-sink"
connection_string = "mongodb://localhost:27017"
db_name = "app_db"
write_mode = "insert"  # default; will fail on duplicate _id

[[services]]
provider = "udf"
name = "udf-rhai"
engine = { kind = "rhai" }
script_path = "examples/rhai/scripts/"

[[connectors]]
enabled = true
name = "append-only-sync"
source = { service_name = "mongo-source", resource = "events", output_encoding = "json" }
middlewares = [
    { service_name = "udf-rhai", resource = "strip_id.rhai", output_encoding = "json" }
]
sinks = [
    { service_name = "mongo-sink", resource = "events_archive", output_encoding = "bson" }
]
```

This pattern is useful for creating audit logs or event archives where each change stream event should be preserved as a separate document.

> **Tip:** If you want upsert behavior instead (update existing documents by `_id`), use `write_mode = "replace"` on the MongoDB service and keep the `_id` field intact.
