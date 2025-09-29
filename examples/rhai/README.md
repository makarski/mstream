# Rhai Middleware Examples

This directory contains example Rhai scripts for transforming events using the UDF middleware.

## Running the Examples

To use these scripts with the middleware:

```rust
use mstream::middleware::udf::rhai::RhaiMiddleware;

let middleware = RhaiMiddleware::new(
    "examples/rhai/scripts".to_string(),
    "json_transform.rhai".to_string()
)?;
```

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

### json_transform.rhai
Transforms JSON events by adding fields, calculating derived values, and cleaning data.

### attribute_enrichment.rhai
Enriches event attributes with processing metadata and derived information.

### data_filter.rhai
Filters and reshapes data based on business rules.

## Sample Data

The `sample_data/` directory contains example JSON events that can be processed by these scripts.
