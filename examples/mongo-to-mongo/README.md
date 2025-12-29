# MongoDB to MongoDB Streaming Example with Data Anonymization

This example demonstrates how to use mstream to stream data from one MongoDB collection to another in real-time using MongoDB Change Streams, while applying data transformations using Rhai scripts.

## Use Case

Stream order documents from the `orders` collection to a `processed_orders` collection while **anonymizing customer data**:
- Mask customer names and emails
- Add processing metadata
- Maintain order structure for analytics
- Perfect for GDPR compliance or creating test datasets

## Architecture

```
MongoDB (ecommerce.orders) 
         ↓ [Change Stream]
    mstream (batch: 100)
         ↓ [Rhai Anonymizer]
         ↓
MongoDB (ecommerce.processed_orders)
```

## Prerequisites

- Docker and Docker Compose installed
- Rust and Cargo installed
- Port 27017 available

## Quick Start

### 1. Start MongoDB

```bash
# From the examples/mongo-to-mongo directory
docker-compose up -d

# Wait for MongoDB to start
sleep 5

# Initialize the replica set (required for Change Streams)
docker exec mongo-to-mongo-example mongosh --username admin --password adminpassword --authenticationDatabase admin --eval "
rs.initiate({
  _id: 'mstream_replica_set',
  members: [
    { _id: 0, host: 'mongo1:27017' }
  ]
})
"

# Wait for replica set to be ready
sleep 5

# Verify replica set is ready
docker exec mongo-to-mongo-example mongosh --username admin --password adminpassword --authenticationDatabase admin --eval "rs.status().ok"
```

### 2. Run mstream with Anonymization

```bash
# From the project root
make run-debug ARGS="--config examples/mongo-to-mongo/mstream-config.toml"
```

You should see logs indicating the anonymizer middleware is loaded:
```
INFO mstream::provision::pipeline: spawning a listener for connector: orders-to-processed-orders
INFO loaded middleware: anonymize_orders.rhai
```

### 3. Insert Test Data

Open a new terminal and insert 1000 test orders with customer data:

```bash
docker exec mongo-to-mongo-example mongosh "mongodb://admin:adminpassword@localhost:27017/ecommerce?authSource=admin" --eval "
const orders = [];
for (let i = 1; i <= 1000; i++) {
  orders.push({
    order_id: 'ORD-' + String(i).padStart(6, '0'),
    customer_name: 'John Doe ' + i,  // Will be anonymized
    customer_email: 'customer' + i + '@example.com',  // Will be anonymized
    items: [
      { 
        product: 'Product ' + i, 
        quantity: Math.floor(Math.random() * 5) + 1, 
        price: Math.round(Math.random() * 100 * 100) / 100 
      }
    ],
    total: Math.round(Math.random() * 500 * 100) / 100,
    status: 'pending',
    created_at: new Date()
  });
}

// Insert in smaller batches to avoid command size limits
const batchSize = 100;
for (let i = 0; i < orders.length; i += batchSize) {
  const batch = orders.slice(i, i + batchSize);
  db.orders.insertMany(batch);
  print('Inserted batch', Math.floor(i / batchSize) + 1, 'of', Math.ceil(orders.length / batchSize));
}

print('Total inserted:', db.orders.countDocuments());
"
```

### 4. Verify Data Anonymization

Check that customer data was anonymized in the destination collection:

```bash
# Compare source vs destination
docker exec mongo-to-mongo-example mongosh "mongodb://admin:adminpassword@localhost:27017/ecommerce?authSource=admin" --eval "
print('=== SOURCE (orders) - Original Data ===');
db.orders.findOne({}, {customer_name: 1, customer_email: 1, order_id: 1, _id: 0});

print('\n=== DESTINATION (processed_orders) - Anonymized Data ===');
db.processed_orders.findOne({}, {customer_name: 1, customer_email: 1, order_id: 1, anonymized: 1, anonymized_at: 1, processed_by: 1, _id: 0});

print('\n=== Counts ===');
print('Source:', db.orders.countDocuments());
print('Destination:', db.processed_orders.countDocuments());
"
```

Expected output:
```javascript
// SOURCE: Original customer data
{
  order_id: 'ORD-000001',
  customer_name: 'John Doe 1',
  customer_email: 'customer1@example.com'
}

// DESTINATION: Anonymized data
{
  order_id: 'ORD-000001',
  customer_name: 'ANONYMIZED',
  customer_email: 'anonymized@example.com',
  anonymized: true,
  anonymized_at: ISODate('2025-12-29T...'),
  processed_by: 'rhai-anonymizer'
}
```

## Configuration Options

### Batching

The example uses batching with a size of 100 for better throughput:

```toml
batch = { kind = "count", size = 100 }
```

Adjust the batch size based on your needs:
- **Small (10-50)**: Lower latency, better for real-time needs
- **Medium (100-500)**: Balanced throughput and latency
- **Large (1000+)**: Maximum throughput for bulk processing

### Customizing the Anonymization Script

Edit `scripts/anonymize_orders.rhai` to customize anonymization logic:

```javascript
fn transform(data, attributes) {
    let order = bson_decode(data);
    
    // Customize anonymization
    order.customer_name = "***REDACTED***";
    order.customer_email = hash(order.customer_email);  // Hash instead of replace
    
    // Add custom fields
    order.data_classification = "anonymized";
    
    return result(bson_encode(order), attributes);
}
```

### Real-time Streaming Without Batching

To disable batching and process events immediately, remove the `batch` line:

```toml
[[connectors]]
enabled = true
name = "orders-to-processed-orders"
source = { service_name = "mongodb-local", resource = "orders", output_encoding = "bson" }
middlewares = [
    { service_name = "anonymizer", resource = "anonymize_orders.rhai", input_encoding = "bson", output_encoding = "bson" }
]
sinks = [
    { service_name = "mongodb-local", resource = "processed_orders" }
]
```

## Cleanup

```bash
# Stop mstream (Ctrl+C in the terminal running mstream)

# Stop and remove MongoDB container (from examples/mongo-to-mongo directory)
docker-compose down -v
```

## How It Works

1. **MongoDB Change Streams**: mstream watches the `orders` collection for any changes (inserts, updates, deletes)
2. **Batching**: Events are collected into batches of 100 for efficient processing
3. **Rhai Middleware**: Each order passes through the anonymization script
4. **Data Transformation**: Customer names/emails are masked, metadata is added
5. **BSON Encoding**: Uses MongoDB's native BSON format for best performance
6. **Automatic Replication**: Anonymized orders are written to `processed_orders`

## Why Replica Set?

MongoDB Change Streams (which mstream uses to detect changes) **require a replica set**. Even though we only have one MongoDB node, we configure it as a single-member replica set to enable Change Streams.

## Next Steps

- Modify the anonymization logic in `scripts/anonymize_orders.rhai`
- Add more middleware steps (e.g., validation, enrichment)
- Stream to [Kafka](../mongo_to_kafka.toml) instead of MongoDB
- Add [schema validation](../../mstream-config.toml.example) to filter fields
- Chain multiple transformation scripts together
