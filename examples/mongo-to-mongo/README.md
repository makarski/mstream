# MongoDB to MongoDB Streaming Example

This example demonstrates how to use mstream to stream data from one MongoDB collection to another in real-time using MongoDB Change Streams.

## Use Case

Stream order documents from the `orders` collection to a `processed_orders` collection, which could be used for:
- Creating a processed/cleaned data layer
- Maintaining a backup collection
- Feeding a read-optimized collection
- Building an event sourcing pattern

## Architecture

```
MongoDB (ecommerce.orders) 
         ↓ [Change Stream]
    mstream (batch: 100)
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

### 2. Run mstream

```bash
# From the project root
make run-debug ARGS="--config examples/mongo-to-mongo/mstream-config.toml"
```

### 3. Insert Test Data

Open a new terminal and insert 1000 test orders:

```bash
docker exec mongo-to-mongo-example mongosh "mongodb://admin:adminpassword@localhost:27017/ecommerce?authSource=admin" --eval "
const orders = [];
for (let i = 1; i <= 1000; i++) {
  orders.push({
    order_id: 'ORD-' + String(i).padStart(6, '0'),
    customer_name: 'Customer ' + i,
    customer_email: 'customer' + i + '@example.com',
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

### 4. Verify Data Transfer

Check that documents were copied to the destination collection:

```bash
# Check counts in both collections
docker exec mongo-to-mongo-example mongosh "mongodb://admin:adminpassword@localhost:27017/ecommerce?authSource=admin" --eval "
print('Source collection (orders):', db.orders.countDocuments());
print('Destination collection (processed_orders):', db.processed_orders.countDocuments());
print('---');
print('Sample from processed_orders:');
db.processed_orders.find().limit(3).forEach(printjson);
"
```

You should see:
- 1000 documents in `orders`
- 1000 documents in `processed_orders`
- Documents streaming in real-time as mstream processes batches

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

### Real-time Streaming

To disable batching and process events immediately, remove the `batch` line:

```toml
[[connectors]]
enabled = true
name = "orders-to-processed-orders"
source = { service_name = "mongodb-local", resource = "orders", output_encoding = "bson" }
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
