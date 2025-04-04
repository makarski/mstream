#!/bin/bash
echo "Waiting for MongoDB servers to be ready..."
for i in {1..30}; do
    if mongosh --host mongo1:27017 --eval 'db.adminCommand("ping")' &>/dev/null &&
        mongosh --host mongo2:27017 --eval 'db.adminCommand("ping")' &>/dev/null &&
        mongosh --host mongo3:27017 --eval 'db.adminCommand("ping")' &>/dev/null; then
    echo "All MongoDB servers are ready"
    break
    fi
    echo "Waiting for MongoDB servers... (attempt $i/30)"
    sleep 2
done

echo "Configuring replica set..."
mongosh --host mongo1 --eval "
    rs.initiate({
    _id: 'mstream_db_replica_set',
    members: [
        {_id: 0, host: 'mongo1:27017', priority: 2},  // Higher priority to prefer as primary
        {_id: 1, host: 'mongo2:27017'},
        {_id: 2, host: 'mongo3:27017'}
    ]
    })
"

# Wait for replica set to stabilize
echo "Waiting for replica set to initialize..."
sleep 10

# Check replica set status
mongosh --host mongo1 --eval "rs.status()"
