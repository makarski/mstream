#!/bin/bash
AUTH_ARGS=(--username admin --password adminpassword --authenticationDatabase admin)

echo "Waiting for MongoDB servers to be ready..."
for i in {1..30}; do
    if mongosh --host mongo1:27017 "${AUTH_ARGS[@]}" --eval 'db.adminCommand("ping")' >/dev/null 2>&1; then
        echo "All MongoDB servers are ready"
        break
    fi
    echo "Waiting for MongoDB servers... (attempt $i/30)"
    sleep 2
done

echo "Configuring replica set..."
mongosh --host mongo1 "${AUTH_ARGS[@]}" --eval '
    rs.initiate({
        _id: "mstream_db_replica_set",
        members: [
            { _id: 0, host: "mongo1:27017" }
        ]
    })
'

echo "Waiting for replica set to initialize..."
sleep 10

mongosh --host mongo1 "${AUTH_ARGS[@]}" --eval "rs.status()"
