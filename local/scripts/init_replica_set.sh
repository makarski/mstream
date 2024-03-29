#!/bin/bash

for i in {1..10}; do
  if mongosh --host mongo1 --eval 'db.adminCommand("ping")' > /dev/null; then
    break
  fi
  echo 'Waiting for mongo1...'
  sleep 1
done

mongosh --host mongo1 --eval "
  rs.initiate({
    _id: 'mstream_db_replica_set',
    members: [
      {_id: 0, host: 'mongo1'},
      {_id: 1, host: 'mongo2'},
      {_id: 2, host: 'mongo3'}
    ]
  })
"
