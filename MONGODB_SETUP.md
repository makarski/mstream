# MongoDB Setup Guide

## Overview

The mstream project uses MongoDB with replica sets for change stream support. The current configuration uses a **single-node replica set** for testing purposes.

## Quick Start

```bash
# Clean start (recommended for first time or after errors)
./scripts/mongodb_clean_start.sh

# Or manually:
docker-compose up -d mongo1
sleep 10
docker exec mongo1 /bin/bash /opt/scripts/init_replica_set.sh
```

## Common Issues

### Issue 1: Authentication Failed

**Error:**
```
MongoServerError: Authentication failed.
```

**Cause:** Old volume data with mismatched authentication configuration.

**Solution:**
```bash
# Stop and remove container and volumes
docker stop mongo1 && docker rm mongo1
docker volume rm mstream_mongovol1

# Start fresh
docker-compose up -d mongo1
sleep 10
docker exec mongo1 /bin/bash /opt/scripts/init_replica_set.sh
```

### Issue 2: Unable to Acquire Security Key

**Error:**
```
Unable to acquire security key[s]
```

**Cause:** MongoDB keyfile has incorrect permissions.

**Solution:**
```bash
chmod 400 local/scripts/mongo-keyfile
docker-compose restart mongo1
```

### Issue 3: Replica Set Not Initialized

**Error:**
```
ReadConcernMajorityNotAvailableYet
No primary exists currently
```

**Cause:** Replica set hasn't been initialized yet.

**Solution:**
```bash
docker exec mongo1 /bin/bash /opt/scripts/init_replica_set.sh
```

## Architecture

### Current: Single-Node Replica Set

```
┌─────────────────┐
│     mongo1      │
│   (PRIMARY)     │
│  Port: 27017    │
└─────────────────┘
```

**Configuration:**
- Replica Set Name: `mstream_db_replica_set`
- Authentication: Enabled (keyfile)
- Admin User: `admin` / `adminpassword`
- Keyfile: `/opt/scripts/mongo-keyfile` (permissions: 400)

### Historical: Three-Node Replica Set

The original setup used 3 nodes:

```
┌─────────┐  ┌─────────┐  ┌─────────┐
│ mongo1  │  │ mongo2  │  │ mongo3  │
│ :27017  │  │ :27018  │  │ :27019  │
└─────────┘  └─────────┘  └─────────┘
```

This was simplified for easier testing.

## Manual Setup Steps

### 1. Start MongoDB

```bash
docker-compose up -d mongo1
```

This creates:
- Container: `mongo1`
- Volume: `mstream_mongovol1`
- Network: `mongoCluster`

### 2. Wait for MongoDB to Start

```bash
# Give MongoDB time to initialize
sleep 10

# Check if running
docker logs mongo1 | tail -20
```

### 3. Initialize Replica Set

```bash
docker exec mongo1 /bin/bash /opt/scripts/init_replica_set.sh
```

The script:
1. Waits for MongoDB to be ready (up to 60 seconds)
2. Authenticates with admin credentials
3. Runs `rs.initiate()` to create the replica set
4. Displays replica set status

### 4. Verify Setup

```bash
# Check replica set status
docker exec mongo1 mongosh --username admin --password adminpassword --authenticationDatabase admin --eval "rs.status()"

# Should show:
# - stateStr: "PRIMARY"
# - ok: 1
```

## Environment Variables

| Variable | Value | Purpose |
|----------|-------|---------|
| `MONGO_INITDB_ROOT_USERNAME` | `admin` | Creates admin user on first start |
| `MONGO_INITDB_ROOT_PASSWORD` | `adminpassword` | Admin user password |

**Important:** These only work with fresh volumes. With existing data, the user must already exist.

## Connection Strings

### Local Development

```
mongodb://admin:adminpassword@localhost:27017/?replicaSet=mstream_db_replica_set&authSource=admin
```

### From Docker Network

```
mongodb://admin:adminpassword@mongo1:27017/?replicaSet=mstream_db_replica_set&authSource=admin
```

## Testing

```bash
# Test connection
docker exec mongo1 mongosh \
  --username admin \
  --password adminpassword \
  --authenticationDatabase admin \
  --eval "db.adminCommand('ping')"

# Test change streams (requires replica set)
docker exec mongo1 mongosh \
  --username admin \
  --password adminpassword \
  --authenticationDatabase admin \
  --eval "use test; db.testcoll.watch()"
```

## Makefile Commands

```bash
# Start MongoDB
make docker-db-up

# Initialize replica set
make db-init-rpl-set

# Check status
make db-check

# Stop MongoDB
make db-stop

# Clean restart
make db-stop
docker volume rm mstream_mongovol1
make docker-db-up
make db-init-rpl-set
```

## Troubleshooting

### Check MongoDB Logs

```bash
docker logs mongo1 --tail 50 --follow
```

### Check Container Status

```bash
docker ps -a | grep mongo1
```

### Check Volume

```bash
docker volume inspect mstream_mongovol1
```

### Connect to Container

```bash
docker exec -it mongo1 bash

# Inside container:
mongosh --username admin --password adminpassword --authenticationDatabase admin
```

### Reset Everything

```bash
#!/bin/bash
# Complete clean reset

# Stop and remove
docker stop mongo1
docker rm mongo1

# Remove volumes
docker volume rm mstream_mongovol1 mstream_mongovol2 mstream_mongovol3 2>/dev/null || true

# Remove network (optional)
docker network rm mongoCluster 2>/dev/null || true

# Start fresh
docker-compose up -d mongo1
sleep 10
docker exec mongo1 /bin/bash /opt/scripts/init_replica_set.sh
```

## Performance Considerations

### Single-Node Limitations

- No high availability
- No automatic failover
- Suitable for development/testing only

### For Production

Consider:
- 3-node replica set minimum
- Separate physical/virtual machines
- Proper backup strategy
- Monitoring and alerting

## Security Notes

### Keyfile

The keyfile (`local/scripts/mongo-keyfile`) is used for inter-replica-set authentication.

**Requirements:**
- Permissions: 400 or 600 (owner read-only)
- Minimum 6 characters
- Maximum 1024 characters
- Base64 characters only

**Never commit keyfile to public repositories!**

### Admin Credentials

Current credentials (`admin`/`adminpassword`) are for development only.

**For production:**
- Use strong passwords
- Use environment variables
- Consider MongoDB Atlas or managed services
- Implement proper RBAC

## References

- [MongoDB Replica Sets](https://www.mongodb.com/docs/manual/replication/)
- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [MongoDB Security Checklist](https://www.mongodb.com/docs/manual/administration/security-checklist/)
