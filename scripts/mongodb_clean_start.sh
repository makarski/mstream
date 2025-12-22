#!/bin/bash
# mongodb_clean_start.sh
# Clean MongoDB startup for mstream project
# Removes old volumes and starts MongoDB with fresh data

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}════════════════════════════════════════════${NC}"
echo -e "${BLUE}  MongoDB Clean Start${NC}"
echo -e "${BLUE}════════════════════════════════════════════${NC}\n"

# Check if mongo1 is running
if docker ps --format '{{.Names}}' | grep -q "^mongo1$"; then
    echo -e "${YELLOW}Stopping running MongoDB container...${NC}"
    docker stop mongo1
fi

# Check if container exists
if docker ps -a --format '{{.Names}}' | grep -q "^mongo1$"; then
    echo -e "${YELLOW}Removing MongoDB container...${NC}"
    docker rm mongo1
fi

# Remove old volumes
echo -e "${YELLOW}Removing old MongoDB volumes...${NC}"
docker volume rm mstream_mongovol1 mstream_mongovol2 mstream_mongovol3 2>/dev/null || true

# Fix keyfile permissions
KEYFILE_PATH="local/scripts/mongo-keyfile"
if [ -f "$KEYFILE_PATH" ]; then
    echo -e "${YELLOW}Checking MongoDB keyfile permissions...${NC}"
    CURRENT_PERMS=$(stat -f "%Lp" "$KEYFILE_PATH" 2>/dev/null || stat -c "%a" "$KEYFILE_PATH" 2>/dev/null)

    if [ "$CURRENT_PERMS" != "400" ] && [ "$CURRENT_PERMS" != "600" ]; then
        echo -e "${YELLOW}Fixing keyfile permissions (current: $CURRENT_PERMS, required: 400)...${NC}"
        chmod 400 "$KEYFILE_PATH"
        echo -e "${GREEN}✓ Keyfile permissions fixed${NC}"
    else
        echo -e "${GREEN}✓ Keyfile permissions are correct ($CURRENT_PERMS)${NC}"
    fi
else
    echo -e "${RED}✗ MongoDB keyfile not found at $KEYFILE_PATH${NC}"
    exit 1
fi

# Start MongoDB
echo -e "\n${YELLOW}Starting MongoDB with fresh volume...${NC}"
docker-compose up -d mongo1

# Wait for MongoDB to be ready
echo -e "${YELLOW}Waiting for MongoDB to start...${NC}"
for i in {1..30}; do
    if docker exec mongo1 mongosh --quiet --eval "db.adminCommand('ping')" 2>/dev/null | grep -q "ok: 1"; then
        echo -e "${GREEN}✓ MongoDB is ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}✗ MongoDB failed to start${NC}"
        docker logs mongo1 --tail 20
        exit 1
    fi
    echo -n "."
    sleep 2
done
echo ""

# Initialize replica set
echo -e "\n${YELLOW}Initializing replica set...${NC}"
if docker exec mongo1 /bin/bash /opt/scripts/init_replica_set.sh; then
    echo -e "${GREEN}✓ Replica set initialized successfully${NC}"
else
    echo -e "${RED}✗ Failed to initialize replica set${NC}"
    docker logs mongo1 --tail 20
    exit 1
fi

# Verify setup
echo -e "\n${YELLOW}Verifying MongoDB setup...${NC}"
MONGO_STATUS=$(docker exec mongo1 mongosh \
    --username admin \
    --password adminpassword \
    --authenticationDatabase admin \
    --quiet \
    --eval "rs.status().members[0].stateStr" 2>/dev/null || echo "FAILED")

if [ "$MONGO_STATUS" = "PRIMARY" ]; then
    echo -e "${GREEN}✓ MongoDB replica set is PRIMARY${NC}"
else
    echo -e "${RED}✗ MongoDB replica set status: $MONGO_STATUS${NC}"
    exit 1
fi

echo -e "\n${GREEN}════════════════════════════════════════════${NC}"
echo -e "${GREEN}  MongoDB Successfully Started!${NC}"
echo -e "${GREEN}════════════════════════════════════════════${NC}\n"

echo "Connection string:"
echo "mongodb://admin:adminpassword@localhost:27017/?replicaSet=mstream_db_replica_set&authSource=admin"
echo ""
echo "To check status:"
echo "  docker exec mongo1 mongosh --username admin --password adminpassword --authenticationDatabase admin --eval 'rs.status()'"
echo ""
