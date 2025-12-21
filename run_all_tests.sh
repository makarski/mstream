#!/bin/bash
# run_all_tests.sh
# Comprehensive test suite runner for mstream
# Runs all test types: unit, trait mocks, mock gRPC, emulator, and integration tests

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test results tracking
UNIT_TESTS_PASSED=false
MOCK_GRPC_TESTS_PASSED=false
EMULATOR_TESTS_PASSED=false
INTEGRATION_TESTS_PASSED=false

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}=== Cleanup ===${NC}"

    # Stop PubSub emulator
    if docker ps -a | grep -q pubsub-emulator; then
        echo "Stopping PubSub emulator..."
        docker stop pubsub-emulator 2>/dev/null || true
        docker rm pubsub-emulator 2>/dev/null || true
    fi

    # Stop MongoDB (optional - user might want to keep it running)
    # Set STOP_MONGODB=true to stop MongoDB after tests
    if [ "${STOP_MONGODB:-false}" = "true" ]; then
        echo "Stopping MongoDB..."
        if docker ps --format '{{.Names}}' | grep -q "^mongo1$"; then
            docker stop mongo1 2>/dev/null || true
            echo "MongoDB container stopped"
        fi
    else
        echo "MongoDB container left running (set STOP_MONGODB=true to stop it)"
    fi

    echo -e "${GREEN}Cleanup complete${NC}\n"
}

# Set up trap for cleanup on exit
trap cleanup EXIT

# Print header
print_header() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}\n"
}

# Print step
print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

# Print success
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Print error
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Start timer
START_TIME=$(date +%s)

print_header "MSTREAM COMPREHENSIVE TEST SUITE"

echo "This script will run all test types:"
echo "  1. Unit tests (~5s)"
echo "  2. Mock gRPC server tests (~0.5s)"
echo "  3. PubSub emulator tests (~2s)"
echo "  4. Integration tests (~40s)"
echo ""
echo "Total estimated time: ~50 seconds"
echo ""

# Check prerequisites
print_header "Checking Prerequisites"

print_step "Checking for required commands..."
MISSING_DEPS=false

if ! command_exists docker; then
    print_error "Docker not found. Required for emulator tests."
    MISSING_DEPS=true
fi

if ! command_exists cargo; then
    print_error "Cargo not found. Rust toolchain required."
    MISSING_DEPS=true
fi

if ! command_exists make; then
    print_error "Make not found. Required for MongoDB setup."
    MISSING_DEPS=true
fi

if [ "$MISSING_DEPS" = true ]; then
    echo ""
    print_error "Missing required dependencies. Please install them first."
    exit 1
fi

print_success "All prerequisites found"

# ============================================================================
# 1. SETUP SERVICES
# ============================================================================

print_header "1. Setting Up Services"

# Start MongoDB
print_step "Checking MongoDB status..."

# Check if MongoDB container is already running
if docker ps --format '{{.Names}}' | grep -q "^mongo1$"; then
    print_success "MongoDB container is already running"
else
    print_step "MongoDB not running, checking if container exists but is stopped..."

    # Check if container exists but is stopped
    if docker ps -a --format '{{.Names}}' | grep -q "^mongo1$"; then
        print_step "Starting existing MongoDB container..."
        if docker start mongo1 > /dev/null 2>&1; then
            print_success "MongoDB container started"
        else
            print_error "Failed to start existing MongoDB container"
            exit 1
        fi
    else
        print_step "Starting MongoDB from docker-compose.yml..."
        if docker-compose up -d mongo1 2>&1; then
            print_success "MongoDB started from docker-compose"
        else
            print_error "Failed to start MongoDB from docker-compose"
            exit 1
        fi
    fi
fi

# Wait for MongoDB to be ready
print_step "Waiting for MongoDB to be ready..."
sleep 3

print_step "Initializing MongoDB replica set..."
sleep 2  # Give MongoDB time to start
if make db-init-rpl-set 2>&1; then
    print_success "MongoDB replica set initialized"
else
    print_error "Failed to initialize replica set (may already be initialized)"
fi

print_step "Checking MongoDB status..."
if make db-check > /dev/null 2>&1; then
    print_success "MongoDB is ready"
else
    print_error "MongoDB health check failed"
    exit 1
fi

# Start PubSub Emulator
print_step "Starting PubSub emulator..."
docker run -d -p 8085:8085 --name pubsub-emulator \
    gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 > /dev/null 2>&1

if [ $? -eq 0 ]; then
    print_success "PubSub emulator started"
else
    print_error "Failed to start PubSub emulator"
    exit 1
fi

print_step "Waiting for emulator to be ready..."
sleep 5

# Verify emulator is running
if curl -s http://localhost:8085 > /dev/null 2>&1; then
    print_success "PubSub emulator is ready"
else
    print_error "PubSub emulator health check failed"
    exit 1
fi

# Create topics and subscriptions
print_step "Creating PubSub topics and subscriptions..."
docker exec pubsub-emulator gcloud pubsub topics create test-topic --project=test-project > /dev/null 2>&1 || true
docker exec pubsub-emulator gcloud pubsub subscriptions create test-subscription \
    --topic=test-topic --project=test-project > /dev/null 2>&1 || true
print_success "PubSub resources created"

# ============================================================================
# 2. RUN UNIT TESTS
# ============================================================================

print_header "2. Running Unit Tests"

print_step "Executing unit tests..."
if make unit-tests 2>&1; then
    UNIT_TESTS_PASSED=true
    print_success "Unit tests PASSED"
else
    print_error "Unit tests FAILED"
fi

# ============================================================================
# 3. RUN MOCK GRPC SERVER TESTS
# ============================================================================

print_header "3. Running Mock gRPC Server Tests"

print_step "Executing mock gRPC server tests..."
if cargo test --test mock_grpc_server_test 2>&1 | tee /tmp/mock_grpc_output.log | grep -q "test result: ok"; then
    MOCK_GRPC_TESTS_PASSED=true
    print_success "Mock gRPC server tests PASSED"
else
    print_error "Mock gRPC server tests FAILED"
    cat /tmp/mock_grpc_output.log
fi

# ============================================================================
# 4. RUN PUBSUB EMULATOR TESTS
# ============================================================================

print_header "4. Running PubSub Emulator Tests"

print_step "Executing emulator tests..."
export USE_PUBSUB_EMULATOR=true
export PUBSUB_SUBSCRIPTION="projects/test-project/subscriptions/test-subscription"

if cargo test --test pubsub_emulator_test -- --ignored 2>&1 | tee /tmp/emulator_output.log | grep -q "test result: ok"; then
    EMULATOR_TESTS_PASSED=true
    print_success "PubSub emulator tests PASSED"
else
    print_error "PubSub emulator tests FAILED"
    cat /tmp/emulator_output.log
fi

# ============================================================================
# 5. RUN INTEGRATION TESTS
# ============================================================================

print_header "5. Running Integration Tests"

print_step "Executing integration tests with emulator..."
export USE_PUBSUB_EMULATOR=true
export MONGO_URI="mongodb://admin:adminpassword@localhost:27017/?replicaSet=rs0&authSource=admin"
export PUBSUB_SUBSCRIPTION="projects/test-project/subscriptions/test-subscription"

if cargo test --test integration_test -- --ignored 2>&1 | tee /tmp/integration_output.log | grep -q "test result: ok"; then
    INTEGRATION_TESTS_PASSED=true
    print_success "Integration tests PASSED"
else
    print_error "Integration tests FAILED"
    echo "Note: Integration test requires MongoDB change streams and may take ~40 seconds"
    cat /tmp/integration_output.log
fi

# ============================================================================
# SUMMARY
# ============================================================================

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

print_header "TEST SUMMARY"

echo "Results:"
echo "--------"

if [ "$UNIT_TESTS_PASSED" = true ]; then
    print_success "Unit Tests: PASSED"
else
    print_error "Unit Tests: FAILED"
fi

if [ "$MOCK_GRPC_TESTS_PASSED" = true ]; then
    print_success "Mock gRPC Server Tests: PASSED"
else
    print_error "Mock gRPC Server Tests: FAILED"
fi

if [ "$EMULATOR_TESTS_PASSED" = true ]; then
    print_success "PubSub Emulator Tests: PASSED"
else
    print_error "PubSub Emulator Tests: FAILED"
fi

if [ "$INTEGRATION_TESTS_PASSED" = true ]; then
    print_success "Integration Tests: PASSED"
else
    print_error "Integration Tests: FAILED"
fi

echo ""
echo "Duration: ${DURATION}s"
echo ""

# Determine overall status
if [ "$UNIT_TESTS_PASSED" = true ] && \
   [ "$MOCK_GRPC_TESTS_PASSED" = true ] && \
   [ "$EMULATOR_TESTS_PASSED" = true ] && \
   [ "$INTEGRATION_TESTS_PASSED" = true ]; then
    print_header "🎉 ALL TESTS PASSED! 🎉"
    exit 0
else
    print_header "❌ SOME TESTS FAILED"
    echo "Check the output above for details"
    exit 1
fi
