#!/bin/bash
# Test mixed expressions with both standard and denormalized schemas
# This helps identify which patterns work and which need fixing

set -e

echo "================================================================================"
echo "Mixed Expression Testing - Standard + Denormalized Schemas"
echo "================================================================================"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Kill any existing server
echo -e "${YELLOW}Stopping any existing servers...${NC}"
pkill -9 -f "target.*clickgraph" 2>/dev/null || true
sleep 2

# Function to start server with a schema
start_server() {
    local schema=$1
    local schema_name=$2
    
    echo -e "\n${YELLOW}Starting server with ${schema_name}...${NC}"
    
    export CLICKHOUSE_URL="http://localhost:8123"
    export CLICKHOUSE_USER="test_user"
    export CLICKHOUSE_PASSWORD="test_pass"
    export CLICKHOUSE_DATABASE="brahmand"
    export GRAPH_CONFIG_PATH="$schema"
    export RUST_LOG="info"
    
    cd /home/gz/clickgraph
    cargo build --release --bin clickgraph > /dev/null 2>&1
    
    nohup ./target/release/clickgraph --http-port 8080 --disable-bolt > /tmp/clickgraph_test.log 2>&1 &
    SERVER_PID=$!
    
    # Wait for server to be ready
    echo "Waiting for server to start (PID: $SERVER_PID)..."
    for i in {1..10}; do
        if curl -s -X POST http://localhost:8080/health > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Server ready${NC}"
            return 0
        fi
        sleep 1
    done
    
    echo -e "${RED}✗ Server failed to start${NC}"
    cat /tmp/clickgraph_test.log
    return 1
}

stop_server() {
    echo -e "\n${YELLOW}Stopping server...${NC}"
    pkill -9 -f "target.*clickgraph" 2>/dev/null || true
    sleep 1
}

# Test 1: Standard Schema
echo ""
echo "================================================================================"
echo "TEST 1: Standard Schema (users/follows)"
echo "================================================================================"

start_server "benchmarks/social_network/schemas/social_benchmark.yaml" "Standard Schema"
python3 tests/integration/test_mixed_expressions.py
STANDARD_EXIT=$?
stop_server

# Test 2: Denormalized Schema  
echo ""
echo "================================================================================"
echo "TEST 2: Denormalized Schema (flights with embedded airports)"
echo "================================================================================"

start_server "benchmarks/schemas/ontime_denormalized.yaml" "Denormalized Schema"
python3 tests/integration/test_mixed_expressions.py
DENORM_EXIT=$?
stop_server

# Summary
echo ""
echo "================================================================================"
echo "FINAL SUMMARY"
echo "================================================================================"

if [ $STANDARD_EXIT -eq 0 ]; then
    echo -e "${GREEN}✓ Standard Schema: All tests passed${NC}"
else
    echo -e "${RED}✗ Standard Schema: Some tests failed${NC}"
fi

if [ $DENORM_EXIT -eq 0 ]; then
    echo -e "${GREEN}✓ Denormalized Schema: All tests passed${NC}"
else
    echo -e "${RED}✗ Denormalized Schema: Some tests failed${NC}"
fi

echo ""
if [ $STANDARD_EXIT -eq 0 ] && [ $DENORM_EXIT -eq 0 ]; then
    echo -e "${GREEN}🎉 ALL TESTS PASSED${NC}"
    exit 0
else
    echo -e "${RED}❌ SOME TESTS FAILED - Review output above${NC}"
    exit 1
fi
