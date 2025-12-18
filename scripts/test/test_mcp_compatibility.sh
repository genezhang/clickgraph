#!/bin/bash
# MCP Compatibility Test - Isolated Environment
# Tests if Neo4j's MCP server can work with ClickGraph's Bolt protocol

set -e

# Isolated test ports (different from main instance)
HTTP_PORT=8081
BOLT_PORT=7688

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "MCP Compatibility Test (Isolated)"
echo "========================================"
echo "HTTP Port: $HTTP_PORT"
echo "Bolt Port: $BOLT_PORT"
echo ""

# Check if Neo4j MCP server is available
if ! command -v npx &> /dev/null; then
    echo -e "${RED}Error: npx not found. Please install Node.js${NC}"
    exit 1
fi

# Start ClickGraph on isolated ports
echo -e "${YELLOW}Starting ClickGraph on isolated ports...${NC}"
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
export RUST_LOG=info

# Kill any existing test instance on these ports
pkill -f "clickgraph.*--http-port $HTTP_PORT" || true
sleep 2

# Start new isolated instance
nohup ./target/release/clickgraph \
    --http-port $HTTP_PORT \
    --bolt-port $BOLT_PORT \
    > /tmp/clickgraph_mcp_test.log 2>&1 &

CLICKGRAPH_PID=$!
echo "ClickGraph PID: $CLICKGRAPH_PID"

# Wait for server to start
echo -n "Waiting for ClickGraph to start..."
for i in {1..30}; do
    if curl -s "http://localhost:$HTTP_PORT/health" > /dev/null 2>&1; then
        echo -e " ${GREEN}OK${NC}"
        break
    fi
    sleep 1
    echo -n "."
done

# Test 1: Basic Bolt connection
echo ""
echo -e "${YELLOW}Test 1: Bolt Protocol Connection${NC}"
python3 << 'PYEOF'
from neo4j import GraphDatabase
import sys

try:
    driver = GraphDatabase.driver("bolt://localhost:7688", 
                                   auth=("neo4j", "password"))
    with driver.session() as session:
        result = session.run("RETURN 1 as num")
        value = result.single()[0]
        if value == 1:
            print("✅ Bolt connection successful")
            sys.exit(0)
        else:
            print("❌ Unexpected result")
            sys.exit(1)
except Exception as e:
    print(f"❌ Bolt connection failed: {e}")
    sys.exit(1)
finally:
    driver.close()
PYEOF

BOLT_TEST=$?

# Test 2: Simple Cypher query via Bolt
echo ""
echo -e "${YELLOW}Test 2: Cypher Query via Bolt${NC}"
python3 << 'PYEOF'
from neo4j import GraphDatabase
import sys

try:
    driver = GraphDatabase.driver("bolt://localhost:7688", 
                                   auth=("neo4j", "password"))
    with driver.session() as session:
        result = session.run("MATCH (n:User) RETURN count(n) as count")
        count = result.single()[0]
        print(f"✅ Query executed: Found {count} users")
        sys.exit(0)
except Exception as e:
    print(f"❌ Query failed: {e}")
    sys.exit(1)
finally:
    driver.close()
PYEOF

QUERY_TEST=$?

# Test 3: Try Neo4j MCP server (if available)
echo ""
echo -e "${YELLOW}Test 3: Neo4j MCP Server Integration${NC}"

# Create a temporary test script for MCP
cat > /tmp/mcp_test_query.json << 'EOF'
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "query",
    "arguments": {
      "query": "MATCH (n:User) RETURN n.name LIMIT 5"
    }
  }
}
EOF

# Check if @modelcontextprotocol/server-neo4j exists
if npx @modelcontextprotocol/server-neo4j --version &>/dev/null 2>&1; then
    echo "Neo4j MCP server found - testing..."
    # Note: This requires interactive testing with Claude Desktop
    echo -e "${YELLOW}Note: Full MCP testing requires Claude Desktop configuration${NC}"
    echo "Add to Claude config:"
    echo '{'
    echo '  "mcpServers": {'
    echo '    "clickgraph-test": {'
    echo '      "command": "npx",'
    echo '      "args": ["@modelcontextprotocol/server-neo4j", "bolt://localhost:7688"]'
    echo '    }'
    echo '  }'
    echo '}'
else
    echo -e "${YELLOW}Neo4j MCP server not installed. Install with:${NC}"
    echo "  npm install -g @modelcontextprotocol/server-neo4j"
fi

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    kill $CLICKGRAPH_PID 2>/dev/null || true
    rm -f /tmp/mcp_test_query.json
}

trap cleanup EXIT

# Summary
echo ""
echo "========================================"
echo "Test Results Summary"
echo "========================================"
if [ $BOLT_TEST -eq 0 ]; then
    echo -e "${GREEN}✅ Bolt Protocol Connection: PASS${NC}"
else
    echo -e "${RED}❌ Bolt Protocol Connection: FAIL${NC}"
fi

if [ $QUERY_TEST -eq 0 ]; then
    echo -e "${GREEN}✅ Cypher Query Execution: PASS${NC}"
else
    echo -e "${RED}❌ Cypher Query Execution: FAIL${NC}"
fi

echo ""
echo "Test server logs: /tmp/clickgraph_mcp_test.log"
echo "Press Ctrl+C to stop the test server"
echo ""

# Keep running for manual testing
wait $CLICKGRAPH_PID
