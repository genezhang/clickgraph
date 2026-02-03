#!/bin/bash
# Quick Neo4j Tools Integration Test
# This script sets up everything needed to test ClickGraph with Neo4j Browser

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  🧪 ClickGraph + Neo4j Browser Quick Test                    ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Set ClickHouse credentials
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export GRAPH_CONFIG_PATH="schemas/test/unified_test_multi_schema.yaml"

echo "1️⃣  Checking for existing ClickGraph processes..."
# Check if HTTP port 8080 is in use
HTTP_PID=$(lsof -ti:8080 2>/dev/null || true)
if [ -n "$HTTP_PID" ]; then
  echo "   Found process on port 8080 (PID: $HTTP_PID)"
  echo "   Stopping old ClickGraph..."
  kill $HTTP_PID 2>/dev/null || true
  sleep 2
fi

# Check if Bolt port 7687 is in use
BOLT_PID=$(lsof -ti:7687 2>/dev/null || true)
if [ -n "$BOLT_PID" ]; then
  echo "   Found process on port 7687 (PID: $BOLT_PID)"
  echo "   Stopping old process..."
  kill $BOLT_PID 2>/dev/null || true
  sleep 2
fi
echo "✅ Ports cleared"
echo ""

echo "2️⃣  Setting up test data..."
./scripts/setup/setup_multi_schema_databases.sh
echo "✅ Test data loaded"
echo ""

echo "3️⃣  Starting ClickGraph..."
cargo run --release --bin clickgraph &
CLICKGRAPH_PID=$!
echo "   PID: $CLICKGRAPH_PID"
echo "   Waiting for ClickGraph to compile and start..."

# Wait for server to be ready (up to 120 seconds for compilation + startup)
MAX_WAIT=120
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ ClickGraph running and responding"
        break
    fi
    
    # Check if process is still alive
    if ! ps -p $CLICKGRAPH_PID > /dev/null 2>&1; then
        echo "❌ ClickGraph process died! Check logs."
        exit 1
    fi
    
    sleep 2
    ELAPSED=$((ELAPSED + 2))
    
    # Show progress every 10 seconds
    if [ $((ELAPSED % 10)) -eq 0 ]; then
        echo "   Still waiting... (${ELAPSED}s elapsed)"
    fi
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "❌ Timeout waiting for ClickGraph to start"
    exit 1
fi
echo ""

echo "4️⃣  Testing procedures via HTTP..."
LABEL_COUNT=$(curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"CALL db.labels()"}' | jq -r '.count // "ERROR"')

if [ "$LABEL_COUNT" == "ERROR" ] || [ -z "$LABEL_COUNT" ]; then
  echo "❌ Procedures not working! Check ClickGraph logs."
  exit 1
fi
echo "   Found $LABEL_COUNT labels"
echo "✅ HTTP procedures working"
echo ""

echo "5️⃣  Starting Neo4j Browser (Docker)..."
if docker ps -a --format '{{.Names}}' | grep -q '^neo4j-browser-test$'; then
  echo "   Removing old container..."
  docker rm -f neo4j-browser-test
fi
docker run -d --name neo4j-browser-test -p 7474:7474 neo4j:5.15 > /dev/null
echo "   Waiting for Browser to start..."
sleep 10
echo "✅ Neo4j Browser running"
echo ""

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  ✅ READY TO TEST!                                            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "🌐 Open in your browser:"
echo "   http://localhost:7474"
echo ""
echo "🔌 Connection settings:"
echo "   URL:      bolt://localhost:7687"
echo "   Username: neo4j"
echo "   Password: password"
echo ""
echo "🧪 Test these queries:"
echo "   CALL dbms.components()"
echo "   CALL db.labels()"
echo "   CALL db.relationshipTypes()"
echo "   CALL db.propertyKeys()"
echo "   MATCH (n:User) RETURN n LIMIT 10"
echo ""
echo "📋 What to verify:"
echo "   ✅ Connection succeeds without errors"
echo "   ✅ All 4 procedures return data"
echo "   ✅ Database info tab shows labels and types"
echo "   ✅ Queries execute successfully"
echo ""
echo "🛑 To stop everything, run:"
echo "   kill $CLICKGRAPH_PID"
echo "   docker stop neo4j-browser-test"
echo ""
echo "⚠️  Note: This script will keep running. Press Ctrl+C to stop ClickGraph."
echo ""

# Function to cleanup on exit
cleanup() {
  echo ""
  echo "🧹 Cleaning up..."
  if ps -p $CLICKGRAPH_PID > /dev/null 2>&1; then
    echo "   Stopping ClickGraph (PID: $CLICKGRAPH_PID)..."
    kill $CLICKGRAPH_PID 2>/dev/null || true
  fi
  echo "   Neo4j Browser will keep running. Stop with: docker stop neo4j-browser-test"
  echo "✅ Done"
}

trap cleanup EXIT

# Wait for user
wait $CLICKGRAPH_PID
