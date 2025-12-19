#!/bin/bash
# Test script for cross-branch JOIN detection

# Kill existing server
pkill -f clickgraph
sleep 1

# Start server with proper environment
export CLICKHOUSE_URL=http://localhost:8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DATABASE=test_zeek
export GRAPH_CONFIG_PATH=./tests/fixtures/schemas/zeek_merged_test.yaml
RUST_LOG=debug target/release/clickgraph --http-port 8080 > /tmp/clickgraph.log 2>&1 &
SERVER_PID=$!

echo "Server starting (PID: $SERVER_PID)..."
sleep 3

# Check if server is running
if ! ps -p $SERVER_PID > /dev/null; then
    echo "❌ Server failed to start!"
    echo "=== Log output ==="
    tail -20 /tmp/clickgraph.log
    exit 1
fi

echo "✅ Server running"

# Test cross-branch pattern query
echo ""
echo "=== Testing Cross-Branch Pattern ==="
echo "Query: MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), (srcip)-[:ACCESSED]->(dest:IP) WHERE srcip.ip = '192.168.1.10' RETURN srcip.ip, d.name, dest.ip"

curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), (srcip)-[:ACCESSED]->(dest:IP) WHERE srcip.ip = '"'"'192.168.1.10'"'"' RETURN srcip.ip, d.name, dest.ip"
  }' | jq '.'

echo ""
echo "=== Server Logs (last 50 lines) ==="
tail -50 /tmp/clickgraph.log | grep -E "(cross-branch|Cross-branch|JOIN|check_and_generate)"

# Stop server
echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
