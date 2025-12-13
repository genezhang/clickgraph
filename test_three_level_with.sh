#!/bin/bash

# Kill any existing server
pkill -9 clickgraph 2>/dev/null
sleep 2

# Start server
cd /home/gz/clickgraph
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DATABASE="brahmand"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

cargo run --bin clickgraph -- --http-port 8080 > /tmp/clickgraph.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
sleep 5

# Test query
echo "Testing three-level WITH nesting..."
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (a:User) WITH a WHERE a.user_id < 100 WITH a WHERE a.user_id < 50 WITH a WHERE a.user_id < 25 MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.user_id, COUNT(b) as follows LIMIT 5"
  }' 

echo ""
echo ""
echo "=== Checking logs for iteration debug output ==="
grep -E "ITERATION|Starting iterative|Found.*alias groups" /tmp/clickgraph.log | head -20

# Kill server
kill $SERVER_PID 2>/dev/null
