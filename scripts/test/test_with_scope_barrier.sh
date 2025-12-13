#!/bin/bash
# Test WITH scope barrier semantics
# MATCH (a)-[]->(b) WITH a MATCH (a)-[]->(b)
# The second b should be different from the first b (scope shielding)

set -e

CLICKGRAPH_BIN="${CLICKGRAPH_BIN:-./target/debug/clickgraph}"
SERVER_PORT="${SERVER_PORT:-8080}"
GRAPH_CONFIG="${GRAPH_CONFIG_PATH:-./benchmarks/social_network/schemas/social_benchmark.yaml}"

echo "=== Test: WITH Scope Barrier (OpenCypher Semantics) ==="
echo "Query: MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a MATCH (a)-[:FOLLOWS]->(b2:User) RETURN a.name, b2.name"
echo ""

# Start server in background if not running
if ! curl -s "http://localhost:${SERVER_PORT}/health" > /dev/null 2>&1; then
    echo "Starting ClickGraph server..."
    GRAPH_CONFIG_PATH="$GRAPH_CONFIG" "$CLICKGRAPH_BIN" --http-port "$SERVER_PORT" > /tmp/clickgraph_test.log 2>&1 &
    SERVER_PID=$!
    sleep 3
    
    # Check if server started
    if ! curl -s "http://localhost:${SERVER_PORT}/health" > /dev/null 2>&1; then
        echo "ERROR: Server failed to start"
        cat /tmp/clickgraph_test.log
        exit 1
    fi
else
    echo "Server already running"
    SERVER_PID=""
fi

# Test query with scope barrier
echo "Testing scope barrier..."
RESPONSE=$(curl -s -X POST "http://localhost:${SERVER_PORT}/query" \
    -H "Content-Type: application/json" \
    -d '{
        "query": "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a MATCH (a)-[:FOLLOWS]->(b2:User) RETURN a.name, b2.name LIMIT 5",
        "sql_only": true
    }')

echo "Response:"
echo "$RESPONSE" | jq -r '.sql // .error // .'
echo ""

# Check for correct SQL generation
if echo "$RESPONSE" | grep -q "with_a_cte"; then
    echo "✓ CTE generated for WITH clause"
else
    echo "✗ CTE not found in SQL"
fi

if echo "$RESPONSE" | grep -q "FROM with_a_cte"; then
    echo "✓ Second MATCH correctly uses CTE"
else
    echo "✗ Second MATCH doesn't use CTE"
fi

# Cleanup
if [ -n "$SERVER_PID" ]; then
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true
fi

echo ""
echo "=== Test Complete ==="
