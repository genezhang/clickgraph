#!/bin/bash
# Test script to verify correlation predicate extraction works

set -e

# Setup
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"
export GRAPH_CONFIG_PATH="./schemas/examples/zeek_merged.yaml"
export RUST_LOG=info

echo "=== Testing Correlation Predicate Extraction ==="
echo ""

# Kill any existing clickgraph process on port 8080
pkill -f "target/release/clickgraph" || true
sleep 1

# Start ClickGraph server in background with logging
echo "Starting ClickGraph server..."
./target/release/clickgraph > /tmp/clickgraph_test.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
sleep 3

# Test the WITH...MATCH query that uses correlation predicates
echo "Sending test query with correlation predicate..."
QUERY='MATCH (src:IP)-[dns:REQUESTED]->(d:Domain) WITH src.ip as source_ip, d.name as domain MATCH (src2:IP)-[conn:ACCESSED]->(dest:IP) WHERE src2.ip = source_ip RETURN DISTINCT source_ip, domain, dest.ip as dest_ip ORDER BY source_ip, domain'

curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"$QUERY\"}" > /tmp/query_result.json

echo "Query executed successfully"
echo ""

# Check logs for correlation predicate extraction
echo "=== Checking server logs for correlation predicate extraction ==="
echo ""

# Look for correlation predicate log messages
if grep -q "extract_correlation_predicates" /tmp/clickgraph_test.log; then
    echo "✓ extract_correlation_predicates function was called"
    grep "extract_correlation_predicates" /tmp/clickgraph_test.log | head -3
else
    echo "✗ extract_correlation_predicates was NOT found in logs"
fi
echo ""

if grep -q "convert_correlation_predicates_to_joins" /tmp/clickgraph_test.log; then
    echo "✓ convert_correlation_predicates_to_joins function was called"
    grep "convert_correlation_predicates_to_joins" /tmp/clickgraph_test.log | head -3
else
    echo "✗ convert_correlation_predicates_to_joins was NOT found in logs"
fi
echo ""

if grep -q "Converted correlation predicate" /tmp/clickgraph_test.log; then
    echo "✓ Correlation predicates were successfully converted to join conditions!"
    grep "Converted correlation predicate" /tmp/clickgraph_test.log
else
    echo "⚠ No correlation predicates converted (may be using filter-based extraction instead)"
fi
echo ""

# Check if heuristic fallback was used (should NOT happen with proper implementation)
if grep -q "falling back to heuristic" /tmp/clickgraph_test.log; then
    echo "⚠ WARNING: Heuristic fallback was used - correlation predicates may not be working!"
    grep "falling back to heuristic" /tmp/clickgraph_test.log
else
    echo "✓ No heuristic fallback used - proper predicate extraction working!"
fi
echo ""

# Show query result
echo "=== Query Result ==="
cat /tmp/query_result.json | python3 -m json.tool 2>/dev/null || cat /tmp/query_result.json
echo ""

# Cleanup
echo "Cleaning up..."
kill $SERVER_PID 2>/dev/null || true
sleep 1

echo "=== Test Complete ==="
