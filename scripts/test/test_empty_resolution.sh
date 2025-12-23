#!/bin/bash
# Test anonymous node resolution

set -e

export RUST_LOG=info
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

echo "=== Testing Anonymous Node Resolution ==="
echo ""
echo "Query: MATCH ()-[:FOLLOWS]->(b:User) RETURN b.name LIMIT 5"
echo ""

curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH ()-[:FOLLOWS]->(b:User) RETURN b.name LIMIT 5"}' | jq .

echo ""
echo "=== Check server logs for resolution messages ==="
