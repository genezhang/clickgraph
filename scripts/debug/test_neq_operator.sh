#!/bin/bash
# Test script to debug != operator issue

export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

echo "Testing != operator with debug output..."
echo "Query: MATCH (u:User) WHERE u.user_id != 1 RETURN u.name"
echo ""

curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User) WHERE u.user_id != 1 RETURN u.name"}' \
  2>/dev/null | jq .

echo ""
echo "---"
echo ""
echo "For comparison, testing <> operator (which works)..."
echo "Query: MATCH (u:User) WHERE u.user_id <> 1 RETURN u.name"
echo ""

curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User) WHERE u.user_id <> 1 RETURN u.name"}' \
  2>/dev/null | jq .
