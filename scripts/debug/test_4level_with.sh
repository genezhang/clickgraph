#!/bin/bash

# Test 4-level WITH query from KNOWN_ISSUES.md

set -e

export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

echo "=== Testing 4-Level WITH Query ==="
echo ""

# The problematic query from KNOWN_ISSUES
QUERY='MATCH (a:User) WHERE a.user_id = 1 WITH a 
MATCH (a)-[:FOLLOWS]->(b:User) WITH a, b 
MATCH (b)-[:FOLLOWS]->(c:User) WITH b, c 
MATCH (c)-[:FOLLOWS]->(d:User) RETURN b.name, c.name, d.name'

echo "Query:"
echo "$QUERY"
echo ""

# First check SQL generation
echo "=== Generated SQL (sql_only=true) ==="
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"$QUERY\", \"sql_only\": true}" | python3 -m json.tool

echo ""
echo ""

# Now try actual execution
echo "=== Query Execution ==="
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"$QUERY\"}" | python3 -m json.tool

echo ""
