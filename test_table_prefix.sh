#!/usr/bin/env bash
# Test table prefix bug in JOINs within CTEs

export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

echo "Testing table prefix in JOINs within CTEs..."
echo ""
echo "Query: MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 1 RETURN a.name, follows"
echo ""

curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 1 RETURN a.name, follows", "sql_only": true}' 2>&1 | python3 -m json.tool

echo ""
echo "====="
echo "Look for JOINs without database prefix (should have brahmand. prefix)"
