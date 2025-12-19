#!/bin/bash
# Test CTE column aliasing issue - Issue #4

set -e

cd "$(dirname "$0")"

export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

echo "🧪 Testing CTE Column Aliasing Issue #4"
echo "=========================================="

# Test 1: WITH node + aggregation, RETURN both node property and aggregation
echo ""
echo "Test 1: WITH a, COUNT(b) - RETURN a.name, follows"
echo "--------------------------------------------------"

QUERY1='MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 1 RETURN a.name, follows ORDER BY a.name LIMIT 5'

echo "Query: $QUERY1"
echo ""

cargo run --bin clickgraph -- --sql-only <<EOF
$QUERY1
EOF

echo ""
echo "✅ Test 1 complete"
echo ""

# Test 2: Workaround - pre-project all properties
echo "Test 2: Workaround - WITH a.name, COUNT(b)"
echo "-------------------------------------------"

QUERY2='MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a.name as name, COUNT(b) as follows WHERE follows > 1 RETURN name, follows ORDER BY name LIMIT 5'

echo "Query: $QUERY2"
echo ""

cargo run --bin clickgraph -- --sql-only <<EOF
$QUERY2
EOF

echo ""
echo "✅ Test 2 complete"
echo ""

echo "🎉 All tests executed"
