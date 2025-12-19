#!/bin/bash
cd /home/gz/clickgraph
pkill -9 clickgraph
sleep 2

export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DATABASE="brahmand"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

cargo run --bin clickgraph -- --http-port 8080 > /tmp/cg_test.log 2>&1 &
SERVER_PID=$!

sleep 5

echo "Testing three-level WITH nesting with sql_only mode..."
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (a:User) WITH a WHERE a.user_id < 100 WITH a WHERE a.user_id < 50 WITH a WHERE a.user_id < 25 MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.user_id, COUNT(b) as follows LIMIT 5", "sql_only": true}' > /tmp/response.json

echo ""
echo "Response has_sql:"
jq '.has_sql' /tmp/response.json 2>/dev/null || cat /tmp/response.json

echo ""
echo "Response execution_mode:"
jq '.execution_mode' /tmp/response.json 2>/dev/null

kill $SERVER_PID 2>/dev/null
