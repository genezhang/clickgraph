#!/bin/bash
#
# Manual Property Pruning Demo
#
# This script demonstrates property pruning optimization by running queries
# with the ClickGraph server in verbose logging mode.
#
# Watch for these log patterns:
# - "PropertyRequirementsAnalyzer: Found requirements for N aliases"
# - "expand_alias_properties_core: Alias 'X' pruned Y properties"
# - "X → Y columns, Z% reduction"

set -e

CLICKGRAPH_BIN="${CLICKGRAPH_BIN:-./target/release/clickgraph}"
SCHEMA="${GRAPH_CONFIG_PATH:-./benchmarks/social_network/schemas/social_benchmark.yaml}"
PORT="${PORT:-8080}"

echo "🔍 Property Pruning Manual Validation"
echo "======================================"
echo ""
echo "This script will:"
echo "1. Start ClickGraph server with verbose logging"
echo "2. Run test queries and show property requirements"
echo "3. Display pruning statistics"
echo ""
echo "Configuration:"
echo "  Binary: $CLICKGRAPH_BIN"
echo "  Schema: $SCHEMA"
echo "  Port: $PORT"
echo ""

# Check if binary exists
if [ ! -f "$CLICKGRAPH_BIN" ]; then
    echo "❌ ClickGraph binary not found at $CLICKGRAPH_BIN"
    echo "   Build with: cargo build --release"
    exit 1
fi

# Check if schema exists
if [ ! -f "$SCHEMA" ]; then
    echo "❌ Schema file not found at $SCHEMA"
    exit 1
fi

echo "Press Ctrl+C to stop the server after reviewing logs"
echo ""
echo "Starting ClickGraph server..."
echo ""

# Export environment variables
export CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:-test_user}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-test_pass}"
export CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-brahmand}"
export GRAPH_CONFIG_PATH="$SCHEMA"
export RUST_LOG="${RUST_LOG:-info}"

# Start server and pipe output through grep to highlight key messages
$CLICKGRAPH_BIN --http-port $PORT 2>&1 | grep --line-buffered -E \
    '(PropertyRequirements|expand_alias_properties_core|pruned|properties:|✂️|📋|🔍|✅|⚠️)' &

SERVER_PID=$!

# Give server time to start
sleep 2

echo ""
echo "Server started (PID: $SERVER_PID)"
echo ""
echo "Now run test queries in another terminal:"
echo ""
echo "# Test 1: Basic property selection (should prune)"
echo "curl -X POST http://localhost:$PORT/query \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"query\": \"MATCH (u:User) WHERE u.user_id = 1 RETURN u.name\", \"database\": \"brahmand\"}'"
echo ""
echo "# Test 2: collect() aggregation (should prune)"
echo "curl -X POST http://localhost:$PORT/query \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"query\": \"MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.user_id = 1 RETURN collect(f)[0].name\", \"database\": \"brahmand\"}'"
echo ""
echo "# Test 3: Wildcard (should NOT prune)"
echo "curl -X POST http://localhost:$PORT/query \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"query\": \"MATCH (u:User) WHERE u.user_id = 1 RETURN u\", \"database\": \"brahmand\"}'"
echo ""
echo "Watch the logs above for property pruning messages!"
echo ""
echo "Press Ctrl+C when done..."
echo ""

# Wait for interrupt
trap "echo ''; echo 'Stopping server...'; kill $SERVER_PID 2>/dev/null; exit 0" INT TERM

wait $SERVER_PID
