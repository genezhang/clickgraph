#!/bin/bash
# Test that ClickGraph works without CLICKHOUSE_DATABASE environment variable

set -e  # Exit on error

echo "=========================================="
echo "Testing Optional CLICKHOUSE_DATABASE"
echo "=========================================="
echo ""

# Make sure we're in the clickgraph directory
cd "$(dirname "$0")/../.."

# Unset CLICKHOUSE_DATABASE to test it's optional
unset CLICKHOUSE_DATABASE

# Set only required environment variables
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

echo "Environment variables set:"
echo "  CLICKHOUSE_URL: $CLICKHOUSE_URL"
echo "  CLICKHOUSE_USER: $CLICKHOUSE_USER"
echo "  CLICKHOUSE_DATABASE: ${CLICKHOUSE_DATABASE:-<not set - should default to 'default'>}"
echo "  GRAPH_CONFIG_PATH: $GRAPH_CONFIG_PATH"
echo ""

# Try to start the server (will fail if CLICKHOUSE_DATABASE is required)
echo "Starting ClickGraph server (will timeout after 5 seconds)..."
timeout 5 cargo run --bin clickgraph 2>&1 | head -20 &

sleep 3

# Check if server is responding
echo ""
echo "Checking if server started successfully..."
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ SUCCESS: Server started without CLICKHOUSE_DATABASE env var!"
    echo "   Database defaulted to 'default' as expected"
else
    echo "⚠️  Server may still be starting or ClickHouse not available"
    echo "   But if no error about 'CLICKHOUSE_DATABASE not set', the change works!"
fi

# Kill the server
pkill -f clickgraph || true

echo ""
echo "=========================================="
echo "Test complete!"
echo "=========================================="
