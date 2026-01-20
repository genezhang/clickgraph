#!/bin/bash
# Load test_integration database tables for integration tests
# Run this script after setting up ClickHouse container

set -e

CLICKHOUSE_USER="${CLICKHOUSE_USER:-test_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-test_pass}"

echo "Loading test_integration data into ClickHouse..."
echo "Using user: $CLICKHOUSE_USER"

# Load group_membership test data
echo "📦 Loading group_membership data..."
cat tests/fixtures/data/group_membership_test_data.sql | \
  docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD"
echo "✅ group_membership data loaded"

# Load filesystem test data  
echo "📦 Loading filesystem data..."
cat tests/fixtures/data/filesystem_test_data.sql | \
  docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD"
echo "✅ filesystem data loaded"

# Load general test_integration data
echo "📦 Loading general test_integration data..."
cat tests/fixtures/data/test_integration_data.sql | \
  docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD"
echo "✅ test_integration data loaded"

echo ""
echo "🎉 All test data loaded successfully!"
echo ""
echo "Verify with:"
echo "  docker exec clickhouse clickhouse-client --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query \"SHOW TABLES FROM test_integration\""
