#!/bin/bash
# Load all test fixtures for integration tests
# Run this script to set up test data before running integration tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR"

# Check if ClickHouse is running
if ! docker exec clickhouse clickhouse-client -q "SELECT 1" > /dev/null 2>&1; then
    echo "Error: ClickHouse container is not running or not accessible"
    exit 1
fi

echo "Loading test fixtures..."

# Load filesystem test data
echo "  ✓ Loading filesystem test data..."
docker exec -i clickhouse clickhouse-client --multiquery < "$FIXTURES_DIR/filesystem_test_data.sql"

# Load group membership test data
echo "  ✓ Loading group membership test data..."
docker exec -i clickhouse clickhouse-client --multiquery < "$FIXTURES_DIR/group_membership_test_data.sql"

# Load OnTime flights test data (for denormalized schema testing)
echo "  ✓ Loading OnTime flights test data..."
docker exec -i clickhouse clickhouse-client --multiquery < "$FIXTURES_DIR/ontime_test_data.sql"

# Verify data was loaded
echo ""
echo "Verification:"
echo "  Filesystem objects: $(docker exec clickhouse clickhouse-client -q "SELECT count(*) FROM test_integration.fs_objects")"
echo "  Filesystem parents: $(docker exec clickhouse clickhouse-client -q "SELECT count(*) FROM test_integration.fs_parent")"
echo "  Users: $(docker exec clickhouse clickhouse-client -q "SELECT count(*) FROM test_integration.users")"
echo "  Groups: $(docker exec clickhouse clickhouse-client -q "SELECT count(*) FROM test_integration.groups")"
echo "  Memberships: $(docker exec clickhouse clickhouse-client -q "SELECT count(*) FROM test_integration.memberships")"
echo "  Flights (OnTime): $(docker exec clickhouse clickhouse-client -q "SELECT count(*) FROM default.flights")"

echo ""
echo "✅ All test fixtures loaded successfully!"
