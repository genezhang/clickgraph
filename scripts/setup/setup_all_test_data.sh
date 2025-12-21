#!/bin/bash
# Unified Test Data Setup Script
# Sets up all test databases and fixtures for ClickGraph integration tests
# Run this before executing integration tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ClickHouse connection settings
CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-clickhouse}"
CLICKHOUSE_CLIENT="docker exec $CLICKHOUSE_CONTAINER clickhouse-client"

echo "🔧 ClickGraph Test Data Setup"
echo "================================"
echo "ClickHouse Container: $CLICKHOUSE_CONTAINER"
echo

# Function to run SQL file
run_sql_file() {
    local file=$1
    local description=$2
    
    echo "📝 $description"
    echo "   File: $file"
    
    if [ ! -f "$file" ]; then
        echo "   ❌ File not found: $file"
        return 1
    fi
    
    if $CLICKHOUSE_CLIENT < "$file" 2>&1 | grep -q "Exception\|Error"; then
        echo "   ❌ Failed to load $file"
        return 1
    else
        echo "   ✅ Success"
        return 0
    fi
}

# Function to verify table data
verify_table() {
    local database=$1
    local table=$2
    
    count=$($CLICKHOUSE_CLIENT -q "SELECT count(*) FROM $database.$table" 2>/dev/null || echo "0")
    echo "   📊 $database.$table: $count rows"
}

echo "Step 1: Setting up test_integration database"
echo "---------------------------------------------"

# Filesystem test data (matrix tests)
run_sql_file "$PROJECT_ROOT/tests/fixtures/data/filesystem_test_data.sql" \
    "Loading filesystem test data"
verify_table "test_integration" "fs_objects"
verify_table "test_integration" "fs_parent"
echo

# Group membership test data (matrix tests)
run_sql_file "$PROJECT_ROOT/tests/fixtures/data/group_membership_test_data.sql" \
    "Loading group membership test data"
verify_table "test_integration" "users"
verify_table "test_integration" "groups"
verify_table "test_integration" "memberships"
echo

# Test integration suite data (conftest fixtures)
if [ -f "$PROJECT_ROOT/tests/integration/suites/test_integration/setup.sql" ]; then
    run_sql_file "$PROJECT_ROOT/tests/integration/suites/test_integration/setup.sql" \
        "Loading test_integration suite data"
    verify_table "test_integration" "products"
    verify_table "test_integration" "purchases"
    verify_table "test_integration" "follows"
    verify_table "test_integration" "friendships"
    echo
fi

echo "Step 2: Setting up brahmand database (benchmark data)"
echo "-------------------------------------------------------"

# Social benchmark data
if [ -f "$PROJECT_ROOT/tests/integration/suites/social_benchmark/setup.sql" ]; then
    run_sql_file "$PROJECT_ROOT/tests/integration/suites/social_benchmark/setup.sql" \
        "Loading social benchmark data"
    verify_table "brahmand" "users_bench"
    verify_table "brahmand" "posts_bench"
    verify_table "brahmand" "user_follows_bench"
    verify_table "brahmand" "post_likes_bench"
    echo
fi

# Security graph data (if needed)
if [ -f "$PROJECT_ROOT/examples/data_security/setup_schema.sql" ]; then
    echo "📝 Loading security graph data"
    $CLICKHOUSE_CLIENT < "$PROJECT_ROOT/examples/data_security/setup_schema.sql" 2>&1 | grep -v "^$" || true
    verify_table "brahmand" "ds_users"
    verify_table "brahmand" "ds_groups"
    verify_table "brahmand" "ds_fs_objects"
    verify_table "brahmand" "ds_memberships"
    verify_table "brahmand" "ds_permissions"
    echo
fi

echo "Step 3: Setting up zeek database (network logs)"
echo "------------------------------------------------"

# Zeek data should already exist from benchmarks
if $CLICKHOUSE_CLIENT -q "SHOW DATABASES" | grep -q "zeek"; then
    echo "   ✅ Zeek database exists"
    verify_table "zeek" "conn_log"
    verify_table "zeek" "dns_log"
else
    echo "   ⚠️  Zeek database not found (optional for matrix tests)"
fi
echo

echo "Step 4: Setting up denormalized edges (flights data)"
echo "------------------------------------------------------"

if [ -f "$PROJECT_ROOT/scripts/test/setup_denormalized_test_data.sql" ]; then
    run_sql_file "$PROJECT_ROOT/scripts/test/setup_denormalized_test_data.sql" \
        "Loading denormalized flights data"
    verify_table "test_integration" "flights"
    echo
fi

echo
echo "================================"
echo "✅ Test Data Setup Complete!"
echo "================================"
echo
echo "Summary of loaded data:"
$CLICKHOUSE_CLIENT -q "
SELECT 
    database,
    name AS table,
    total_rows AS rows,
    formatReadableSize(total_bytes) AS size
FROM system.tables 
WHERE database IN ('test_integration', 'brahmand', 'zeek') 
    AND total_rows > 0
ORDER BY database, name
FORMAT PrettyCompact
"

echo
echo "Ready to run integration tests! 🚀"
echo
echo "Usage:"
echo "  pytest tests/integration/         # Run all integration tests"
echo "  pytest tests/integration/matrix/  # Run matrix tests"
echo "  pytest tests/integration/wiki/    # Run wiki tests"
echo
