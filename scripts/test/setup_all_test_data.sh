#!/usr/bin/env bash
# ============================================================================
# Setup All Test Data for ClickGraph Integration Tests
# ============================================================================
# This script loads all test data needed to run the full integration test suite.
#
# Prerequisites:
#   - ClickHouse running (docker-compose up -d)
#   - ClickGraph server running
#
# Usage:
#   ./scripts/test/setup_all_test_data.sh
#
# With custom ClickHouse credentials:
#   CLICKHOUSE_USER=myuser CLICKHOUSE_PASSWORD=mypass ./scripts/test/setup_all_test_data.sh
# ============================================================================

set -e

# Configuration
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-test_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-test_pass}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Execute SQL file (handles multi-statement files)
run_sql_file() {
    local sql_file="$1"
    local description="$2"
    
    if [[ ! -f "$sql_file" ]]; then
        log_warn "SQL file not found: $sql_file"
        return 1
    fi
    
    log_info "Loading: $description"
    
    # Split the SQL file into individual statements and execute them
    # Remove comments and empty lines, then split on semicolons
    sed 's/--.*$//g' "$sql_file" | \
    tr '\n' ' ' | \
    sed 's/;/;\n/g' | \
    grep -v '^[[:space:]]*$' | \
    while IFS= read -r statement; do
        # Remove leading/trailing whitespace
        statement=$(echo "$statement" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        if [[ -n "$statement" && "$statement" != ";" ]]; then
            if ! run_sql "$statement" > /dev/null 2>&1; then
                log_error "Failed to execute statement: $statement"
                return 1
            fi
        fi
    done
    
    echo " ✓"
}

# Execute SQL query
run_sql() {
    local query="$1"
    curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" \
        --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        --data "$query"
}

echo "=============================================="
echo "ClickGraph Test Data Setup"
echo "=============================================="
echo "Host: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
echo "User: $CLICKHOUSE_USER"
echo ""

# Check ClickHouse connectivity
log_info "Checking ClickHouse connection..."
if ! run_sql "SELECT 1" > /dev/null 2>&1; then
    log_error "Cannot connect to ClickHouse. Is it running?"
    exit 1
fi
echo " ✓ ClickHouse is reachable"
echo ""

# Create databases if they don't exist
log_info "Creating databases..."
run_sql "CREATE DATABASE IF NOT EXISTS test_integration"
run_sql "CREATE DATABASE IF NOT EXISTS zeek"
run_sql "CREATE DATABASE IF NOT EXISTS brahmand"
echo " ✓ Databases ready"
echo ""

# 1. Standard benchmark data (social_benchmark schema)
log_info "=== Benchmark Test Data (Small Scale) ==="
run_sql_file "$PROJECT_ROOT/tests/fixtures/data/benchmark_small_data.sql" "Small-scale benchmark data"
echo ""

# 2. Integration test data (test_integration database)
log_info "=== Integration Test Data ==="
run_sql_file "$PROJECT_ROOT/tests/fixtures/data/test_integration_data.sql" "Integration test tables"
echo ""

# 3. Denormalized flights data (for ontime_flights schema)
log_info "=== Denormalized Flights Data ==="
run_sql_file "$PROJECT_ROOT/scripts/test/setup_denormalized_test_data.sql" "Flights test data"
# Also create in default database for ontime_benchmark schema
run_sql "CREATE TABLE IF NOT EXISTS default.flights AS test_integration.flights"
run_sql "INSERT INTO default.flights SELECT * FROM test_integration.flights"
log_info "Copied flights table to default database for ontime_benchmark schema"
echo ""

# 4. Filesystem test data (for filesystem schema)
log_info "=== Filesystem Test Data ==="
run_sql_file "$PROJECT_ROOT/tests/fixtures/data/filesystem_test_data.sql" "Filesystem test data"
echo ""

# 5. Group membership test data (for group membership schema)
log_info "=== Group Membership Test Data ==="
run_sql_file "$PROJECT_ROOT/tests/fixtures/data/group_membership_test_data.sql" "Group membership test data"
echo ""

# 6. Property expressions test data (for property_expressions schema)
log_info "=== Property Expressions Test Data ==="
run_sql_file "$PROJECT_ROOT/tests/fixtures/data/setup_property_expressions.sql" "Property expressions test data"
echo ""

# 7. Polymorphic interactions data (for social_polymorphic schema)
log_info "=== Polymorphic Interactions Data ==="
# Create interactions table if it doesn't exist
run_sql "
CREATE TABLE IF NOT EXISTS brahmand.interactions (
    from_id UInt64,
    to_id UInt64,
    interaction_type String,
    from_type String,
    to_type String,
    timestamp DateTime DEFAULT now(),
    interaction_weight Float32 DEFAULT 1.0
) ENGINE = Memory
"

# Insert sample polymorphic interactions
run_sql "
INSERT INTO brahmand.interactions (from_id, to_id, interaction_type, from_type, to_type, interaction_weight) VALUES
    (1, 2, 'FOLLOWS', 'User', 'User', 1.0),
    (1, 3, 'FOLLOWS', 'User', 'User', 1.0),
    (2, 3, 'FOLLOWS', 'User', 'User', 1.0),
    (3, 1, 'FOLLOWS', 'User', 'User', 1.0),
    (1, 101, 'AUTHORED', 'User', 'Post', 1.0),
    (2, 102, 'AUTHORED', 'User', 'Post', 1.0),
    (3, 103, 'AUTHORED', 'User', 'Post', 1.0),
    (1, 102, 'LIKES', 'User', 'Post', 0.8),
    (2, 101, 'LIKES', 'User', 'Post', 0.9),
    (3, 102, 'LIKES', 'User', 'Post', 0.7),
    (1, 103, 'LIKES', 'User', 'Post', 0.6),
    (2, 101, 'COMMENTED', 'User', 'Post', 1.0),
    (3, 101, 'COMMENTED', 'User', 'Post', 1.0),
    (1, 102, 'COMMENTED', 'User', 'Post', 1.0),
    (1, 102, 'SHARED', 'User', 'Post', 1.0),
    (3, 101, 'SHARED', 'User', 'Post', 1.0)
"
echo " ✓ Polymorphic interactions data loaded"
echo ""

# 5. Zeek DNS log data (for zeek_dns schema)  
log_info "=== Zeek DNS Log Data ==="
run_sql "
CREATE TABLE IF NOT EXISTS zeek.dns_log (
    ts Float64,
    uid String,
    \`id.orig_h\` String,
    \`id.orig_p\` UInt16,
    \`id.resp_h\` String,
    \`id.resp_p\` UInt16,
    proto String,
    trans_id UInt16,
    query String,
    qclass UInt16,
    qclass_name String,
    qtype UInt16,
    qtype_name String,
    rcode UInt16,
    rcode_name String,
    AA UInt8,
    TC UInt8,
    RD UInt8,
    RA UInt8,
    Z UInt8,
    answers Array(String),
    TTLs Array(Float64),
    rejected UInt8
) ENGINE = Memory
"

# Insert DNS data one by one to avoid parsing issues
run_sql "INSERT INTO zeek.dns_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, trans_id, query, qclass, qclass_name, qtype, qtype_name, rcode, rcode_name, AA, TC, RD, RA, Z, answers, TTLs, rejected) VALUES (1591367999.305988, 'CMdzit1AMNsmfAIiQc', '192.168.4.76', 36844, '192.168.4.1', 53, 'udp', 12345, 'testmyids.com', 1, 'C_INTERNET', 1, 'A', 0, 'NOERROR', 0, 0, 1, 1, 0, ['31.3.245.133'], [3600.0], 0)"
run_sql "INSERT INTO zeek.dns_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, trans_id, query, qclass, qclass_name, qtype, qtype_name, rcode, rcode_name, AA, TC, RD, RA, Z, answers, TTLs, rejected) VALUES (1591368000.123456, 'CK2fW44phZnrNcM2Xd', '192.168.4.76', 36845, '192.168.4.1', 53, 'udp', 12346, 'example.com', 1, 'C_INTERNET', 1, 'A', 0, 'NOERROR', 0, 0, 1, 1, 0, ['93.184.216.34', '93.184.216.35'], [3600.0, 3600.0], 0)"
run_sql "INSERT INTO zeek.dns_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, trans_id, query, qclass, qclass_name, qtype, qtype_name, rcode, rcode_name, AA, TC, RD, RA, Z, answers, TTLs, rejected) VALUES (1591368001.234567, 'CNxm3r2kP8FKnJ9YY5', '10.0.0.1', 40000, '8.8.8.8', 53, 'udp', 12347, 'google.com', 1, 'C_INTERNET', 1, 'A', 0, 'NOERROR', 0, 0, 1, 1, 0, ['142.250.80.46'], [300.0], 0)"
run_sql "INSERT INTO zeek.dns_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, trans_id, query, qclass, qclass_name, qtype, qtype_name, rcode, rcode_name, AA, TC, RD, RA, Z, answers, TTLs, rejected) VALUES (1591368002.345678, 'CAbC123DefGhIjKlM', '192.168.4.76', 36846, '192.168.4.1', 53, 'udp', 12348, 'malware.bad', 1, 'C_INTERNET', 1, 'A', 3, 'NXDOMAIN', 0, 0, 1, 1, 0, [], [], 0)"
run_sql "INSERT INTO zeek.dns_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, trans_id, query, qclass, qclass_name, qtype, qtype_name, rcode, rcode_name, AA, TC, RD, RA, Z, answers, TTLs, rejected) VALUES (1591368003.456789, 'CXyZ789AbCdEfGhIj', '10.0.0.2', 40001, '192.168.4.1', 53, 'udp', 12349, 'internal.corp', 1, 'C_INTERNET', 1, 'A', 0, 'NOERROR', 0, 0, 1, 1, 0, ['192.168.1.100'], [7200.0], 0)"
echo " ✓ Zeek DNS log data loaded"
echo ""

# 6. Zeek connection log data (for zeek_conn schema)
log_info "=== Zeek Connection Log Data ==="
run_sql "
CREATE TABLE IF NOT EXISTS zeek.conn_log (
    ts Float64,
    uid String,
    \`id.orig_h\` String,
    \`id.orig_p\` UInt16,
    \`id.resp_h\` String,
    \`id.resp_p\` UInt16,
    proto String,
    service String,
    duration Float64,
    orig_bytes UInt64,
    resp_bytes UInt64,
    conn_state String,
    missed_bytes UInt64,
    history String,
    orig_pkts UInt64,
    resp_pkts UInt64
) ENGINE = Memory
"

# Insert connection data one by one
run_sql "INSERT INTO zeek.conn_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, service, duration, orig_bytes, resp_bytes, conn_state, missed_bytes, history, orig_pkts, resp_pkts) VALUES (1591367999.305988, 'CMdzit1AMNsmfAIiQc', '192.168.4.76', 36844, '192.168.4.1', 53, 'udp', 'dns', 0.066851, 62, 141, 'SF', 0, 'Dd', 2, 2)"
run_sql "INSERT INTO zeek.conn_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, service, duration, orig_bytes, resp_bytes, conn_state, missed_bytes, history, orig_pkts, resp_pkts) VALUES (1591368000.123456, 'CK2fW44phZnrNcM2Xd', '192.168.4.76', 49152, '10.0.0.1', 80, 'tcp', 'http', 1.234567, 1024, 2048, 'SF', 0, 'ShADadFf', 10, 8)"
run_sql "INSERT INTO zeek.conn_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, service, duration, orig_bytes, resp_bytes, conn_state, missed_bytes, history, orig_pkts, resp_pkts) VALUES (1591368001.789012, 'CNxm3r2kP8FKnJ9YY5', '10.0.0.1', 443, '192.168.4.76', 51234, 'tcp', 'ssl', 5.678901, 4096, 8192, 'SF', 0, 'ShADadFf', 20, 15)"
run_sql "INSERT INTO zeek.conn_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, service, duration, orig_bytes, resp_bytes, conn_state, missed_bytes, history, orig_pkts, resp_pkts) VALUES (1591368002.345678, 'CAbC123DefGhIjKlM', '192.168.4.76', 54321, '8.8.8.8', 53, 'udp', 'dns', 0.012345, 40, 120, 'SF', 0, 'Dd', 1, 1)"
run_sql "INSERT INTO zeek.conn_log (ts, uid, \`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, proto, service, duration, orig_bytes, resp_bytes, conn_state, missed_bytes, history, orig_pkts, resp_pkts) VALUES (1591368003.567890, 'CXyZ789AbCdEfGhIj', '8.8.8.8', 53, '192.168.4.76', 54322, 'udp', 'dns', 0.023456, 45, 130, 'SF', 0, 'Dd', 1, 1)"
echo " ✓ Zeek connection log data loaded"
echo " ✓ Zeek connection log data loaded"
echo ""

# 7. Multi-tenant parameterized view data
log_info "=== Multi-Tenant Parameterized View Data ==="

# Create base tables
run_sql "
CREATE TABLE IF NOT EXISTS brahmand.multi_tenant_users (
    user_id UInt32,
    tenant_id String,
    name String,
    email String,
    country String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (tenant_id, user_id)
"

run_sql "
CREATE TABLE IF NOT EXISTS brahmand.multi_tenant_orders (
    order_id UInt32,
    tenant_id String,
    user_id UInt32,
    product String,
    amount Float64,
    order_date DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (tenant_id, order_id)
"

run_sql "
CREATE TABLE IF NOT EXISTS brahmand.multi_tenant_friendships (
    friendship_id UInt32,
    tenant_id String,
    user_id_from UInt32,
    user_id_to UInt32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (tenant_id, friendship_id)
"

# Check if data exists, insert if empty
user_count=$(run_sql "SELECT count() FROM brahmand.multi_tenant_users" 2>/dev/null || echo "0")
if [[ "$user_count" == "0" ]]; then
    run_sql "
    INSERT INTO brahmand.multi_tenant_users (user_id, tenant_id, name, email, country) VALUES
        (1, 'acme', 'Alice Anderson', 'alice@acme.com', 'USA'),
        (2, 'acme', 'Bob Brown', 'bob@acme.com', 'USA'),
        (3, 'acme', 'Carol Chen', 'carol@acme.com', 'China'),
        (4, 'globex', 'David Davis', 'david@globex.com', 'UK'),
        (5, 'globex', 'Emma Evans', 'emma@globex.com', 'UK'),
        (6, 'globex', 'Frank Foster', 'frank@globex.com', 'France'),
        (7, 'initech', 'Grace Green', 'grace@initech.com', 'USA'),
        (8, 'initech', 'Henry Hall', 'henry@initech.com', 'Canada')
    "
    
    run_sql "
    INSERT INTO brahmand.multi_tenant_orders (order_id, tenant_id, user_id, product, amount) VALUES
        (101, 'acme', 1, 'Widget A', 29.99),
        (102, 'acme', 1, 'Widget B', 49.99),
        (103, 'acme', 2, 'Gadget X', 99.99),
        (201, 'globex', 4, 'Service Plan A', 199.99),
        (202, 'globex', 5, 'Service Plan B', 299.99),
        (301, 'initech', 7, 'Consulting', 500.00)
    "
    
    run_sql "
    INSERT INTO brahmand.multi_tenant_friendships (friendship_id, tenant_id, user_id_from, user_id_to) VALUES
        (1, 'acme', 1, 2),
        (2, 'acme', 2, 3),
        (3, 'globex', 4, 5),
        (4, 'globex', 5, 6)
    "
    echo " ✓ Multi-tenant base data inserted"
else
    echo " ✓ Multi-tenant data already exists ($user_count users)"
fi

# Create single-parameter parameterized views
run_sql "CREATE OR REPLACE VIEW brahmand.users_by_tenant AS SELECT user_id, tenant_id, name, email, country, created_at FROM brahmand.multi_tenant_users WHERE tenant_id = {tenant_id:String}"
run_sql "CREATE OR REPLACE VIEW brahmand.orders_by_tenant AS SELECT order_id, tenant_id, user_id, product, amount, order_date FROM brahmand.multi_tenant_orders WHERE tenant_id = {tenant_id:String}"
run_sql "CREATE OR REPLACE VIEW brahmand.friendships_by_tenant AS SELECT friendship_id, tenant_id, user_id_from, user_id_to, friendship_date FROM brahmand.multi_tenant_friendships WHERE tenant_id = {tenant_id:String}"

# Create multi-parameter parameterized views
run_sql "CREATE OR REPLACE VIEW brahmand.users_by_tenant_and_country AS SELECT user_id, tenant_id, name, email, country, created_at FROM brahmand.multi_tenant_users WHERE tenant_id = {tenant_id:String} AND country = {country:String}"
run_sql "CREATE OR REPLACE VIEW brahmand.orders_by_tenant_and_date AS SELECT order_id, tenant_id, user_id, product, amount, order_date FROM brahmand.multi_tenant_orders WHERE tenant_id = {tenant_id:String} AND order_date >= {start_date:Date} AND order_date <= {end_date:Date}"

echo " ✓ Multi-tenant parameterized views created"
echo ""

# 8. Data security example data
log_info "=== Data Security Example Data ==="
run_sql "CREATE DATABASE IF NOT EXISTS data_security"
run_sql_file "$PROJECT_ROOT/examples/data_security/setup_schema.sql" "Data security schema and data"
echo ""

# Summary
log_info "=== Summary ==="
echo "Databases:"
run_sql "SHOW DATABASES" | grep -E "brahmand|test_integration|zeek|data_security" | while read db; do
    echo "  - $db"
done

echo ""
echo "Tables in test_integration:"
run_sql "SHOW TABLES FROM test_integration" | while read tbl; do
    count=$(run_sql "SELECT count() FROM test_integration.$tbl" 2>/dev/null || echo "?")
    echo "  - $tbl: $count rows"
done

echo ""
echo "Tables in zeek:"
run_sql "SHOW TABLES FROM zeek" | while read tbl; do
    count=$(run_sql "SELECT count() FROM zeek.$tbl" 2>/dev/null || echo "?")
    echo "  - $tbl: $count rows"
done

echo ""
echo "Key tables in brahmand:"
for tbl in users_bench posts_bench user_follows_bench post_likes_bench interactions multi_tenant_users multi_tenant_orders multi_tenant_friendships; do
    count=$(run_sql "SELECT count() FROM brahmand.$tbl" 2>/dev/null || echo "0")
    echo "  - $tbl: $count rows"
done

echo ""
log_info "=============================================="
log_info "Test data setup complete!"
log_info "=============================================="
echo ""
echo "Next steps:"
echo "  1. Start ClickGraph: ./target/debug/clickgraph --http-port 8765"
echo "  2. Load schemas: CLICKGRAPH_URL=http://localhost:8765 python scripts/test/load_test_schemas.py"
echo "  3. Run tests: CLICKGRAPH_URL=http://localhost:8765 pytest tests/integration/"
