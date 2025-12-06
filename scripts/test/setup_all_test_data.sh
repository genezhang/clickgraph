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

# Execute SQL file
run_sql_file() {
    local sql_file="$1"
    local description="$2"
    
    if [[ ! -f "$sql_file" ]]; then
        log_warn "SQL file not found: $sql_file"
        return 1
    fi
    
    log_info "Loading: $description"
    curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" \
        --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        --data-binary "@$sql_file" \
        && echo " ✓" || { log_error "Failed to load $sql_file"; return 1; }
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
log_info "=== Standard Benchmark Data ==="
if [[ -f "$PROJECT_ROOT/scripts/setup/setup_medium_benchmark_data.sql" ]]; then
    # Check if data already exists
    count=$(run_sql "SELECT count() FROM brahmand.users_bench" 2>/dev/null || echo "0")
    if [[ "$count" == "0" ]]; then
        log_info "Loading benchmark data (this may take a moment)..."
        run_sql_file "$PROJECT_ROOT/scripts/setup/setup_medium_benchmark_data.sql" "Medium benchmark data"
    else
        log_info "Benchmark data already exists ($count users)"
    fi
else
    log_warn "Medium benchmark data SQL not found, skipping"
fi
echo ""

# 2. Integration test data (test_integration database)
log_info "=== Integration Test Data ==="
run_sql_file "$PROJECT_ROOT/scripts/setup/setup_integration_test_data.sql" "Integration test tables"
echo ""

# 3. Denormalized flights data (for ontime_flights schema)
log_info "=== Denormalized Flights Data ==="
run_sql_file "$PROJECT_ROOT/scripts/test/setup_denormalized_test_data.sql" "Flights test data"
echo ""

# 4. Polymorphic interactions data (for social_polymorphic schema)
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
    -- User follows User
    (1, 2, 'FOLLOWS', 'User', 'User', 1.0),
    (1, 3, 'FOLLOWS', 'User', 'User', 1.0),
    (2, 3, 'FOLLOWS', 'User', 'User', 1.0),
    (3, 1, 'FOLLOWS', 'User', 'User', 1.0),
    
    -- User authored Post
    (1, 101, 'AUTHORED', 'User', 'Post', 1.0),
    (2, 102, 'AUTHORED', 'User', 'Post', 1.0),
    (3, 103, 'AUTHORED', 'User', 'Post', 1.0),
    
    -- User likes Post
    (1, 102, 'LIKES', 'User', 'Post', 0.8),
    (2, 101, 'LIKES', 'User', 'Post', 0.9),
    (3, 102, 'LIKES', 'User', 'Post', 0.7),
    (1, 103, 'LIKES', 'User', 'Post', 0.6),
    
    -- User commented on Post  
    (2, 101, 'COMMENTED', 'User', 'Post', 1.0),
    (3, 101, 'COMMENTED', 'User', 'Post', 1.0),
    (1, 102, 'COMMENTED', 'User', 'Post', 1.0),
    
    -- User shared Post
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

run_sql "
INSERT INTO zeek.dns_log VALUES
    (1591367999.305988, 'CMdzit1AMNsmfAIiQc', '192.168.4.76', 36844, '192.168.4.1', 53, 
     'udp', 12345, 'testmyids.com', 1, 'C_INTERNET', 1, 'A', 0, 'NOERROR',
     0, 0, 1, 1, 0, ['31.3.245.133'], [3600], 0),
    (1591368000.123456, 'CK2fW44phZnrNcM2Xd', '192.168.4.76', 36845, '192.168.4.1', 53,
     'udp', 12346, 'example.com', 1, 'C_INTERNET', 1, 'A', 0, 'NOERROR',
     0, 0, 1, 1, 0, ['93.184.216.34', '93.184.216.35'], [3600, 3600], 0),
    (1591368001.234567, 'CNxm3r2kP8FKnJ9YY5', '10.0.0.1', 40000, '8.8.8.8', 53,
     'udp', 12347, 'google.com', 1, 'C_INTERNET', 1, 'A', 0, 'NOERROR',
     0, 0, 1, 1, 0, ['142.250.80.46'], [300], 0),
    (1591368002.345678, 'CAbC123DefGhIjKlM', '192.168.4.76', 36846, '192.168.4.1', 53,
     'udp', 12348, 'malware.bad', 1, 'C_INTERNET', 1, 'A', 3, 'NXDOMAIN',
     0, 0, 1, 1, 0, [], [], 0),
    (1591368003.456789, 'CXyZ789AbCdEfGhIj', '10.0.0.2', 40001, '192.168.4.1', 53,
     'udp', 12349, 'internal.corp', 1, 'C_INTERNET', 1, 'A', 0, 'NOERROR',
     0, 0, 1, 1, 0, ['192.168.1.100'], [7200], 0)
"
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

run_sql "
INSERT INTO zeek.conn_log VALUES
    (1591367999.305988, 'CMdzit1AMNsmfAIiQc', '192.168.4.76', 36844, '192.168.4.1', 53, 'udp', 'dns', 0.066851, 62, 141, 'SF', 0, 'Dd', 2, 2),
    (1591368000.123456, 'CK2fW44phZnrNcM2Xd', '192.168.4.76', 49152, '10.0.0.1', 80, 'tcp', 'http', 1.234567, 1024, 2048, 'SF', 0, 'ShADadFf', 10, 8),
    (1591368001.789012, 'CNxm3r2kP8FKnJ9YY5', '10.0.0.1', 443, '192.168.4.76', 51234, 'tcp', 'ssl', 5.678901, 4096, 8192, 'SF', 0, 'ShADadFf', 20, 15),
    (1591368002.345678, 'CAbC123DefGhIjKlM', '192.168.4.76', 54321, '8.8.8.8', 53, 'udp', 'dns', 0.012345, 40, 120, 'SF', 0, 'Dd', 1, 1),
    (1591368003.567890, 'CXyZ789AbCdEfGhIj', '8.8.8.8', 53, '192.168.4.76', 54322, 'udp', 'dns', 0.023456, 45, 130, 'SF', 0, 'Dd', 1, 1)
"
echo " ✓ Zeek connection log data loaded"
echo ""

# Summary
log_info "=== Summary ==="
echo "Databases:"
run_sql "SHOW DATABASES" | grep -E "brahmand|test_integration|zeek" | while read db; do
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
for tbl in users_bench posts_bench user_follows_bench post_likes_bench interactions; do
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
