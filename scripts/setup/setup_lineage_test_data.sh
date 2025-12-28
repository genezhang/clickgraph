#!/bin/bash
# Setup script for lineage edge constraint test data
# Used by tests/integration/test_edge_constraints.py

set -e

CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-8123}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-test_user}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-test_pass}

echo "🔧 Setting up lineage test data..."

# Execute SQL via ClickHouse HTTP interface
run_sql() {
    local sql="$1"
    echo "  Executing: ${sql:0:60}..."
    echo "$sql" | curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" \
        --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        --data-binary @-
    echo ""
}

# Create database
run_sql "CREATE DATABASE IF NOT EXISTS lineage;"

# Create data_files table
run_sql "DROP TABLE IF EXISTS lineage.data_files;"
run_sql "CREATE TABLE lineage.data_files (
    file_id UInt32,
    file_path String,
    file_size_bytes UInt64,
    created_timestamp DateTime,
    pipeline_stage String,
    file_checksum String
) ENGINE = MergeTree() ORDER BY file_id;"

# Create file_lineage table
run_sql "DROP TABLE IF EXISTS lineage.file_lineage;"
run_sql "CREATE TABLE lineage.file_lineage (
    source_file_id UInt32,
    target_file_id UInt32,
    copy_operation_type String,
    operation_timestamp DateTime,
    operated_by_user String
) ENGINE = MergeTree() ORDER BY (source_file_id, target_file_id);"

# Insert test data - data files
run_sql "INSERT INTO lineage.data_files VALUES
    (1, '/data/raw/input.csv', 1000, '2024-01-01 10:00:00', 'raw', 'abc123'),
    (2, '/data/cleaned/clean.csv', 900, '2024-01-01 11:00:00', 'cleaned', 'def456'),
    (3, '/data/enriched/enrich.csv', 1200, '2024-01-01 12:00:00', 'enriched', 'ghi789'),
    (4, '/data/final/output.csv', 1100, '2024-01-01 13:00:00', 'final', 'jkl012');"

# Insert lineage edges: 1 -> 2 -> 3 -> 4
run_sql "INSERT INTO lineage.file_lineage VALUES
    (1, 2, 'clean', '2024-01-01 11:00:00', 'alice'),
    (2, 3, 'enrich', '2024-01-01 12:00:00', 'bob'),
    (3, 4, 'aggregate', '2024-01-01 13:00:00', 'carol');"

# Insert invalid edge for constraint testing (4 -> 2 violates timestamp constraint)
run_sql "INSERT INTO lineage.file_lineage VALUES
    (4, 2, 'bad_copy', '2024-01-01 14:00:00', 'eve');"

# Verify data
echo ""
echo "📊 Verifying data..."
echo "Data files count:"
run_sql "SELECT count(*) FROM lineage.data_files;"

echo "File lineage count:"
run_sql "SELECT count(*) FROM lineage.file_lineage;"

echo ""
echo "✅ Lineage test data setup complete!"
echo ""
echo "Test data structure:"
echo "  - 4 data files (IDs: 1, 2, 3, 4)"
echo "  - 4 lineage edges (valid: 1→2, 2→3, 3→4; invalid: 4→2)"
echo "  - Edge constraint: from.timestamp <= to.timestamp"
echo "  - Invalid edge 4→2 should be filtered by constraint"
