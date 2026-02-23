#!/bin/bash
# Setup zeek database for coupled edges testing
#
# Creates the zeek database with dns_log and conn_log tables
# demonstrating the "coupled edges" pattern where multiple relationships
# are defined from the same table.
#
# Coupled Edges Pattern:
#   - dns_log defines 2 edges: REQUESTED (IP->Domain) and RESOLVED_TO (Domain->ResolvedIP)
#   - conn_log defines 1 edge: ACCESSED (IP->IP)
#
# Corresponding schema: schemas/dev/zeek_merged_test.yaml

set -e

CLICKHOUSE_USER="${CLICKHOUSE_USER:-test_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"

# Build clickhouse-client command with auth
if [ -n "$CLICKHOUSE_PASSWORD" ]; then
    CH_CMD="docker exec clickhouse clickhouse-client --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD"
else
    CH_CMD="docker exec clickhouse clickhouse-client --user $CLICKHOUSE_USER"
fi

echo "=== Setting up zeek database ==="

# Create database
$CH_CMD -q "CREATE DATABASE IF NOT EXISTS zeek"

# Create dns_log table
$CH_CMD -q "
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
    TTLs Array(UInt32)
) ENGINE = Memory
"

# Create conn_log table
$CH_CMD -q "
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

echo "Tables created. Inserting data..."

# Insert dns_log data
$CH_CMD -q "
INSERT INTO zeek.dns_log (\`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, ts, uid, proto, trans_id, query, qclass, qclass_name, qtype, qtype_name, rcode, rcode_name, AA, TC, RD, RA, Z, answers, TTLs) VALUES
('192.168.1.10', 54321, '8.8.8.8', 53, 1700000001.0, 'DNS001', 'udp', 1, 'example.com', 1, 'IN', 1, 'A', 0, 'NOERROR', 0, 0, 1, 1, 0, ['93.184.216.34'], [3600]),
('192.168.1.10', 54322, '8.8.8.8', 53, 1700000002.0, 'DNS002', 'udp', 2, 'malware.bad', 1, 'IN', 1, 'A', 0, 'NOERROR', 0, 0, 1, 1, 0, ['10.0.0.99'], [3600]),
('192.168.1.20', 54323, '8.8.8.8', 53, 1700000003.0, 'DNS003', 'udp', 3, 'google.com', 1, 'IN', 1, 'A', 0, 'NOERROR', 0, 0, 1, 1, 0, ['142.250.80.46'], [300]),
('192.168.1.10', 54324, '8.8.8.8', 53, 1700000004.0, 'DNS004', 'udp', 4, 'cdn.example.com', 1, 'IN', 1, 'A', 0, 'NOERROR', 0, 0, 1, 1, 0, ['93.184.216.34', '93.184.216.35'], [60, 60]),
('192.168.1.30', 54325, '8.8.8.8', 53, 1700000005.0, 'DNS005', 'udp', 5, 'test.com', 1, 'IN', 1, 'A', 0, 'NOERROR', 0, 0, 1, 1, 0, ['1.2.3.4'], [600])
"

# Insert conn_log data
$CH_CMD -q "
INSERT INTO zeek.conn_log (\`id.orig_h\`, \`id.orig_p\`, \`id.resp_h\`, \`id.resp_p\`, ts, uid, proto, service, duration, orig_bytes, resp_bytes, conn_state, missed_bytes, history, orig_pkts, resp_pkts) VALUES
('192.168.1.10', 54321, '93.184.216.34', 80, 1700000100.0, 'CONN001', 'tcp', 'http', 1.5, 1024, 2048, 'SF', 0, 'ShADadFf', 10, 8),
('192.168.1.10', 54322, '10.0.0.99', 443, 1700000101.0, 'CONN002', 'tcp', 'ssl', 5.2, 4096, 8192, 'SF', 0, 'ShADadFf', 20, 15),
('192.168.1.20', 54323, '142.250.80.46', 443, 1700000102.0, 'CONN003', 'tcp', 'ssl', 2.3, 2048, 4096, 'SF', 0, 'ShADadFf', 15, 12),
('192.168.1.10', 54324, '93.184.216.35', 443, 1700000103.0, 'CONN004', 'tcp', 'ssl', 0.8, 512, 1024, 'SF', 0, 'ShADadFf', 5, 4),
('192.168.1.30', 54325, '1.2.3.4', 80, 1700000104.0, 'CONN005', 'tcp', 'http', 3.1, 3072, 6144, 'SF', 0, 'ShADadFf', 25, 20)
"

echo ""
echo "=== Data loaded ==="
echo ""
echo "DNS logs:"
$CH_CMD -q "SELECT count(*) as count FROM zeek.dns_log"
echo ""
echo "Connection logs:"
$CH_CMD -q "SELECT count(*) as count FROM zeek.conn_log"
echo ""
echo "Start server with:"
echo "  GRAPH_CONFIG_PATH=schemas/test/unified_test_multi_schema.yaml cargo run --bin clickgraph"
