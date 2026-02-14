#!/bin/bash
# Setup test data for FK-edge schema (foreign key as relationship)
# Database: db_fk_edge
# Schema: schemas/dev/orders_customers_fk.yaml
#
# Tables:
#   - customers_fk (node: Customer)
#   - orders_fk (node: Order, also edge table for PLACED_BY)
#
# FK-edge pattern: orders_fk.customer_id is a FK to customers_fk.customer_id
# The Order table IS the edge table (no separate edge table).

set -e

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-test_user}"
CH_PASS="${CLICKHOUSE_PASSWORD:-test_pass}"

run_sql() {
    echo "$1" | curl -s "${CH_URL}/?user=${CH_USER}&password=${CH_PASS}" --data-binary @-
}

echo "=== Setting up db_fk_edge ==="

run_sql "CREATE DATABASE IF NOT EXISTS db_fk_edge"

# Customers table
run_sql "CREATE TABLE IF NOT EXISTS db_fk_edge.customers_fk (
    customer_id UInt64,
    name String,
    email String
) ENGINE = Memory"

# Orders table (also the edge table for PLACED_BY relationship)
run_sql "CREATE TABLE IF NOT EXISTS db_fk_edge.orders_fk (
    order_id UInt64,
    customer_id UInt64,
    order_date Date,
    total_amount Float64
) ENGINE = Memory"

# Clear existing data
run_sql "TRUNCATE TABLE IF EXISTS db_fk_edge.customers_fk"
run_sql "TRUNCATE TABLE IF EXISTS db_fk_edge.orders_fk"

echo "Tables created. Inserting data..."

run_sql "INSERT INTO db_fk_edge.customers_fk VALUES
(100, 'Alice', 'alice@shop.com'),
(101, 'Bob', 'bob@shop.com'),
(102, 'Carol', 'carol@shop.com'),
(103, 'David', 'david@shop.com')"

run_sql "INSERT INTO db_fk_edge.orders_fk VALUES
(1, 100, '2024-01-10', 99.99),
(2, 100, '2024-01-20', 149.50),
(3, 101, '2024-02-05', 29.99),
(4, 101, '2024-02-15', 199.00),
(5, 102, '2024-03-01', 75.00),
(6, 103, '2024-03-10', 250.00),
(7, 100, '2024-04-01', 50.00),
(8, 102, '2024-04-15', 120.00)"

echo ""
echo "=== Data loaded ==="
echo "Customers: $(run_sql 'SELECT count() FROM db_fk_edge.customers_fk')"
echo "Orders:    $(run_sql 'SELECT count() FROM db_fk_edge.orders_fk')"
echo ""
echo "Start server with:"
echo "  GRAPH_CONFIG_PATH=schemas/dev/orders_customers_fk.yaml cargo run --bin clickgraph"
