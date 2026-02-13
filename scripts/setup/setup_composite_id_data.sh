#!/bin/bash
# Setup test data for composite node ID schema
# Database: db_composite_id
# Schema: schemas/examples/composite_node_id_test.yaml

set -e

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-test_user}"
CH_PASS="${CLICKHOUSE_PASSWORD:-test_pass}"

run_sql() {
    echo "$1" | curl -s "${CH_URL}/?user=${CH_USER}&password=${CH_PASS}" --data-binary @-
}

echo "=== Setting up db_composite_id ==="

run_sql "CREATE DATABASE IF NOT EXISTS db_composite_id"

# Accounts: composite PK (bank_id, account_number)
run_sql "CREATE TABLE IF NOT EXISTS db_composite_id.accounts (
    bank_id String,
    account_number String,
    holder_name String,
    account_type String,
    balance Float64,
    opened_date Date
) ENGINE = Memory"

# Customers: single PK
run_sql "CREATE TABLE IF NOT EXISTS db_composite_id.customers (
    customer_id UInt64,
    name String,
    email String,
    city String
) ENGINE = Memory"

# Edge: customer owns account (single -> composite)
run_sql "CREATE TABLE IF NOT EXISTS db_composite_id.account_ownership (
    customer_id UInt64,
    bank_id String,
    account_number String,
    role String,
    since Date
) ENGINE = Memory"

# Edge: transfer between accounts (composite -> composite)
run_sql "CREATE TABLE IF NOT EXISTS db_composite_id.transfers (
    transfer_id UInt64,
    from_bank_id String,
    from_account_number String,
    to_bank_id String,
    to_account_number String,
    amount Float64,
    transfer_date Date
) ENGINE = Memory"

# Clear existing data
run_sql "TRUNCATE TABLE IF EXISTS db_composite_id.accounts"
run_sql "TRUNCATE TABLE IF EXISTS db_composite_id.customers"
run_sql "TRUNCATE TABLE IF EXISTS db_composite_id.account_ownership"
run_sql "TRUNCATE TABLE IF EXISTS db_composite_id.transfers"

echo "Tables created. Inserting data..."

run_sql "INSERT INTO db_composite_id.accounts VALUES
('CHASE', 'CHK-001', 'Alice Johnson', 'Checking', 5200.50, '2022-01-15'),
('CHASE', 'SAV-002', 'Alice Johnson', 'Savings', 15000.00, '2022-01-15'),
('CHASE', 'CHK-003', 'Bob Smith', 'Checking', 3100.75, '2022-06-01'),
('CHASE', 'SAV-004', 'Charlie Brown', 'Savings', 8500.00, '2023-03-10'),
('WELLS', 'WF-1001', 'Alice Johnson', 'Checking', 7800.25, '2021-11-20'),
('WELLS', 'WF-1002', 'Diana Lee', 'Savings', 22000.00, '2022-08-05'),
('WELLS', 'WF-1003', 'Bob Smith', 'Checking', 4200.00, '2023-01-15'),
('WELLS', 'WF-1004', 'Eve Martinez', 'Savings', 12500.50, '2023-06-20')"

run_sql "INSERT INTO db_composite_id.customers VALUES
(1, 'Alice Johnson', 'alice@example.com', 'New York'),
(2, 'Bob Smith', 'bob@example.com', 'Chicago'),
(3, 'Charlie Brown', 'charlie@example.com', 'Boston'),
(4, 'Diana Lee', 'diana@example.com', 'Seattle'),
(5, 'Eve Martinez', 'eve@example.com', 'Miami')"

run_sql "INSERT INTO db_composite_id.account_ownership VALUES
(1, 'CHASE', 'CHK-001', 'primary', '2022-01-15'),
(1, 'CHASE', 'SAV-002', 'primary', '2022-01-15'),
(1, 'WELLS', 'WF-1001', 'primary', '2021-11-20'),
(2, 'CHASE', 'CHK-003', 'primary', '2022-06-01'),
(2, 'WELLS', 'WF-1003', 'primary', '2023-01-15'),
(3, 'CHASE', 'SAV-004', 'primary', '2023-03-10'),
(4, 'WELLS', 'WF-1002', 'primary', '2022-08-05'),
(5, 'WELLS', 'WF-1004', 'primary', '2023-06-20'),
(1, 'CHASE', 'CHK-003', 'joint', '2022-06-01'),
(2, 'CHASE', 'SAV-002', 'joint', '2022-03-01')"

run_sql "INSERT INTO db_composite_id.transfers VALUES
(1, 'CHASE', 'CHK-001', 'CHASE', 'SAV-002', 500.00, '2024-01-10'),
(2, 'CHASE', 'CHK-001', 'WELLS', 'WF-1001', 1000.00, '2024-01-15'),
(3, 'WELLS', 'WF-1001', 'CHASE', 'CHK-001', 250.00, '2024-02-01'),
(4, 'CHASE', 'CHK-003', 'WELLS', 'WF-1003', 800.00, '2024-02-10'),
(5, 'WELLS', 'WF-1002', 'WELLS', 'WF-1004', 3000.00, '2024-03-01'),
(6, 'CHASE', 'SAV-004', 'WELLS', 'WF-1002', 1500.00, '2024-03-15'),
(7, 'WELLS', 'WF-1004', 'CHASE', 'SAV-004', 700.00, '2024-04-01'),
(8, 'CHASE', 'CHK-001', 'CHASE', 'CHK-003', 200.00, '2024-04-10')"

echo ""
echo "=== Data loaded ==="
echo "Accounts:  $(run_sql 'SELECT count() FROM db_composite_id.accounts')"
echo "Customers: $(run_sql 'SELECT count() FROM db_composite_id.customers')"
echo "Ownership: $(run_sql 'SELECT count() FROM db_composite_id.account_ownership')"
echo "Transfers: $(run_sql 'SELECT count() FROM db_composite_id.transfers')"
echo ""
echo "Start server with:"
echo "  GRAPH_CONFIG_PATH=schemas/examples/composite_node_id_test.yaml cargo run --bin clickgraph"
