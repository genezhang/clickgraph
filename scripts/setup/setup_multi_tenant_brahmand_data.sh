#!/bin/bash
# Setup test data for multi-tenant parameterized views
# Database: brahmand
# Schemas: schemas/test/multi_tenant.yaml, schemas/test/multi_tenant_multi_param.yaml,
#          schemas/test/multi_tenant_date_range.yaml
#
# Tables:
#   - multi_tenant_users, multi_tenant_orders, multi_tenant_friendships
# Parameterized views:
#   - users_by_tenant, orders_by_tenant, friendships_by_tenant (single-param)
#   - users_by_tenant_and_country, orders_by_tenant_and_date (multi-param)
#
# Extracted from scripts/test/setup_all_test_data.sh's "Multi-Tenant
# Parameterized View Data" section so CI/nightly can set this data up without
# running the full heavyweight script (which also touches test_integration,
# zeek, and data_security — already covered by other targeted setup scripts).

set -e

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-test_user}"
CH_PASS="${CLICKHOUSE_PASSWORD:-test_pass}"

run_sql() {
    echo "$1" | curl -s "${CH_URL}/?user=${CH_USER}&password=${CH_PASS}" --data-binary @-
}

echo "=== Setting up brahmand (multi-tenant parameterized views) ==="

run_sql "CREATE DATABASE IF NOT EXISTS brahmand"

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

# Single-parameter parameterized views
run_sql "CREATE OR REPLACE VIEW brahmand.users_by_tenant AS SELECT user_id, tenant_id, name, email, country, created_at FROM brahmand.multi_tenant_users WHERE tenant_id = {tenant_id:String}"
run_sql "CREATE OR REPLACE VIEW brahmand.orders_by_tenant AS SELECT order_id, tenant_id, user_id, product, amount, order_date FROM brahmand.multi_tenant_orders WHERE tenant_id = {tenant_id:String}"
run_sql "CREATE OR REPLACE VIEW brahmand.friendships_by_tenant AS SELECT friendship_id, tenant_id, user_id_from, user_id_to, created_at FROM brahmand.multi_tenant_friendships WHERE tenant_id = {tenant_id:String}"

# Multi-parameter parameterized views
run_sql "CREATE OR REPLACE VIEW brahmand.users_by_tenant_and_country AS SELECT user_id, tenant_id, name, email, country, created_at FROM brahmand.multi_tenant_users WHERE tenant_id = {tenant_id:String} AND country = {country:String}"
run_sql "CREATE OR REPLACE VIEW brahmand.orders_by_tenant_and_date AS SELECT order_id, tenant_id, user_id, product, amount, order_date FROM brahmand.multi_tenant_orders WHERE tenant_id = {tenant_id:String} AND order_date >= {start_date:Date} AND order_date <= {end_date:Date}"

echo " ✓ Multi-tenant parameterized views created"
