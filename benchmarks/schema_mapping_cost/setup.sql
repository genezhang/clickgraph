-- ============================================================================
-- Experiment: canonical-VIEW Cypher  vs  ClickGraph GENERATED SQL
-- Physical schemas mirror the real ClickGraph variation fixtures:
--   composite_node_id_test.yaml  +  orders_customers_fk.yaml
-- MergeTree ORDER BY = native id columns so PK/index usage is observable.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS db_composite_id;
CREATE DATABASE IF NOT EXISTS test_integration;

-- ---- COMPOSITE-ID physical tables ----
CREATE TABLE IF NOT EXISTS db_composite_id.accounts (
    bank_id UInt32, account_number UInt64, holder_name String,
    account_type String, balance Float64, opened_date Date
) ENGINE = MergeTree ORDER BY (bank_id, account_number);

CREATE TABLE IF NOT EXISTS db_composite_id.transfers (
    transfer_id UInt64,
    from_bank_id UInt32, from_account_number UInt64,
    to_bank_id UInt32, to_account_number UInt64,
    amount Float64, transfer_date Date
) ENGINE = MergeTree ORDER BY (from_bank_id, from_account_number);

-- ---- COMPOSITE-ID canonical graph views (synthetic single-column id) ----
CREATE OR REPLACE VIEW db_composite_id.node_Account AS
SELECT concat(toString(bank_id), '|', toString(account_number)) AS id,
       bank_id, account_number, holder_name, account_type, balance, opened_date
FROM db_composite_id.accounts;

CREATE OR REPLACE VIEW db_composite_id.edge_TRANSFERRED AS
SELECT concat(toString(from_bank_id), '|', toString(from_account_number)) AS start_id,
       concat(toString(to_bank_id),   '|', toString(to_account_number))   AS end_id,
       transfer_id, amount, transfer_date
FROM db_composite_id.transfers;

-- ---- FK-EDGE physical tables ----
CREATE TABLE IF NOT EXISTS test_integration.orders_fk (
    order_id UInt64, customer_id UInt64, order_date Date, total_amount Float64
) ENGINE = MergeTree ORDER BY (order_id);

CREATE TABLE IF NOT EXISTS test_integration.customers_fk (
    customer_id UInt64, name String, email String
) ENGINE = MergeTree ORDER BY (customer_id);

-- ---- FK-EDGE canonical graph views ----
-- The edge IS the orders table; the canonical model still materializes it as a
-- distinct edge relation, so orders_fk is referenced as BOTH node_Order and edge.
CREATE OR REPLACE VIEW test_integration.node_Order AS
SELECT order_id AS id, order_id, order_date, total_amount FROM test_integration.orders_fk;

CREATE OR REPLACE VIEW test_integration.node_Customer AS
SELECT customer_id AS id, customer_id, name, email FROM test_integration.customers_fk;

CREATE OR REPLACE VIEW test_integration.edge_PLACED_BY AS
SELECT order_id AS start_id, customer_id AS end_id FROM test_integration.orders_fk;
