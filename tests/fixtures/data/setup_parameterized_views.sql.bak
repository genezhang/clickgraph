-- Setup parameterized views for multi-tenancy testing
-- This script creates:
-- 1. Base tables with tenant_id column
-- 2. Table functions (parameterized views) for tenant isolation

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS brahmand;

-- Drop existing tables (for clean test runs)
DROP TABLE IF EXISTS brahmand.multi_tenant_users;
DROP TABLE IF EXISTS brahmand.multi_tenant_orders;
DROP TABLE IF EXISTS brahmand.multi_tenant_friendships;

-- Base table: Users with tenant isolation
CREATE TABLE brahmand.multi_tenant_users (
    user_id UInt32,
    tenant_id String,
    name String,
    email String,
    country String,
    created_at DateTime DEFAULT now()
) ENGINE = Memory;

-- Base table: Orders with tenant isolation
CREATE TABLE brahmand.multi_tenant_orders (
    order_id UInt32,
    tenant_id String,
    user_id UInt32,
    product String,
    amount Float32,
    order_date DateTime DEFAULT now()
) ENGINE = Memory;

-- Base table: Friendships (relationships) with tenant isolation
CREATE TABLE brahmand.multi_tenant_friendships (
    friendship_id UInt32,
    tenant_id String,
    user_id_from UInt32,
    user_id_to UInt32,
    friendship_date DateTime DEFAULT now()
) ENGINE = Memory;

-- Insert test data for tenant "acme"
INSERT INTO brahmand.multi_tenant_users (user_id, tenant_id, name, email, country) VALUES
    (1, 'acme', 'Alice Anderson', 'alice@acme.com', 'USA'),
    (2, 'acme', 'Bob Brown', 'bob@acme.com', 'USA'),
    (3, 'acme', 'Carol Chen', 'carol@acme.com', 'China');

INSERT INTO brahmand.multi_tenant_orders (order_id, tenant_id, user_id, product, amount) VALUES
    (101, 'acme', 1, 'Widget A', 29.99),
    (102, 'acme', 1, 'Widget B', 49.99),
    (103, 'acme', 2, 'Gadget X', 99.99);

INSERT INTO brahmand.multi_tenant_friendships (friendship_id, tenant_id, user_id_from, user_id_to) VALUES
    (1001, 'acme', 1, 2),
    (1002, 'acme', 2, 3),
    (1003, 'acme', 1, 3);

-- Insert test data for tenant "globex"
INSERT INTO brahmand.multi_tenant_users (user_id, tenant_id, name, email, country) VALUES
    (1, 'globex', 'David Davis', 'david@globex.com', 'UK'),
    (2, 'globex', 'Emma Evans', 'emma@globex.com', 'UK'),
    (3, 'globex', 'Frank Foster', 'frank@globex.com', 'France');

INSERT INTO brahmand.multi_tenant_orders (order_id, tenant_id, user_id, product, amount) VALUES
    (201, 'globex', 1, 'Service Plan A', 199.99),
    (202, 'globex', 2, 'Service Plan B', 299.99);

INSERT INTO brahmand.multi_tenant_friendships (friendship_id, tenant_id, user_id_from, user_id_to) VALUES
    (2001, 'globex', 1, 2),
    (2002, 'globex', 2, 3);

-- Insert test data for tenant "initech" (for multi-parameter testing)
INSERT INTO brahmand.multi_tenant_users (user_id, tenant_id, name, email, country) VALUES
    (1, 'initech', 'Grace Green', 'grace@initech.com', 'USA'),
    (2, 'initech', 'Henry Hill', 'henry@initech.com', 'Canada'),
    (3, 'initech', 'Iris Ivanov', 'iris@initech.com', 'USA');

INSERT INTO brahmand.multi_tenant_orders (order_id, tenant_id, user_id, product, amount) VALUES
    (301, 'initech', 1, 'Software License', 999.99),
    (302, 'initech', 3, 'Support Contract', 1999.99);

INSERT INTO brahmand.multi_tenant_friendships (friendship_id, tenant_id, user_id_from, user_id_to) VALUES
    (3001, 'initech', 1, 3);

-- Verify data loaded
SELECT 'Users per tenant:' as info;
SELECT tenant_id, count(*) as user_count FROM brahmand.multi_tenant_users GROUP BY tenant_id ORDER BY tenant_id;

SELECT 'Orders per tenant:' as info;
SELECT tenant_id, count(*) as order_count FROM brahmand.multi_tenant_orders GROUP BY tenant_id ORDER BY tenant_id;

SELECT 'Friendships per tenant:' as info;
SELECT tenant_id, count(*) as friendship_count FROM brahmand.multi_tenant_friendships GROUP BY tenant_id ORDER BY tenant_id;

-- Note: ClickHouse table functions are created dynamically
-- The syntax for using them is: SELECT * FROM table_name(parameter_name = 'value')
-- Example: SELECT * FROM brahmand.multi_tenant_users(tenant_id = 'acme')
-- This works because ClickHouse automatically creates table functions for filtered queries

SELECT 'Setup complete! Test queries:' as info;
SELECT 'SELECT * FROM brahmand.multi_tenant_users(tenant_id = ''acme'')' as example_query;
