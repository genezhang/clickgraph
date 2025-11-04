-- ============================================================================
-- Integration Test Database Setup for ClickGraph
-- ============================================================================
-- This script sets up the test_integration database with tables and data
-- for running integration tests.
--
-- Run with:
--   docker exec clickhouse clickhouse-client --user test_user --password test_pass --multiquery < setup_integration_test_data.sql
--
-- Or pipe it:
--   Get-Content setup_integration_test_data.sql | docker exec -i clickhouse clickhouse-client --user test_user --password test_pass --multiquery
-- ============================================================================

-- Create test database
CREATE DATABASE IF NOT EXISTS test_integration;

USE test_integration;

-- ============================================================================
-- Drop existing tables (for clean re-runs)
-- ============================================================================
DROP TABLE IF EXISTS follows;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS purchases;
DROP TABLE IF EXISTS friendships;

-- ============================================================================
-- Create Tables (using Memory engine for Windows compatibility)
-- ============================================================================

-- Users table
CREATE TABLE users (
    user_id UInt32,
    name String,
    age UInt8
) ENGINE = Memory;

-- Follows relationship (directed)
CREATE TABLE follows (
    follower_id UInt32,
    followed_id UInt32,
    since String  -- Using String for simplicity in tests
) ENGINE = Memory;

-- Products table (for multi-type graph tests)
CREATE TABLE products (
    product_id UInt32,
    name String,
    price Float64,
    category String
) ENGINE = Memory;

-- Purchases relationship (User -> Product)
CREATE TABLE purchases (
    user_id UInt32,
    product_id UInt32,
    purchase_date String,
    quantity UInt32
) ENGINE = Memory;

-- Friendships (for undirected relationship tests)
CREATE TABLE friendships (
    user_id_1 UInt32,
    user_id_2 UInt32,
    since String
) ENGINE = Memory;

-- ============================================================================
-- Insert Test Data
-- ============================================================================

-- Users: 5 users with varying ages
INSERT INTO users VALUES 
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Charlie', 35),
    (4, 'Diana', 28),
    (5, 'Eve', 32);

-- Follows: Create a graph with various path lengths
-- Path structure: Alice->Bob->Diana->Eve, Alice->Charlie->Diana->Eve
INSERT INTO follows VALUES 
    (1, 2, '2023-01-01'),
    (1, 3, '2023-01-15'),
    (2, 3, '2023-02-01'),
    (2, 4, '2023-02-15'),
    (3, 4, '2023-03-01'),
    (4, 5, '2023-03-15');

-- Products: 3 products for multi-type graph tests
INSERT INTO products VALUES 
    (101, 'Laptop', 999.99, 'Electronics'),
    (102, 'Coffee Maker', 89.99, 'Appliances'),
    (103, 'Running Shoes', 129.99, 'Sports');

-- Purchases: User-Product relationships
INSERT INTO purchases VALUES 
    (1, 101, '2023-06-01', 1),
    (1, 103, '2023-06-15', 2),
    (2, 102, '2023-07-01', 1),
    (3, 101, '2023-07-15', 1),
    (4, 103, '2023-08-01', 1);

-- Friendships: Undirected relationships
INSERT INTO friendships VALUES 
    (1, 2, '2022-01-01'),
    (2, 3, '2022-02-01'),
    (1, 4, '2022-03-01');

-- ============================================================================
-- Verify Setup
-- ============================================================================
SELECT 'Setup Complete!' as status;
SELECT '============================================' as separator;

SELECT 'Users Table:' as info;
SELECT * FROM users ORDER BY user_id;

SELECT '============================================' as separator;
SELECT 'Follows Table:' as info;
SELECT * FROM follows ORDER BY follower_id, followed_id;

SELECT '============================================' as separator;
SELECT 'Products Table:' as info;
SELECT * FROM products ORDER BY product_id;

SELECT '============================================' as separator;
SELECT 'Purchases Table:' as info;
SELECT * FROM purchases ORDER BY user_id, product_id;

SELECT '============================================' as separator;
SELECT 'Friendships Table:' as info;
SELECT * FROM friendships ORDER BY user_id_1, user_id_2;

SELECT '============================================' as separator;
SELECT 'Table Counts:' as info;
SELECT 
    'users' as table_name, 
    count() as row_count 
FROM users
UNION ALL
SELECT 
    'follows' as table_name, 
    count() as row_count 
FROM follows
UNION ALL
SELECT 
    'products' as table_name, 
    count() as row_count 
FROM products
UNION ALL
SELECT 
    'purchases' as table_name, 
    count() as row_count 
FROM purchases
UNION ALL
SELECT 
    'friendships' as table_name, 
    count() as row_count 
FROM friendships;
