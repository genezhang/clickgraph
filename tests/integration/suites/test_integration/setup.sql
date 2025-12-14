-- Setup SQL for Test Integration Suite
-- Database: test_integration
-- Creates basic test tables for integration testing

-- Create Users table
CREATE TABLE IF NOT EXISTS test_integration.users (
    user_id UInt32,
    name String,
    age UInt32
) ENGINE = Memory;

-- Create Follows relationship table
CREATE TABLE IF NOT EXISTS test_integration.follows (
    follower_id UInt32,
    followed_id UInt32,
    since String
) ENGINE = Memory;

-- Create Products table
CREATE TABLE IF NOT EXISTS test_integration.products (
    product_id UInt32,
    name String,
    price Float64,
    category String
) ENGINE = Memory;

-- Create Purchases relationship table
CREATE TABLE IF NOT EXISTS test_integration.purchases (
    user_id UInt32,
    product_id UInt32,
    purchase_date String,
    quantity UInt32
) ENGINE = Memory;

-- Create Friendships relationship table
CREATE TABLE IF NOT EXISTS test_integration.friendships (
    user_id_1 UInt32,
    user_id_2 UInt32,
    since String
) ENGINE = Memory;

-- Insert test data - Users (5 users for path testing)
INSERT INTO test_integration.users VALUES
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Charlie', 35),
    (4, 'Diana', 28),
    (5, 'Eve', 32);

-- Insert Follows (forms paths: Alice->Bob->Diana->Eve, Alice->Charlie->Diana)
INSERT INTO test_integration.follows VALUES
    (1, 2, '2022-01-01'),  -- Alice -> Bob
    (1, 3, '2022-02-01'),  -- Alice -> Charlie
    (2, 3, '2022-03-01'),  -- Bob -> Charlie
    (2, 4, '2022-04-01'),  -- Bob -> Diana
    (3, 4, '2022-05-01'),  -- Charlie -> Diana
    (4, 5, '2022-06-01');  -- Diana -> Eve

-- Insert Products
INSERT INTO test_integration.products VALUES
    (101, 'Laptop', 999.99, 'Electronics'),
    (102, 'Mouse', 29.99, 'Electronics'),
    (103, 'Desk', 299.99, 'Furniture'),
    (104, 'Chair', 199.99, 'Furniture');

-- Insert Purchases
INSERT INTO test_integration.purchases VALUES
    (1, 101, '2022-01-15', 1),
    (1, 102, '2022-01-15', 2),
    (2, 103, '2022-02-20', 1),
    (3, 101, '2022-03-10', 1),
    (4, 104, '2022-04-05', 1);

-- Insert Friendships
INSERT INTO test_integration.friendships VALUES
    (1, 2, '2021-01-01'),
    (1, 3, '2021-02-01'),
    (2, 4, '2021-03-01');
