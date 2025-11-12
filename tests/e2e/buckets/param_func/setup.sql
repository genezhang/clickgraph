-- Setup SQL for Parameter + Function E2E Tests
-- Database: test_param_func
-- Creates simple test data for testing parameter and function combinations

-- Create Users table
CREATE TABLE IF NOT EXISTS test_param_func.users (
    id UInt32,
    name String,
    email String,
    age UInt8,
    status String,
    created_at DateTime
) ENGINE = Memory;

-- Insert test data
INSERT INTO test_param_func.users VALUES
    (1, 'Alice', 'alice@example.com', 28, 'active', '2024-01-15 10:00:00'),
    (2, 'Bob', 'bob@example.com', 35, 'active', '2024-02-20 14:30:00'),
    (3, 'Charlie', 'charlie@example.com', 42, 'inactive', '2024-03-10 09:15:00'),
    (4, 'Diana', 'diana@example.com', 31, 'active', '2024-04-05 16:45:00'),
    (5, 'Eve', 'eve@example.com', 26, 'pending', '2024-05-12 11:20:00');

-- Create Products table  
CREATE TABLE IF NOT EXISTS test_param_func.products (
    id UInt32,
    name String,
    price Float64,
    category String,
    in_stock Boolean
) ENGINE = Memory;

-- Insert test data
INSERT INTO test_param_func.products VALUES
    (101, 'Laptop', 999.99, 'electronics', true),
    (102, 'Mouse', 29.99, 'electronics', true),
    (103, 'Desk', 349.50, 'furniture', false),
    (104, 'Chair', 199.00, 'furniture', true),
    (105, 'Monitor', 450.00, 'electronics', true);

-- Create Orders table
CREATE TABLE IF NOT EXISTS test_param_func.orders (
    id UInt32,
    user_id UInt32,
    product_id UInt32,
    quantity UInt16,
    total Float64,
    order_date DateTime
) ENGINE = Memory;

-- Insert test data
INSERT INTO test_param_func.orders VALUES
    (1001, 1, 101, 1, 999.99, '2024-06-01 10:00:00'),
    (1002, 2, 102, 2, 59.98, '2024-06-02 11:30:00'),
    (1003, 1, 105, 1, 450.00, '2024-06-03 14:15:00'),
    (1004, 3, 103, 1, 349.50, '2024-06-04 09:45:00'),
    (1005, 4, 104, 2, 398.00, '2024-06-05 16:20:00');
