-- Setup demo data for E2E tests
-- Windows-compatible script using Memory engine

USE brahmand;

-- Create in-memory tables for demo purposes
CREATE TABLE IF NOT EXISTS customers_mem (
    customer_id UInt32,
    email String,
    first_name String,
    last_name String,
    age UInt8,
    gender String,
    country String,
    city String,
    registration_date Date,
    total_spent Float64,
    is_premium UInt8
) ENGINE = Memory;

CREATE TABLE IF NOT EXISTS products_mem (
    product_id UInt32,
    name String,
    category String,
    brand String,
    price Float64,
    rating Float32,
    num_reviews UInt32,
    in_stock UInt8,
    created_date Date
) ENGINE = Memory;

CREATE TABLE IF NOT EXISTS orders_mem (
    order_id UInt32,
    customer_id UInt32,
    product_id UInt32,
    quantity UInt16,
    unit_price Float64,
    total_amount Float64,
    order_date Date,
    order_time DateTime,
    status String
) ENGINE = Memory;

-- Insert sample customers
INSERT INTO customers_mem VALUES
    (1, 'alice@example.com', 'Alice', 'Johnson', 28, 'F', 'USA', 'New York', '2023-01-15', 2450.50, 1),
    (2, 'bob@example.com', 'Bob', 'Smith', 35, 'M', 'USA', 'San Francisco', '2023-02-20', 1820.75, 0),
    (3, 'carol@example.com', 'Carol', 'Williams', 42, 'F', 'UK', 'London', '2023-03-10', 3200.00, 1);

-- Insert sample products
INSERT INTO products_mem VALUES
    (1, 'Laptop Pro 15', 'Electronics', 'TechBrand', 1299.99, 4.5, 1250, 1, '2023-01-01'),
    (2, 'Wireless Mouse', 'Accessories', 'TechBrand', 29.99, 4.3, 856, 1, '2023-02-15'),
    (3, 'USB-C Cable 6ft', 'Accessories', 'CableCo', 12.99, 4.7, 2340, 1, '2023-03-01');

-- Insert sample orders (represents PURCHASED relationships)
INSERT INTO orders_mem VALUES
    (1, 1, 1, 1, 1299.99, 1299.99, '2023-04-01', '2023-04-01 14:30:00', 'delivered'),
    (2, 1, 2, 2, 29.99, 59.98, '2023-04-05', '2023-04-05 10:15:00', 'delivered'),
    (3, 2, 3, 3, 12.99, 38.97, '2023-04-10', '2023-04-10 16:45:00', 'shipped'),
    (4, 3, 1, 1, 1299.99, 1299.99, '2023-04-12', '2023-04-12 09:20:00', 'delivered');

SELECT 'Setup complete!' AS status,
       (SELECT count() FROM customers_mem) AS customers,
       (SELECT count() FROM products_mem) AS products,
       (SELECT count() FROM orders_mem) AS orders;
