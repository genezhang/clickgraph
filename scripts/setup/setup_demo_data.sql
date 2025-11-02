USE ecommerce;

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

CREATE TABLE IF NOT EXISTS reviews_mem (
    review_id UInt32,
    customer_id UInt32,
    product_id UInt32,
    order_id UInt32,
    rating UInt8,
    review_text String,
    review_date Date,
    helpful_votes UInt32
) ENGINE = Memory;

-- Insert data
INSERT INTO customers_mem VALUES 
    (1, 'alice.johnson@email.com', 'Alice', 'Johnson', 28, 'F', 'USA', 'New York', '2023-01-15', 1250.00, 1),
    (2, 'bob.smith@email.com', 'Bob', 'Smith', 34, 'M', 'Canada', 'Toronto', '2023-02-20', 890.50, 0),
    (3, 'carol.brown@email.com', 'Carol', 'Brown', 42, 'F', 'UK', 'London', '2023-01-10', 2100.75, 1);

INSERT INTO products_mem VALUES
    (101, 'iPhone 15 Pro', 'Electronics', 'Apple', 999.99, 4.5, 1250, 1, '2023-09-15'),
    (102, 'Samsung Galaxy S24', 'Electronics', 'Samsung', 849.99, 4.3, 890, 1, '2024-01-20'),
    (103, 'Sony WH-1000XM5', 'Electronics', 'Sony', 399.99, 4.7, 2100, 1, '2023-05-10');

INSERT INTO orders_mem VALUES
    (1001, 1, 101, 1, 999.99, 999.99, '2024-01-20', '2024-01-20 14:30:00', 'delivered'),
    (1002, 1, 103, 1, 399.99, 399.99, '2024-02-15', '2024-02-15 10:15:00', 'delivered'),
    (1003, 2, 102, 1, 849.99, 849.99, '2024-01-25', '2024-01-25 16:45:00', 'delivered');

INSERT INTO reviews_mem VALUES
    (2001, 1, 101, 1001, 5, 'Amazing phone! Camera quality is outstanding.', '2024-01-25', 15),
    (2002, 1, 103, 1002, 5, 'Best noise cancellation I have ever experienced.', '2024-02-20', 23),
    (2003, 2, 102, 1003, 4, 'Great Android phone. Battery life could be better.', '2024-02-01', 8);