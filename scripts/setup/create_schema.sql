-- E-commerce Analytics Database Schema
-- Connect to ClickHouse and create the schema

USE ecommerce;

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id UInt32,
    email String,
    first_name String,
    last_name String,
    age UInt8,
    gender Enum8('M' = 1, 'F' = 2, 'O' = 3),
    country String,
    city String,
    registration_date Date,
    total_spent Decimal(10,2),
    is_premium UInt8 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY customer_id;

-- Products table
CREATE TABLE IF NOT EXISTS products (
    product_id UInt32,
    name String,
    category String,
    brand String,
    price Decimal(8,2),
    rating Float32,
    num_reviews UInt32,
    in_stock UInt8 DEFAULT 1,
    created_date Date
) ENGINE = MergeTree()
ORDER BY product_id;

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id UInt32,
    customer_id UInt32,
    product_id UInt32,
    quantity UInt16,
    unit_price Decimal(8,2),
    total_amount Decimal(10,2),
    order_date Date,
    order_time DateTime,
    status Enum8('pending' = 1, 'shipped' = 2, 'delivered' = 3, 'cancelled' = 4)
) ENGINE = MergeTree()
ORDER BY (order_date, order_id);

-- Reviews table
CREATE TABLE IF NOT EXISTS reviews (
    review_id UInt32,
    customer_id UInt32,
    product_id UInt32,
    order_id UInt32,
    rating UInt8, -- 1-5 stars
    review_text String,
    review_date Date,
    helpful_votes UInt32 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY review_date;

-- Category relationships (products can belong to subcategories)
CREATE TABLE IF NOT EXISTS category_hierarchy (
    parent_category String,
    child_category String,
    level UInt8
) ENGINE = MergeTree()
ORDER BY (parent_category, child_category);