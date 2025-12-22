-- Test Integration Database Setup
-- This creates tables and data for TestUser, TestProduct, TEST_FOLLOWS, TEST_PURCHASED, TEST_FRIENDS_WITH
-- Used by the simple_graph fixture and most integration tests

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS test_integration;

-- Drop existing tables to ensure clean state
DROP TABLE IF EXISTS test_integration.users;
DROP TABLE IF EXISTS test_integration.follows;
DROP TABLE IF EXISTS test_integration.products;
DROP TABLE IF EXISTS test_integration.purchases;
DROP TABLE IF EXISTS test_integration.friendships;
DROP TABLE IF EXISTS test_integration.flights;

-- Users table for TestUser nodes
CREATE TABLE test_integration.users (
    user_id UInt32,
    name String,
    email String,
    age UInt8,
    city String,
    country String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY user_id;

-- Follows relationship table for TEST_FOLLOWS
CREATE TABLE test_integration.follows (
    follower_id UInt32,
    followed_id UInt32,
    since String,
    strength Float32 DEFAULT 1.0,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (follower_id, followed_id);

-- Products table for TestProduct nodes
CREATE TABLE test_integration.products (
    product_id UInt32,
    name String,
    price Float32,
    category String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY product_id;

-- Purchases relationship table for TEST_PURCHASED
CREATE TABLE test_integration.purchases (
    user_id UInt32,
    product_id UInt32,
    quantity UInt32 DEFAULT 1,
    purchase_date DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (user_id, product_id);

-- Friendships relationship table for TEST_FRIENDS_WITH (bidirectional)
CREATE TABLE test_integration.friendships (
    user1_id UInt32,
    user2_id UInt32,
    since String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (user1_id, user2_id);

-- Flights table for Airport->Airport relationships (denormalized pattern)
CREATE TABLE test_integration.flights (
    flight_id UInt32,
    flight_number String,
    airline String,
    
    -- Origin airport (from_node) - denormalized properties
    Origin String,
    OriginCityName String,
    OriginState String,
    
    -- Destination airport (to_node) - denormalized properties
    Dest String,
    DestCityName String,
    DestState String,
    
    -- Flight properties
    dep_time String,
    arr_time String,
    distance_miles UInt32
) ENGINE = MergeTree() ORDER BY flight_id;

-- ============================================================================
-- INSERT DATA
-- ============================================================================

-- Insert test users (5 users: Alice, Bob, Charlie, Diana, Eve)
INSERT INTO test_integration.users (user_id, name, email, age, city, country) VALUES
    (1, 'Alice', 'alice@example.com', 30, 'New York', 'USA'),
    (2, 'Bob', 'bob@example.com', 25, 'London', 'UK'),
    (3, 'Charlie', 'charlie@example.com', 35, 'Paris', 'France'),
    (4, 'Diana', 'diana@example.com', 28, 'Berlin', 'Germany'),
    (5, 'Eve', 'eve@example.com', 32, 'Tokyo', 'Japan');

-- Insert follows relationships
-- Graph: Alice(1)->Bob(2)->Charlie(3)->Diana(4), Alice(1)->Charlie(3), Bob(2)->Diana(4)
-- Eve is isolated
INSERT INTO test_integration.follows (follower_id, followed_id, since, strength) VALUES
    (1, 2, '2023-01-15', 0.9),
    (2, 3, '2023-02-20', 0.8),
    (3, 4, '2023-03-25', 0.7),
    (1, 3, '2023-04-10', 0.6),
    (2, 4, '2023-05-05', 0.5);

-- Insert test products (5 products in different categories)
INSERT INTO test_integration.products (product_id, name, price, category) VALUES
    (1, 'Laptop', 999.99, 'Electronics'),
    (2, 'Phone', 599.99, 'Electronics'),
    (3, 'Book', 19.99, 'Books'),
    (4, 'Headphones', 149.99, 'Electronics'),
    (5, 'Coffee Maker', 79.99, 'Appliances');

-- Insert purchases (users buying products)
INSERT INTO test_integration.purchases (user_id, product_id, quantity) VALUES
    (1, 1, 1),
    (1, 3, 2),
    (2, 2, 1),
    (3, 4, 1),
    (4, 5, 1);

-- Insert friendships (bidirectional relationships)
INSERT INTO test_integration.friendships (user1_id, user2_id, since) VALUES
    (1, 2, '2022-06-01'),
    (2, 3, '2022-07-15'),
    (3, 4, '2022-08-20');

-- Insert denormalized flight data
INSERT INTO test_integration.flights VALUES
    (1, 'AA100', 'American Airlines', 
     'LAX', 'Los Angeles', 'CA',
     'SFO', 'San Francisco', 'CA',
     '08:00', '09:30', 337),
    
    (2, 'UA200', 'United Airlines',
     'SFO', 'San Francisco', 'CA',
     'JFK', 'New York', 'NY',
     '10:00', '18:30', 2586),
    
    (3, 'DL300', 'Delta Airlines',
     'JFK', 'New York', 'NY',
     'LAX', 'Los Angeles', 'CA',
     '09:00', '12:30', 2475),
    
    (4, 'AA400', 'American Airlines',
     'ORD', 'Chicago', 'IL',
     'ATL', 'Atlanta', 'GA',
     '07:00', '10:00', 606),
    
    (5, 'DL500', 'Delta Airlines',
     'ATL', 'Atlanta', 'GA',
     'LAX', 'Los Angeles', 'CA',
     '11:00', '13:30', 1946),
    
    (6, 'UA600', 'United Airlines',
     'LAX', 'Los Angeles', 'CA',
     'ORD', 'Chicago', 'IL',
     '14:00', '20:00', 1745);


