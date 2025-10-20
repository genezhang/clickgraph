-- Setup test data for variable-length path testing
-- Creates users and friendships tables with sample data

USE social;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS friendships;
DROP TABLE IF EXISTS users;

-- Create users table
CREATE TABLE users (
    user_id UInt32,
    full_name String,
    email_address String
) ENGINE = MergeTree()
ORDER BY user_id;

-- Create friendships table
CREATE TABLE friendships (
    user1_id UInt32,
    user2_id UInt32,
    since_date Date
) ENGINE = MergeTree()
ORDER BY (user1_id, user2_id);

-- Insert test users
INSERT INTO users (user_id, full_name, email_address) VALUES
(1, 'Alice Smith', 'alice@example.com'),
(2, 'Bob Johnson', 'bob@example.com'),
(3, 'Charlie Brown', 'charlie@example.com');

-- Insert test friendships
-- This creates both 1-hop and 2-hop paths:
-- 1 -> 2 (1 hop)
-- 2 -> 3 (1 hop)
-- 1 -> 3 (1 hop direct, OR 2 hops via 2)
INSERT INTO friendships (user1_id, user2_id, since_date) VALUES
(1, 2, '2024-01-15'),
(2, 3, '2024-02-20'),
(1, 3, '2024-03-10');
