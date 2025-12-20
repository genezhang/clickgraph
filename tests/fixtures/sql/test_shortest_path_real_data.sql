-- Setup test data for shortest path testing
-- Windows-compatible: Use ENGINE = Memory

CREATE DATABASE IF NOT EXISTS social;

USE social;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS user_follows;
DROP TABLE IF EXISTS users;

-- Create users table with ENGINE = Memory
CREATE TABLE users (
    user_id UInt32,
    full_name String,
    email_address String,
    registration_date Date,
    is_active UInt8
) ENGINE = Memory;

-- Create user_follows table with ENGINE = Memory
CREATE TABLE user_follows (
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = Memory;

-- Insert test users
INSERT INTO users VALUES
    (1, 'Alice Johnson', 'alice@example.com', '2023-01-15', 1),
    (2, 'Bob Smith', 'bob@example.com', '2023-02-20', 1),
    (3, 'Carol Brown', 'carol@example.com', '2023-03-10', 1),
    (4, 'David Lee', 'david@example.com', '2023-04-05', 1),
    (5, 'Eve Martinez', 'eve@example.com', '2023-05-12', 1),
    (6, 'Frank Wilson', 'frank@example.com', '2023-06-01', 1);

-- Insert follow relationships (creates a connected graph)
-- Network structure:
--   Alice -> Bob -> Carol -> David -> Eve
--          |              |
--          +-> Carol      +-> Eve (shortcut)
--   Frank is isolated (disconnected)
INSERT INTO user_follows VALUES
    (1, 2, '2023-01-20'),
    (2, 3, '2023-02-15'),
    (3, 4, '2023-03-20'),
    (4, 5, '2023-04-10'),
    (1, 3, '2023-01-25');

-- Verify data
SELECT 'Users:' as info, count() as count FROM users;
SELECT 'Follows:' as info, count() as count FROM user_follows;
SELECT 'Alice follows:' as info, followed_id FROM user_follows WHERE follower_id = 1;
