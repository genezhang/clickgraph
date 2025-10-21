-- Test setup for multiple relationship types
-- Create tables with Memory engine for Windows compatibility

CREATE DATABASE IF NOT EXISTS test_multi_rel;
USE test_multi_rel;

-- Drop existing tables
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS follows;
DROP TABLE IF EXISTS friends;

-- Create users table
CREATE TABLE users (
    user_id UInt32,
    name String
) ENGINE = Memory;

-- Create follows relationship table
CREATE TABLE follows (
    from_user_id UInt32,
    to_user_id UInt32
) ENGINE = Memory;

-- Create friends relationship table
CREATE TABLE friends (
    from_user_id UInt32,
    to_user_id UInt32,
    friendship_date Date
) ENGINE = Memory;

-- Insert test data
INSERT INTO users VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie'),
    (4, 'Diana');

-- Alice follows Bob and Charlie
INSERT INTO follows VALUES
    (1, 2),
    (1, 3);

-- Alice is friends with Bob
INSERT INTO friends VALUES
    (1, 2, '2023-01-01');

-- Bob is friends with Charlie
INSERT INTO friends VALUES
    (2, 3, '2023-02-01');