-- Setup persistent test data for manual testing
-- This data persists across ClickGraph restarts

DROP TABLE IF EXISTS test_integration.users;
DROP TABLE IF EXISTS test_integration.follows;

CREATE DATABASE IF NOT EXISTS test_integration;

-- Users table with MergeTree for persistence
CREATE TABLE test_integration.users (
    user_id UInt32,
    name String,
    age UInt32
) ENGINE = MergeTree()
ORDER BY user_id;

-- Follows relationship table
CREATE TABLE test_integration.follows (
    follower_id UInt32,
    followed_id UInt32
) ENGINE = MergeTree()
ORDER BY (follower_id, followed_id);

-- Insert test data
-- Alice (1) follows Bob (2) and Charlie (3)
-- Bob (2) follows Charlie (3)
-- Diana (4) follows Alice (1)
-- Eve (5) follows Bob (2) and Diana (4)
INSERT INTO test_integration.users VALUES 
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Charlie', 35),
    (4, 'Diana', 28),
    (5, 'Eve', 32);

INSERT INTO test_integration.follows VALUES 
    (1, 2),  -- Alice follows Bob
    (1, 3),  -- Alice follows Charlie
    (2, 3),  -- Bob follows Charlie
    (4, 1),  -- Diana follows Alice
    (5, 2),  -- Eve follows Bob
    (5, 4);  -- Eve follows Diana

-- Verify data
SELECT 'Users:' as table_name, count() as count FROM test_integration.users
UNION ALL
SELECT 'Follows:', count() FROM test_integration.follows;
