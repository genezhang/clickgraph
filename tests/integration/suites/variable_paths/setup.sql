-- Setup SQL for Variable-Length Paths Integration Tests
-- Database: test_vlp
-- Creates test data for variable-length path pattern testing

-- Create Users table
CREATE TABLE IF NOT EXISTS test_vlp.users (
    user_id UInt32,
    name String,
    level UInt8
) ENGINE = Memory;

-- Create Follows relationship table (for path traversal)
CREATE TABLE IF NOT EXISTS test_vlp.follows (
    follower_id UInt32,
    followed_id UInt32,
    since Date
) ENGINE = Memory;

-- Insert Users (create a longer chain for multi-hop testing)
INSERT INTO test_vlp.users VALUES
    (1, 'Alice', 1),
    (2, 'Bob', 2),
    (3, 'Charlie', 3),
    (4, 'Diana', 4),
    (5, 'Eve', 5),
    (6, 'Frank', 6);

-- Insert Follows to create paths:
-- Alice -> Bob -> Charlie -> Diana -> Eve -> Frank (5-hop chain)
-- Alice -> Charlie (shortcut)
-- Bob -> Diana (shortcut)
INSERT INTO test_vlp.follows VALUES
    (1, 2, '2022-01-01'),  -- Alice -> Bob
    (2, 3, '2022-01-02'),  -- Bob -> Charlie
    (3, 4, '2022-01-03'),  -- Charlie -> Diana
    (4, 5, '2022-01-04'),  -- Diana -> Eve
    (5, 6, '2022-01-05'),  -- Eve -> Frank
    (1, 3, '2022-02-01'),  -- Alice -> Charlie (shortcut)
    (2, 4, '2022-02-02');  -- Bob -> Diana (shortcut)
