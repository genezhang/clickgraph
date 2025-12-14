-- Setup SQL for Shortest Paths Integration Tests
-- Database: test_shortest
-- Creates test data for shortestPath() and allShortestPaths() testing

-- Create Users table
CREATE TABLE IF NOT EXISTS test_shortest.users (
    user_id UInt32,
    name String
) ENGINE = Memory;

-- Create Follows relationship table
CREATE TABLE IF NOT EXISTS test_shortest.follows (
    follower_id UInt32,
    followed_id UInt32,
    weight Float64
) ENGINE = Memory;

-- Insert Users (create multiple paths for shortest path testing)
INSERT INTO test_shortest.users VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie'),
    (4, 'Diana'),
    (5, 'Eve');

-- Insert Follows to create multiple paths with different lengths:
-- Shortest path Alice->Eve: Alice->Charlie->Diana->Eve (3 hops)
-- Longer path: Alice->Bob->Charlie->Diana->Eve (4 hops)
-- Another path: Alice->Bob->Diana->Eve (3 hops)
INSERT INTO test_shortest.follows VALUES
    -- Path 1: Alice->Bob->Charlie->Diana->Eve (4 hops)
    (1, 2, 1.0),  -- Alice -> Bob
    (2, 3, 1.0),  -- Bob -> Charlie
    (3, 4, 1.0),  -- Charlie -> Diana
    (4, 5, 1.0),  -- Diana -> Eve
    
    -- Path 2: Alice->Charlie->Diana->Eve (3 hops - shortest)
    (1, 3, 1.0),  -- Alice -> Charlie
    
    -- Path 3: Alice->Bob->Diana->Eve (3 hops - also shortest)
    (2, 4, 1.0);  -- Bob -> Diana
