-- Setup SQL for Optional Match Integration Tests
-- Database: test_optional_match
-- Creates test data for OPTIONAL MATCH LEFT JOIN semantics

-- Create Users table
CREATE TABLE IF NOT EXISTS test_optional_match.users (
    user_id UInt32,
    name String,
    age UInt32,
    city String
) ENGINE = Memory;

-- Create Follows relationship table
CREATE TABLE IF NOT EXISTS test_optional_match.follows (
    follower_id UInt32,
    followed_id UInt32,
    since Date
) ENGINE = Memory;

-- Create Posts table
CREATE TABLE IF NOT EXISTS test_optional_match.posts (
    post_id UInt32,
    author_id UInt32,
    content String,
    created_at DateTime
) ENGINE = Memory;

-- Insert Users (some without follows/posts for OPTIONAL MATCH testing)
INSERT INTO test_optional_match.users VALUES
    (1, 'Alice', 30, 'NYC'),
    (2, 'Bob', 25, 'London'),
    (3, 'Charlie', 35, 'Berlin'),
    (4, 'Diana', 28, 'Paris'),
    (5, 'Eve', 32, 'Tokyo');

-- Insert Follows (Alice and Bob follow people, Charlie/Diana/Eve don't)
INSERT INTO test_optional_match.follows VALUES
    (1, 2, '2022-01-01'),  -- Alice -> Bob
    (1, 3, '2022-02-01'),  -- Alice -> Charlie
    (2, 4, '2022-03-01');  -- Bob -> Diana

-- Insert Posts (Alice and Bob have posts, others don't)
INSERT INTO test_optional_match.posts VALUES
    (101, 1, 'Alice post 1', '2023-01-01 10:00:00'),
    (102, 1, 'Alice post 2', '2023-01-02 10:00:00'),
    (103, 2, 'Bob post 1', '2023-01-03 10:00:00');
