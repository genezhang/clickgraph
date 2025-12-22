-- Small-scale benchmark data for tests
-- Uses social_benchmark schema (users_bench, posts_bench, etc.)
-- Intentionally small (5-10 users) for fast, predictable tests

-- Drop existing tables if they exist
DROP TABLE IF EXISTS brahmand.users_bench;
DROP TABLE IF EXISTS brahmand.posts_bench;
DROP TABLE IF EXISTS brahmand.user_follows_bench;
DROP TABLE IF EXISTS brahmand.post_likes_bench;

-- Create users_bench table
CREATE TABLE brahmand.users_bench (
    user_id UInt32,
    full_name String,
    email_address String,
    registration_date Date,
    is_active UInt8,
    country String,
    city String
) ENGINE = MergeTree()
ORDER BY user_id;

-- Create posts_bench table
CREATE TABLE brahmand.posts_bench (
    post_id UInt32,
    author_id UInt32,
    content String,
    created_at DateTime,
    post_date Date
) ENGINE = MergeTree()
ORDER BY post_id;

-- Create user_follows_bench table
CREATE TABLE brahmand.user_follows_bench (
    follow_id UInt32,
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = MergeTree()
ORDER BY follow_id;

-- Create post_likes_bench table
CREATE TABLE brahmand.post_likes_bench (
    user_id UInt32,
    post_id UInt32,
    like_date Date
) ENGINE = MergeTree()
ORDER BY (user_id, post_id);

-- Insert test users (8 users to match test expectations)
INSERT INTO brahmand.users_bench VALUES
(1, 'Alice Johnson', 'alice@example.com', '2024-01-15', 1, 'USA', 'New York'),
(2, 'Bob Smith', 'bob@example.com', '2024-02-20', 1, 'UK', 'London'),
(3, 'Charlie Brown', 'charlie@example.com', '2024-01-10', 0, 'Canada', 'Toronto'),
(4, 'Diana Prince', 'diana@example.com', '2024-03-05', 1, 'USA', 'San Francisco'),
(5, 'Eve Adams', 'eve@example.com', '2024-02-14', 1, 'Germany', 'Berlin'),
(6, 'Frank Miller', 'frank@example.com', '2023-12-01', 1, 'Australia', 'Sydney'),
(7, 'Grace Hopper', 'grace@example.com', '2024-01-20', 1, 'USA', 'Boston'),
(8, 'Hank Pym', 'hank@example.com', '2023-11-15', 0, 'France', 'Paris');

-- Insert test posts
INSERT INTO brahmand.posts_bench VALUES
(1, 1, 'Hello world!', '2024-01-16 10:00:00', '2024-01-16'),
(2, 1, 'Cypher is awesome', '2024-01-17 15:30:00', '2024-01-17'),
(3, 2, 'Graph databases rock', '2024-02-21 09:00:00', '2024-02-21'),
(4, 3, 'Testing ClickGraph', '2024-01-11 12:00:00', '2024-01-11'),
(5, 4, 'San Francisco vibes', '2024-03-06 08:00:00', '2024-03-06');

-- Insert follow relationships
INSERT INTO brahmand.user_follows_bench VALUES
(1, 1, 2, '2024-01-20'),  -- Alice follows Bob
(2, 1, 3, '2024-01-21'),  -- Alice follows Charlie
(3, 1, 4, '2024-01-22'),  -- Alice follows Diana
(4, 2, 1, '2024-02-22'),  -- Bob follows Alice
(5, 2, 3, '2024-02-23'),  -- Bob follows Charlie
(6, 3, 1, '2024-01-12'),  -- Charlie follows Alice
(7, 4, 1, '2024-03-07'),  -- Diana follows Alice
(8, 5, 1, '2024-02-15'),  -- Eve follows Alice
(9, 5, 2, '2024-02-16'),  -- Eve follows Bob
(10, 6, 7, '2023-12-05'); -- Frank follows Grace

-- Insert post likes
INSERT INTO brahmand.post_likes_bench VALUES
(2, 1, '2024-01-16'),  -- Bob likes Alice's first post
(3, 1, '2024-01-16'),  -- Charlie likes Alice's first post
(4, 1, '2024-01-16'),  -- Diana likes Alice's first post
(2, 2, '2024-01-17'),  -- Bob likes Alice's second post
(1, 3, '2024-02-21'),  -- Alice likes Bob's post
(3, 3, '2024-02-21'),  -- Charlie likes Bob's post
(5, 1, '2024-02-15'),  -- Eve likes Alice's first post
(6, 1, '2024-01-16');  -- Frank likes Alice's first post
