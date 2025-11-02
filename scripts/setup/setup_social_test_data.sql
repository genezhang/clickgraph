-- Setup social network test data with proper schema
-- This matches the examples/social_network_view.yaml configuration

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS social;

USE social;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS user_follows;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS posts;
DROP TABLE IF EXISTS post_likes;

-- Create users table
CREATE TABLE users (
    user_id UInt32,
    full_name String,
    email_address String,
    registration_date Date,
    is_active UInt8
) ENGINE = MergeTree()
ORDER BY user_id;

-- Create user_follows table with correct column names
CREATE TABLE user_follows (
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = MergeTree()
ORDER BY (follower_id, followed_id);

-- Create posts table
CREATE TABLE posts (
    post_id UInt32,
    author_id UInt32,
    post_title String,
    post_content String,
    post_date DateTime
) ENGINE = MergeTree()
ORDER BY post_id;

-- Create post_likes table
CREATE TABLE post_likes (
    user_id UInt32,
    post_id UInt32,
    like_date DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, post_id);

-- Insert test users
INSERT INTO users VALUES
    (1, 'Alice Johnson', 'alice@example.com', '2023-01-15', 1),
    (2, 'Bob Smith', 'bob@example.com', '2023-02-20', 1),
    (3, 'Carol Brown', 'carol@example.com', '2023-03-10', 1),
    (4, 'David Lee', 'david@example.com', '2023-04-05', 1),
    (5, 'Eve Martinez', 'eve@example.com', '2023-05-12', 1);

-- Insert follow relationships (creates a graph)
-- Alice follows Bob, Carol, David
-- Bob follows Carol, Eve
-- Carol follows David
-- David follows Eve
-- Eve follows Alice (creates a cycle)
INSERT INTO user_follows VALUES
    (1, 2, '2023-01-20'),  -- Alice -> Bob
    (1, 3, '2023-01-25'),  -- Alice -> Carol
    (1, 4, '2023-02-01'),  -- Alice -> David
    (2, 3, '2023-02-15'),  -- Bob -> Carol
    (2, 5, '2023-03-01'),  -- Bob -> Eve
    (3, 4, '2023-03-20'),  -- Carol -> David
    (4, 5, '2023-04-10'),  -- David -> Eve
    (5, 1, '2023-05-15');  -- Eve -> Alice (cycle)

-- Insert some test posts
INSERT INTO posts VALUES
    (101, 1, 'First Post', 'Hello world from Alice!', '2023-01-16 10:00:00'),
    (102, 2, 'Bobs Thoughts', 'Sharing my experience...', '2023-02-21 14:30:00'),
    (103, 3, 'Carols Update', 'Just finished a great project!', '2023-03-11 09:15:00'),
    (104, 1, 'Alice Again', 'Another day, another post', '2023-03-15 16:45:00');

-- Insert some likes
INSERT INTO post_likes VALUES
    (2, 101, '2023-01-16 11:00:00'),  -- Bob likes Alice's post
    (3, 101, '2023-01-16 12:30:00'),  -- Carol likes Alice's post
    (1, 102, '2023-02-21 15:00:00'),  -- Alice likes Bob's post
    (2, 103, '2023-03-11 10:00:00'),  -- Bob likes Carol's post
    (3, 104, '2023-03-15 17:00:00');  -- Carol likes Alice's second post

-- Verify data
SELECT 'Users:' as table_name, count() as count FROM users
UNION ALL
SELECT 'User Follows:' as table_name, count() as count FROM user_follows
UNION ALL
SELECT 'Posts:' as table_name, count() as count FROM posts
UNION ALL
SELECT 'Post Likes:' as table_name, count() as count FROM post_likes;

-- Show follow graph
SELECT 
    u1.full_name as follower,
    u2.full_name as followed,
    uf.follow_date
FROM user_follows uf
JOIN users u1 ON uf.follower_id = u1.user_id
JOIN users u2 ON uf.followed_id = u2.user_id
ORDER BY uf.follow_date;
