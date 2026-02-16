-- ClickGraph Demo Data Initialization
-- Creates and populates demo tables for social network graph

-- Users table
CREATE TABLE IF NOT EXISTS social.users (
  user_id UInt32,
  name String,
  email String,
  created_at DateTime DEFAULT now()
) ENGINE = Memory;

-- Posts table
CREATE TABLE IF NOT EXISTS social.posts (
  post_id UInt32,
  content String,
  created_at DateTime DEFAULT now()
) ENGINE = Memory;

-- AUTHORED relationship (User -> Post)
CREATE TABLE IF NOT EXISTS social.post_authored (
  user_id UInt32,
  post_id UInt32
) ENGINE = Memory;

-- LIKED relationship (User -> Post)
CREATE TABLE IF NOT EXISTS social.post_likes (
  user_id UInt32,
  post_id UInt32,
  created_at DateTime DEFAULT now()
) ENGINE = Memory;

-- User follows User relationship
CREATE TABLE IF NOT EXISTS social.user_follows (
  follower_id UInt32,
  followed_id UInt32,
  created_at DateTime DEFAULT now()
) ENGINE = Memory;

-- Insert sample users
INSERT INTO social.users VALUES
  (1, 'Alice', 'alice@example.com', '2026-01-01 10:00:00'),
  (2, 'Bob', 'bob@example.com', '2026-01-02 11:00:00'),
  (3, 'Carol', 'carol@example.com', '2026-01-03 12:00:00'),
  (4, 'David', 'david@example.com', '2026-01-04 13:00:00'),
  (5, 'Eve', 'eve@example.com', '2026-01-05 14:00:00');

-- Insert sample posts
INSERT INTO social.posts VALUES
  (1, 'Cypher is cool', '2026-01-10 10:00:00'),
  (2, 'Neo4j browser', '2026-01-11 11:00:00'),
  (3, 'Hello World', '2026-01-12 12:00:00'),
  (4, 'ClickGraph rocks', '2026-01-13 13:00:00'),
  (5, 'Graph databases FTW', '2026-01-14 14:00:00');

-- Insert AUTHORED relationships (User -> Post)
INSERT INTO social.post_authored VALUES
  (1, 1),  -- Alice authored "Cypher is cool"
  (1, 4),  -- Alice authored "ClickGraph rocks"
  (2, 2),  -- Bob authored "Neo4j browser"
  (3, 3),  -- Carol authored "Hello World"
  (4, 5);  -- David authored "Graph databases FTW"

-- Insert LIKED relationships (User -> Post)
INSERT INTO social.post_likes VALUES
  (2, 1, '2026-01-10 15:00:00'),  -- Bob likes post 1
  (3, 1, '2026-01-10 16:00:00'),  -- Carol likes post 1
  (1, 2, '2026-01-11 12:00:00'),  -- Alice likes post 2
  (4, 3, '2026-01-12 13:00:00'),  -- David likes post 3
  (3, 4, '2026-01-13 14:00:00');  -- Carol likes post 4

-- Insert FOLLOWS relationships (User -> User)
INSERT INTO social.user_follows VALUES
  (1, 2, '2026-01-01 15:00:00'),  -- Alice follows Bob
  (1, 3, '2026-01-02 15:00:00'),  -- Alice follows Carol
  (2, 1, '2026-01-02 16:00:00'),  -- Bob follows Alice
  (3, 4, '2026-01-03 15:00:00'),  -- Carol follows David
  (4, 3, '2026-01-04 15:00:00'),  -- David follows Carol
  (5, 1, '2026-01-05 15:00:00');  -- Eve follows Alice
