#!/bin/bash
# Initialize demo data for ClickGraph demo

# Wait for ClickHouse to be ready
until clickhouse-client --quiet --query "SELECT 1" 2>/dev/null; do
  echo "Waiting for ClickHouse..."
  sleep 1
done

echo "ClickHouse is ready, creating demo tables..."

# Create tables and insert data
clickhouse-client << 'EOF'
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
  (1, 1),
  (1, 4),
  (2, 2),
  (3, 3),
  (4, 5);

-- Insert LIKED relationships (User -> Post)
INSERT INTO social.post_likes VALUES
  (2, 1, '2026-01-10 15:00:00'),
  (3, 1, '2026-01-10 16:00:00'),
  (1, 2, '2026-01-11 12:00:00'),
  (4, 3, '2026-01-12 13:00:00'),
  (3, 4, '2026-01-13 14:00:00');

-- Insert FOLLOWS relationships (User -> User)
INSERT INTO social.user_follows VALUES
  (1, 2, '2026-01-01 15:00:00'),
  (1, 3, '2026-01-02 15:00:00'),
  (2, 1, '2026-01-02 16:00:00'),
  (3, 4, '2026-01-03 15:00:00'),
  (4, 3, '2026-01-04 15:00:00'),
  (5, 1, '2026-01-05 15:00:00');

-- Verify data was loaded
SELECT 'Tables created and data inserted' as result;
SELECT COUNT(*) as user_count FROM social.users;
SELECT COUNT(*) as post_count FROM social.posts;
EOF

echo "✓ Demo data setup complete"
