#!/bin/bash
# Setup demo data for ClickGraph + Neo4j Browser demo
# Creates small sample social network data for quick testing
#
# Usage: bash setup_demo_data.sh
# Or run manually after docker-compose is up

CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-demo_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-demo_pass}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-social}"

echo "Setting up demo data..."
echo ""

# Check if ClickHouse is running
if ! curl -s "$CLICKHOUSE_URL/ping" > /dev/null 2>&1; then
    echo "❌ ClickHouse not responding at $CLICKHOUSE_URL"
    echo "Start services first: docker-compose up -d"
    exit 1
fi

# Create tables
echo "Creating tables..."

# Users table
curl -s -X POST "$CLICKHOUSE_URL/?query=CREATE%20TABLE%20IF%20NOT%20EXISTS%20$CLICKHOUSE_DATABASE.users%20%28user_id%20UInt32%2C%20name%20String%2C%20email%20String%2C%20created_at%20DateTime%29%20ENGINE%3DMemory" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" > /dev/null

# Posts table
curl -s -X POST "$CLICKHOUSE_URL/?query=CREATE%20TABLE%20IF%20NOT%20EXISTS%20$CLICKHOUSE_DATABASE.posts%20%28post_id%20UInt32%2C%20user_id%20UInt32%2C%20content%20String%2C%20created_at%20DateTime%29%20ENGINE%3DMemory" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" > /dev/null

# User follows table
curl -s -X POST "$CLICKHOUSE_URL/?query=CREATE%20TABLE%20IF%20NOT%20EXISTS%20$CLICKHOUSE_DATABASE.user_follows%20%28follower_id%20UInt32%2C%20followed_id%20UInt32%2C%20created_at%20DateTime%29%20ENGINE%3DMemory" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" > /dev/null

# Post authored table
curl -s -X POST "$CLICKHOUSE_URL/?query=CREATE%20TABLE%20IF%20NOT%20EXISTS%20$CLICKHOUSE_DATABASE.post_authored%20%28user_id%20UInt32%2C%20post_id%20UInt32%29%20ENGINE%3DMemory" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" > /dev/null

# Post likes table
curl -s -X POST "$CLICKHOUSE_URL/?query=CREATE%20TABLE%20IF%20NOT%20EXISTS%20$CLICKHOUSE_DATABASE.post_likes%20%28user_id%20UInt32%2C%20post_id%20UInt32%2C%20created_at%20DateTime%29%20ENGINE%3DMemory" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" > /dev/null

echo "✓ Tables created"
echo ""

# Insert sample users
echo "Inserting sample data..."

curl -s -X POST "$CLICKHOUSE_URL/" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" \
  -d "INSERT INTO $CLICKHOUSE_DATABASE.users VALUES
(1, 'Alice', 'alice@example.com', '2024-01-01 10:00:00'),
(2, 'Bob', 'bob@example.com', '2024-01-02 11:00:00'),
(3, 'Carol', 'carol@example.com', '2024-01-03 12:00:00'),
(4, 'David', 'david@example.com', '2024-01-04 13:00:00'),
(5, 'Eve', 'eve@example.com', '2024-01-05 14:00:00')" > /dev/null

# Insert sample posts
curl -s -X POST "$CLICKHOUSE_URL/" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" \
  -d "INSERT INTO $CLICKHOUSE_DATABASE.posts VALUES
(1, 1, 'Hello world!', '2024-01-01 10:30:00'),
(2, 1, 'Graph queries are fun', '2024-01-01 14:00:00'),
(3, 2, 'Just started with ClickGraph', '2024-01-02 12:00:00'),
(4, 3, 'Neo4j Browser works great', '2024-01-03 15:00:00'),
(5, 4, 'Cypher is cool', '2024-01-04 13:30:00')" > /dev/null

# Insert follow relationships
curl -s -X POST "$CLICKHOUSE_URL/" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" \
  -d "INSERT INTO $CLICKHOUSE_DATABASE.user_follows VALUES
(1, 2, '2024-01-01 11:00:00'),
(1, 3, '2024-01-01 11:15:00'),
(2, 3, '2024-01-02 13:00:00'),
(2, 4, '2024-01-02 13:30:00'),
(3, 4, '2024-01-03 16:00:00'),
(3, 5, '2024-01-03 16:30:00'),
(4, 5, '2024-01-04 14:00:00'),
(5, 1, '2024-01-05 15:00:00')" > /dev/null

# Insert authored relationships
curl -s -X POST "$CLICKHOUSE_URL/" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" \
  -d "INSERT INTO $CLICKHOUSE_DATABASE.post_authored VALUES
(1, 1),
(1, 2),
(2, 3),
(3, 4),
(4, 5)" > /dev/null

# Insert likes
curl -s -X POST "$CLICKHOUSE_URL/" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" \
  -d "INSERT INTO $CLICKHOUSE_DATABASE.post_likes VALUES
(2, 1, '2024-01-01 12:00:00'),
(3, 1, '2024-01-01 13:00:00'),
(2, 2, '2024-01-01 16:00:00'),
(4, 2, '2024-01-01 17:00:00'),
(1, 3, '2024-01-02 14:00:00'),
(4, 3, '2024-01-02 15:00:00'),
(5, 4, '2024-01-03 18:00:00'),
(1, 5, '2024-01-04 15:00:00')" > /dev/null

echo "✓ Sample data inserted"
echo ""

# Verify data
echo "Verifying data..."
USER_COUNT=$(curl -s -X POST "$CLICKHOUSE_URL/?query=SELECT%20COUNT%28%29%20FROM%20$CLICKHOUSE_DATABASE.users" \
  -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" 2>/dev/null)

echo "✓ Users: $USER_COUNT"
echo ""
echo "✅ Demo data setup complete!"
echo ""
echo "Try a query in Neo4j Browser:"
echo "  MATCH (u:User) RETURN u LIMIT 5"
