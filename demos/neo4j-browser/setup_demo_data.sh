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

# Load data from init-db.sql using docker exec
echo "Loading sample data (30 users, 50 posts, ~270 total rows)..."

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/init-db.sql"

if [ ! -f "$SQL_FILE" ]; then
  echo "Error: init-db.sql not found at $SQL_FILE"
  exit 1
fi

# Copy SQL file into container and execute
docker cp "$SQL_FILE" clickhouse-demo:/tmp/init-db.sql
docker exec clickhouse-demo bash -c "clickhouse-client --multiquery < /tmp/init-db.sql" > /dev/null 2>&1

echo "✓ Sample data loaded from init-db.sql"
echo ""

# Verify data
echo "Verifying data..."
USER_COUNT=$(docker exec clickhouse-demo clickhouse-client -q "SELECT COUNT(*) FROM $CLICKHOUSE_DATABASE.users" 2>/dev/null)
POST_COUNT=$(docker exec clickhouse-demo clickhouse-client -q "SELECT COUNT(*) FROM $CLICKHOUSE_DATABASE.posts" 2>/dev/null)

echo "✓ Loaded $USER_COUNT users, $POST_COUNT posts"
echo ""
echo "✅ Demo data setup complete!"
echo ""
echo "Try a query in Neo4j Browser:"
echo "  MATCH (u:User) RETURN u LIMIT 5"
