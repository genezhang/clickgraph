#!/bin/bash
# Verify demo data for ClickGraph + Neo4j Browser demo
# Data is loaded automatically by ClickHouse on first container start via init-db.sql.
# Run this script to confirm the data is present and counts are correct.
#
# Usage: bash setup_demo_data.sh

CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-demo_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-demo_pass}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-social}"

echo "Verifying demo data..."
echo ""

# Check if ClickHouse is running
if ! curl -s "$CLICKHOUSE_URL/ping" > /dev/null 2>&1; then
    echo "❌ ClickHouse not responding at $CLICKHOUSE_URL"
    echo "Start services first: docker-compose up -d"
    exit 1
fi

query() {
  curl -s -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
    "$CLICKHOUSE_URL/?query=$1"
}

USER_COUNT=$(query "SELECT+COUNT(*)+FROM+$CLICKHOUSE_DATABASE.users")
POST_COUNT=$(query "SELECT+COUNT(*)+FROM+$CLICKHOUSE_DATABASE.posts")
FOLLOWS_COUNT=$(query "SELECT+COUNT(*)+FROM+$CLICKHOUSE_DATABASE.user_follows")
LIKES_COUNT=$(query "SELECT+COUNT(*)+FROM+$CLICKHOUSE_DATABASE.post_likes")
AUTHORED_COUNT=$(query "SELECT+COUNT(*)+FROM+$CLICKHOUSE_DATABASE.post_authored")

echo "Table row counts:"
echo "  users:        $USER_COUNT  (expected: 30)"
echo "  posts:        $POST_COUNT  (expected: 50)"
echo "  user_follows: $FOLLOWS_COUNT  (expected: 60)"
echo "  post_likes:   $LIKES_COUNT  (expected: 80)"
echo "  post_authored: $AUTHORED_COUNT  (expected: 50)"
echo ""

if [ "$USER_COUNT" = "30" ] && [ "$POST_COUNT" = "50" ]; then
    echo "✅ Demo data verified!"
else
    echo "⚠️  Unexpected counts. ClickHouse may still be loading data."
    echo "   If this is a fresh start, wait 10s and retry."
    echo "   If the volume is new, check: docker logs clickhouse-demo"
fi
