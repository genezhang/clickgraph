#!/bin/bash
# Setup test data for polymorphic edge schema testing
# Creates tables and loads sample data for social_polymorphic.yaml

set -e

echo "=================================================="
echo "Setting up Polymorphic Edge Test Data"
echo "=================================================="
echo ""

# Configuration
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
DATABASE="brahmand"

# Helper function to execute ClickHouse SQL
execute_sql() {
    local sql="$1"
    local description="$2"
    
    echo "  → $description"
    
    if [ -n "$CLICKHOUSE_PASSWORD" ]; then
        curl -sS "$CLICKHOUSE_URL" \
            --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
            --data-binary "$sql"
    else
        curl -sS "$CLICKHOUSE_URL" \
            --user "$CLICKHOUSE_USER:" \
            --data-binary "$sql"
    fi
    echo ""
}

echo "Step 1: Create database"
execute_sql "CREATE DATABASE IF NOT EXISTS $DATABASE" "Create database '$DATABASE'"

echo ""
echo "Step 2: Create tables"
echo ""

# Users table
execute_sql "DROP TABLE IF EXISTS $DATABASE.users" "Drop users table if exists"
execute_sql "
CREATE TABLE $DATABASE.users (
    user_id UInt64,
    username String,
    email String,
    created_at DateTime DEFAULT now()
) ENGINE = Memory
" "Create users table"

# Posts table
execute_sql "DROP TABLE IF EXISTS $DATABASE.posts" "Drop posts table if exists"
execute_sql "
CREATE TABLE $DATABASE.posts (
    post_id UInt64,
    title String,
    body String,
    created_at DateTime DEFAULT now()
) ENGINE = Memory
" "Create posts table"

# Polymorphic interactions table
execute_sql "DROP TABLE IF EXISTS $DATABASE.interactions" "Drop interactions table if exists"
execute_sql "
CREATE TABLE $DATABASE.interactions (
    from_id UInt64,
    to_id UInt64,
    interaction_type String,
    from_type String,
    to_type String,
    timestamp DateTime DEFAULT now(),
    interaction_weight Float32 DEFAULT 1.0
) ENGINE = Memory
" "Create polymorphic interactions table"

echo ""
echo "Step 3: Load test data"
echo ""

# Insert users
execute_sql "
INSERT INTO $DATABASE.users (user_id, username, email, created_at) VALUES
(1, 'alice', 'alice@example.com', '2024-01-01 10:00:00'),
(2, 'bob', 'bob@example.com', '2024-01-02 11:00:00'),
(3, 'charlie', 'charlie@example.com', '2024-01-03 12:00:00'),
(4, 'diana', 'diana@example.com', '2024-01-04 13:00:00');
" "Insert 4 users"

# Insert posts
execute_sql "
INSERT INTO $DATABASE.posts (post_id, title, body, created_at) VALUES
(101, 'First Post', 'This is my first post!', '2024-01-05 14:00:00'),
(102, 'Second Post', 'Another day, another post', '2024-01-06 15:00:00'),
(103, 'Third Post', 'Hello world!', '2024-01-07 16:00:00');
" "Insert 3 posts"

# Insert polymorphic interactions
execute_sql "
INSERT INTO $DATABASE.interactions (from_id, to_id, interaction_type, from_type, to_type, timestamp, interaction_weight) VALUES
(1, 2, 'FOLLOWS', 'User', 'User', '2024-01-10 10:00:00', 1.0),
(1, 3, 'FOLLOWS', 'User', 'User', '2024-01-10 11:00:00', 1.0),
(2, 3, 'FOLLOWS', 'User', 'User', '2024-01-10 12:00:00', 1.0),
(3, 4, 'FOLLOWS', 'User', 'User', '2024-01-10 13:00:00', 1.0),
(1, 101, 'LIKES', 'User', 'Post', '2024-01-11 10:00:00', 1.5),
(2, 101, 'LIKES', 'User', 'Post', '2024-01-11 11:00:00', 1.5),
(1, 102, 'LIKES', 'User', 'Post', '2024-01-11 12:00:00', 1.5),
(3, 103, 'LIKES', 'User', 'Post', '2024-01-11 13:00:00', 1.5),
(1, 101, 'AUTHORED', 'User', 'Post', '2024-01-05 14:00:00', 2.0),
(2, 102, 'AUTHORED', 'User', 'Post', '2024-01-06 15:00:00', 2.0),
(3, 103, 'AUTHORED', 'User', 'Post', '2024-01-07 16:00:00', 2.0),
(2, 101, 'COMMENTED', 'User', 'Post', '2024-01-12 10:00:00', 0.5),
(3, 101, 'COMMENTED', 'User', 'Post', '2024-01-12 11:00:00', 0.5),
(4, 102, 'COMMENTED', 'User', 'Post', '2024-01-12 12:00:00', 0.5)
" "Insert 14 polymorphic interactions"

echo ""
echo "Step 4: Verify data"
echo ""

execute_sql "SELECT 'Users:' AS info, count() AS count FROM $DATABASE.users" "Count users"
execute_sql "SELECT 'Posts:' AS info, count() AS count FROM $DATABASE.posts" "Count posts"
execute_sql "SELECT 'Interactions:' AS info, count() AS count FROM $DATABASE.interactions" "Count interactions"
execute_sql "SELECT interaction_type, count() AS count FROM $DATABASE.interactions GROUP BY interaction_type ORDER BY interaction_type" "Interactions by type"

echo ""
echo "=================================================="
echo "✅ Polymorphic edge test data setup complete!"
echo "=================================================="
echo ""
echo "Tables created:"
echo "  - $DATABASE.users (4 users)"
echo "  - $DATABASE.posts (3 posts)"
echo "  - $DATABASE.interactions (14 interactions)"
echo ""
echo "Interaction types:"
echo "  - FOLLOWS (4)"
echo "  - LIKES (4)"
echo "  - AUTHORED (3)"
echo "  - COMMENTED (3)"
echo ""
echo "Ready to test with: schemas/examples/social_polymorphic.yaml"
echo ""
