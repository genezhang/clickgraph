#!/bin/bash
# Setup test data for standard schema (separate node + edge tables)
# Database: db_standard
# Schema: schemas/dev/social_standard.yaml
#
# Tables:
#   - users (node: User)
#   - posts (node: Post)
#   - user_follows (edge: FOLLOWS, User→User)
#   - post_likes (edge: LIKED, User→Post)
#   - posts also serves as denormalized edge: AUTHORED (User→Post)
#   - friendships (edge: FRIENDS_WITH, User→User, undirected)

set -e

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-test_user}"
CH_PASS="${CLICKHOUSE_PASSWORD:-test_pass}"

run_sql() {
    echo "$1" | curl -s "${CH_URL}/?user=${CH_USER}&password=${CH_PASS}" --data-binary @-
}

echo "=== Setting up db_standard ==="

run_sql "CREATE DATABASE IF NOT EXISTS db_standard"

# Users table
run_sql "CREATE TABLE IF NOT EXISTS db_standard.users (
    user_id UInt64,
    full_name String,
    email_address String,
    registration_date Date,
    is_active UInt8,
    country String,
    city String
) ENGINE = Memory"

# Posts table (also denormalized edge table for AUTHORED)
run_sql "CREATE TABLE IF NOT EXISTS db_standard.posts (
    post_id UInt64,
    user_id UInt64,
    content String,
    created_at DateTime
) ENGINE = Memory"

# FOLLOWS edge
run_sql "CREATE TABLE IF NOT EXISTS db_standard.user_follows (
    follower_id UInt64,
    followed_id UInt64,
    follow_date Date
) ENGINE = Memory"

# LIKED edge
run_sql "CREATE TABLE IF NOT EXISTS db_standard.post_likes (
    user_id UInt64,
    post_id UInt64,
    liked_at DateTime
) ENGINE = Memory"

# FRIENDS_WITH edge (undirected)
run_sql "CREATE TABLE IF NOT EXISTS db_standard.friendships (
    user_id_1 UInt64,
    user_id_2 UInt64,
    since Date
) ENGINE = Memory"

# Clear existing data
for t in users posts user_follows post_likes friendships; do
    run_sql "TRUNCATE TABLE IF EXISTS db_standard.$t"
done

echo "Tables created. Inserting data..."

run_sql "INSERT INTO db_standard.users VALUES
(1, 'Alice Smith', 'alice@example.com', '2023-01-01', 1, 'USA', 'New York'),
(2, 'Bob Jones', 'bob@example.com', '2023-02-15', 1, 'USA', 'Chicago'),
(3, 'Carol White', 'carol@example.com', '2023-03-20', 1, 'UK', 'London'),
(4, 'David Brown', 'david@example.com', '2023-04-10', 0, 'Canada', 'Toronto'),
(5, 'Eve Davis', 'eve@example.com', '2023-05-05', 1, 'Germany', 'Berlin')"

run_sql "INSERT INTO db_standard.posts VALUES
(1, 1, 'Hello world!', '2024-01-01 10:00:00'),
(2, 1, 'Rust is great', '2024-01-15 14:00:00'),
(3, 2, 'Graph databases rock', '2024-02-01 09:00:00'),
(4, 3, 'London calling', '2024-02-20 16:00:00'),
(5, 4, 'Winter in Toronto', '2024-03-01 11:00:00')"

run_sql "INSERT INTO db_standard.user_follows VALUES
(1, 2, '2023-01-15'),
(1, 3, '2023-02-01'),
(2, 1, '2023-02-20'),
(2, 3, '2023-03-01'),
(3, 1, '2023-03-15'),
(3, 4, '2023-04-01'),
(4, 5, '2023-05-01'),
(5, 1, '2023-05-10'),
(5, 2, '2023-06-01')"

run_sql "INSERT INTO db_standard.post_likes VALUES
(1, 3, '2024-02-02 10:00:00'),
(1, 4, '2024-02-21 11:00:00'),
(2, 1, '2024-01-02 12:00:00'),
(2, 4, '2024-02-22 13:00:00'),
(3, 1, '2024-01-03 14:00:00'),
(3, 2, '2024-01-16 15:00:00'),
(4, 3, '2024-02-03 16:00:00'),
(5, 2, '2024-01-17 17:00:00')"

run_sql "INSERT INTO db_standard.friendships VALUES
(1, 2, '2023-06-01'),
(1, 3, '2023-07-01'),
(2, 4, '2023-08-01'),
(3, 5, '2023-09-01')"

echo ""
echo "=== Data loaded ==="
echo "Users:       $(run_sql 'SELECT count() FROM db_standard.users')"
echo "Posts:       $(run_sql 'SELECT count() FROM db_standard.posts')"
echo "Follows:     $(run_sql 'SELECT count() FROM db_standard.user_follows')"
echo "Likes:       $(run_sql 'SELECT count() FROM db_standard.post_likes')"
echo "Friendships: $(run_sql 'SELECT count() FROM db_standard.friendships')"
echo ""
echo "Start server with:"
echo "  GRAPH_CONFIG_PATH=schemas/dev/social_standard.yaml cargo run --bin clickgraph"
