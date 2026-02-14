#!/bin/bash
# Setup test data for polymorphic edge schema
# Database: db_polymorphic
# Schema: schemas/dev/social_polymorphic.yaml
#
# Tables:
#   - users (node: User)
#   - posts (node: Post)
#   - interactions (polymorphic edge table with type_column discriminator)
#
# The interactions table stores ALL edge types (FOLLOWS, LIKES, AUTHORED,
# COMMENTED, SHARED) in a single table, distinguished by interaction_type column.

set -e

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-test_user}"
CH_PASS="${CLICKHOUSE_PASSWORD:-test_pass}"

run_sql() {
    echo "$1" | curl -s "${CH_URL}/?user=${CH_USER}&password=${CH_PASS}" --data-binary @-
}

echo "=== Setting up db_polymorphic ==="

run_sql "CREATE DATABASE IF NOT EXISTS db_polymorphic"

# Users table
run_sql "CREATE TABLE IF NOT EXISTS db_polymorphic.users (
    user_id UInt64,
    full_name String,
    email_address String
) ENGINE = Memory"

# Posts table
run_sql "CREATE TABLE IF NOT EXISTS db_polymorphic.posts (
    post_id UInt64,
    content String,
    created_at DateTime
) ENGINE = Memory"

# Polymorphic interactions table
run_sql "CREATE TABLE IF NOT EXISTS db_polymorphic.interactions (
    from_id UInt64,
    to_id UInt64,
    interaction_type String,
    from_type String,
    to_type String,
    timestamp DateTime DEFAULT now(),
    interaction_weight Float32 DEFAULT 1.0
) ENGINE = Memory"

# Clear existing data
for t in users posts interactions; do
    run_sql "TRUNCATE TABLE IF EXISTS db_polymorphic.$t"
done

echo "Tables created. Inserting data..."

run_sql "INSERT INTO db_polymorphic.users VALUES
(1, 'Alice Smith', 'alice@example.com'),
(2, 'Bob Jones', 'bob@example.com'),
(3, 'Carol White', 'carol@example.com'),
(4, 'David Brown', 'david@example.com'),
(5, 'Eve Davis', 'eve@example.com')"

run_sql "INSERT INTO db_polymorphic.posts VALUES
(1, 'Hello world!', '2024-01-01 10:00:00'),
(2, 'Rust is great', '2024-01-15 14:00:00'),
(3, 'Graph databases rock', '2024-02-01 09:00:00'),
(4, 'London calling', '2024-02-20 16:00:00'),
(5, 'Winter in Toronto', '2024-03-01 11:00:00')"

# FOLLOWS: 10 edges (User->User)
run_sql "INSERT INTO db_polymorphic.interactions (from_id, to_id, interaction_type, from_type, to_type, timestamp, interaction_weight) VALUES
(1, 2, 'FOLLOWS', 'User', 'User', '2025-01-01', 1),
(2, 3, 'FOLLOWS', 'User', 'User', '2025-01-02', 1),
(3, 1, 'FOLLOWS', 'User', 'User', '2025-01-03', 1),
(3, 2, 'FOLLOWS', 'User', 'User', '2025-01-04', 1),
(4, 1, 'FOLLOWS', 'User', 'User', '2025-01-05', 1),
(5, 2, 'FOLLOWS', 'User', 'User', '2025-01-06', 1),
(1, 3, 'FOLLOWS', 'User', 'User', '2025-01-07', 1),
(4, 5, 'FOLLOWS', 'User', 'User', '2025-01-08', 1),
(5, 1, 'FOLLOWS', 'User', 'User', '2025-01-09', 1),
(2, 1, 'FOLLOWS', 'User', 'User', '2025-01-10', 1)"

# LIKES: 6 edges (User->Post)
run_sql "INSERT INTO db_polymorphic.interactions (from_id, to_id, interaction_type, from_type, to_type, timestamp, interaction_weight) VALUES
(1, 1, 'LIKES', 'User', 'Post', '2025-02-01', 0.8),
(2, 2, 'LIKES', 'User', 'Post', '2025-02-02', 0.9),
(3, 1, 'LIKES', 'User', 'Post', '2025-02-03', 0.7),
(4, 2, 'LIKES', 'User', 'Post', '2025-02-04', 0.6),
(5, 1, 'LIKES', 'User', 'Post', '2025-02-05', 0.9),
(1, 2, 'LIKES', 'User', 'Post', '2025-02-06', 0.5)"

# AUTHORED: 5 edges (User->Post)
run_sql "INSERT INTO db_polymorphic.interactions (from_id, to_id, interaction_type, from_type, to_type, timestamp, interaction_weight) VALUES
(1, 1, 'AUTHORED', 'User', 'Post', '2025-03-01', 1),
(2, 2, 'AUTHORED', 'User', 'Post', '2025-03-02', 1),
(3, 3, 'AUTHORED', 'User', 'Post', '2025-03-03', 1),
(4, 4, 'AUTHORED', 'User', 'Post', '2025-03-04', 1),
(5, 5, 'AUTHORED', 'User', 'Post', '2025-03-05', 1)"

# COMMENTED: 5 edges (User->Post)
run_sql "INSERT INTO db_polymorphic.interactions (from_id, to_id, interaction_type, from_type, to_type, timestamp, interaction_weight) VALUES
(1, 1, 'COMMENTED', 'User', 'Post', '2025-04-01', 0.3),
(2, 1, 'COMMENTED', 'User', 'Post', '2025-04-02', 0.4),
(3, 2, 'COMMENTED', 'User', 'Post', '2025-04-03', 0.5),
(4, 1, 'COMMENTED', 'User', 'Post', '2025-04-04', 0.6),
(5, 3, 'COMMENTED', 'User', 'Post', '2025-04-05', 0.7)"

# SHARED: 3 edges (User->Post)
run_sql "INSERT INTO db_polymorphic.interactions (from_id, to_id, interaction_type, from_type, to_type, timestamp, interaction_weight) VALUES
(1, 2, 'SHARED', 'User', 'Post', '2025-05-01', 0.8),
(2, 3, 'SHARED', 'User', 'Post', '2025-05-02', 0.9),
(3, 1, 'SHARED', 'User', 'Post', '2025-05-03', 0.7)"

echo ""
echo "=== Data loaded ==="
echo "Users:        $(run_sql 'SELECT count() FROM db_polymorphic.users')"
echo "Posts:        $(run_sql 'SELECT count() FROM db_polymorphic.posts')"
echo "Interactions: $(run_sql 'SELECT count() FROM db_polymorphic.interactions')"
run_sql "SELECT interaction_type, count() AS cnt FROM db_polymorphic.interactions GROUP BY interaction_type ORDER BY interaction_type FORMAT PrettyCompact"
echo ""
echo "Start server with:"
echo "  GRAPH_CONFIG_PATH=schemas/dev/social_polymorphic.yaml cargo run --bin clickgraph"
