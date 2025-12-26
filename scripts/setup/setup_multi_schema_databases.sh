#!/bin/bash
# Setup databases and tables for all schemas in unified_test_multi_schema.yaml
# This script creates the complete test environment for multi-schema support

set -e  # Exit on error

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-test_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-test_pass}"

echo "=== Multi-Schema Database Setup ==="
echo "Setting up databases for 6 schemas:"
echo "  1. social_benchmark (brahmand)"
echo "  2. test_fixtures (brahmand)"
echo "  3. ldbc_snb (ldbc)"
echo "  4. denormalized_flights (travel)"
echo "  5. pattern_comp (brahmand)"
echo "  6. zeek_logs (security)"
echo ""

# Function to execute SQL
run_sql() {
    curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" \
        --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        --data-binary "$1"
}

echo "Step 1: Creating databases..."
run_sql "CREATE DATABASE IF NOT EXISTS brahmand;"
run_sql "CREATE DATABASE IF NOT EXISTS ldbc;"
run_sql "CREATE DATABASE IF NOT EXISTS travel;"
run_sql "CREATE DATABASE IF NOT EXISTS security;"
echo "✓ Databases created"

echo ""
echo "Step 2: social_benchmark schema (brahmand database)"
echo "  Creating tables: users_bench, posts_bench, user_follows_bench, post_likes_bench"

run_sql "DROP TABLE IF EXISTS brahmand.users_bench;"
run_sql "DROP TABLE IF EXISTS brahmand.posts_bench;"
run_sql "DROP TABLE IF EXISTS brahmand.user_follows_bench;"
run_sql "DROP TABLE IF EXISTS brahmand.post_likes_bench;"

run_sql "
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
"

run_sql "
CREATE TABLE brahmand.posts_bench (
    post_id UInt32,
    user_id UInt32,
    content String,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, post_id);
"

run_sql "
CREATE TABLE brahmand.user_follows_bench (
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = MergeTree()
ORDER BY (follower_id, followed_id);
"

run_sql "
CREATE TABLE brahmand.post_likes_bench (
    user_id UInt32,
    post_id UInt32,
    liked_at DateTime
) ENGINE = MergeTree()
ORDER BY (post_id, user_id);
"

# Insert sample data
run_sql "
INSERT INTO brahmand.users_bench VALUES
    (1, 'Alice Smith', 'alice@example.com', '2024-01-01', 1, 'USA', 'New York'),
    (2, 'Bob Jones', 'bob@example.com', '2024-01-05', 1, 'UK', 'London'),
    (3, 'Charlie Brown', 'charlie@example.com', '2024-01-10', 1, 'Canada', 'Toronto'),
    (4, 'Diana Prince', 'diana@example.com', '2024-01-15', 1, 'Australia', 'Sydney'),
    (5, 'Eve Wilson', 'eve@example.com', '2024-01-20', 0, 'France', 'Paris');
"

run_sql "
INSERT INTO brahmand.posts_bench VALUES
    (101, 1, 'Hello world!', '2024-02-01 10:00:00'),
    (102, 1, 'Learning Cypher', '2024-02-02 11:00:00'),
    (103, 2, 'ClickHouse rocks', '2024-02-03 12:00:00'),
    (104, 3, 'Graph databases are cool', '2024-02-04 13:00:00');
"

run_sql "
INSERT INTO brahmand.user_follows_bench VALUES
    (1, 2, '2024-01-25'),
    (1, 3, '2024-01-26'),
    (2, 3, '2024-01-27'),
    (2, 4, '2024-01-28'),
    (4, 1, '2024-01-29');
"

run_sql "
INSERT INTO brahmand.post_likes_bench VALUES
    (2, 101, '2024-02-01 11:00:00'),
    (3, 101, '2024-02-01 12:00:00'),
    (1, 103, '2024-02-03 13:00:00'),
    (4, 104, '2024-02-04 14:00:00');
"

echo "✓ social_benchmark schema ready"

echo ""
echo "Step 3: test_fixtures schema (brahmand database)"
echo "  Creating tables: users, products, groups, purchases, friendships, memberships, ratings"

run_sql "DROP TABLE IF EXISTS brahmand.users;"
run_sql "DROP TABLE IF EXISTS brahmand.products;"
run_sql "DROP TABLE IF EXISTS brahmand.groups;"
run_sql "DROP TABLE IF EXISTS brahmand.purchases;"
run_sql "DROP TABLE IF EXISTS brahmand.friendships;"
run_sql "DROP TABLE IF EXISTS brahmand.memberships;"
run_sql "DROP TABLE IF EXISTS brahmand.ratings;"

run_sql "
CREATE TABLE brahmand.users (
    user_id UInt32,
    name String,
    email String
) ENGINE = MergeTree()
ORDER BY user_id;
"

run_sql "
CREATE TABLE brahmand.products (
    product_id UInt32,
    name String,
    price Float32
) ENGINE = MergeTree()
ORDER BY product_id;
"

run_sql "
CREATE TABLE brahmand.groups (
    group_id UInt32,
    name String
) ENGINE = MergeTree()
ORDER BY group_id;
"

run_sql "
CREATE TABLE brahmand.purchases (
    user_id UInt32,
    product_id UInt32,
    purchase_date Date,
    quantity UInt32
) ENGINE = MergeTree()
ORDER BY (user_id, product_id);
"

run_sql "
CREATE TABLE brahmand.friendships (
    user_id_1 UInt32,
    user_id_2 UInt32,
    since Date
) ENGINE = MergeTree()
ORDER BY (user_id_1, user_id_2);
"

run_sql "
CREATE TABLE brahmand.memberships (
    user_id UInt32,
    group_id UInt32,
    joined_date Date,
    role String
) ENGINE = MergeTree()
ORDER BY (user_id, group_id);
"

run_sql "
CREATE TABLE brahmand.ratings (
    user_id UInt32,
    product_id UInt32,
    rating UInt8,
    review_text String
) ENGINE = MergeTree()
ORDER BY (user_id, product_id);
"

# Insert sample data
run_sql "
INSERT INTO brahmand.users VALUES
    (10, 'Test User 1', 'test1@example.com'),
    (11, 'Test User 2', 'test2@example.com'),
    (12, 'Test User 3', 'test3@example.com');
"

run_sql "
INSERT INTO brahmand.products VALUES
    (201, 'Laptop', 999.99),
    (202, 'Mouse', 29.99),
    (203, 'Keyboard', 79.99);
"

run_sql "
INSERT INTO brahmand.groups VALUES
    (301, 'Developers'),
    (302, 'Designers');
"

run_sql "
INSERT INTO brahmand.purchases VALUES
    (10, 201, '2024-03-01', 1),
    (11, 202, '2024-03-02', 2),
    (10, 203, '2024-03-03', 1);
"

run_sql "
INSERT INTO brahmand.friendships VALUES
    (10, 11, '2024-01-01'),
    (11, 12, '2024-01-02');
"

run_sql "
INSERT INTO brahmand.memberships VALUES
    (10, 301, '2024-01-01', 'admin'),
    (11, 301, '2024-01-02', 'member'),
    (12, 302, '2024-01-03', 'member');
"

run_sql "
INSERT INTO brahmand.ratings VALUES
    (10, 201, 5, 'Excellent laptop!'),
    (11, 202, 4, 'Good mouse');
"

echo "✓ test_fixtures schema ready"

echo ""
echo "Step 4: ldbc_snb schema (ldbc database)"
echo "  Creating tables: person, comment, forum, tag, person_knows_person, person_likes_comment, forum_containerof_post, forum_hasmember_person"

run_sql "DROP TABLE IF EXISTS ldbc.person;"
run_sql "DROP TABLE IF EXISTS ldbc.comment;"
run_sql "DROP TABLE IF EXISTS ldbc.forum;"
run_sql "DROP TABLE IF EXISTS ldbc.tag;"
run_sql "DROP TABLE IF EXISTS ldbc.person_knows_person;"
run_sql "DROP TABLE IF EXISTS ldbc.person_likes_comment;"
run_sql "DROP TABLE IF EXISTS ldbc.forum_containerof_post;"
run_sql "DROP TABLE IF EXISTS ldbc.forum_hasmember_person;"

run_sql "
CREATE TABLE ldbc.person (
    personId UInt64,
    firstName String,
    lastName String,
    creationDate DateTime,
    moderatorId UInt64 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY personId;
"

run_sql "
CREATE TABLE ldbc.comment (
    commentId UInt64,
    content String,
    creationDate DateTime,
    creatorId UInt64,
    replyOfCommentId UInt64 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY commentId;
"

run_sql "
CREATE TABLE ldbc.forum (
    forumId UInt64,
    title String,
    creationDate DateTime,
    moderatorId UInt64
) ENGINE = MergeTree()
ORDER BY forumId;
"

run_sql "
CREATE TABLE ldbc.tag (
    tagId UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY tagId;
"

run_sql "
CREATE TABLE ldbc.person_knows_person (
    person1Id UInt64,
    person2Id UInt64
) ENGINE = MergeTree()
ORDER BY (person1Id, person2Id);
"

run_sql "
CREATE TABLE ldbc.person_likes_comment (
    personId UInt64,
    commentId UInt64
) ENGINE = MergeTree()
ORDER BY (personId, commentId);
"

run_sql "
CREATE TABLE ldbc.forum_containerof_post (
    forumId UInt64,
    postId UInt64
) ENGINE = MergeTree()
ORDER BY (forumId, postId);
"

run_sql "
CREATE TABLE ldbc.forum_hasmember_person (
    forumId UInt64,
    personId UInt64
) ENGINE = MergeTree()
ORDER BY (forumId, personId);
"

# Insert minimal sample data
run_sql "
INSERT INTO ldbc.person VALUES
    (1001, 'John', 'Doe', '2024-01-01 00:00:00', 0),
    (1002, 'Jane', 'Smith', '2024-01-02 00:00:00', 0),
    (1003, 'Bob', 'Johnson', '2024-01-03 00:00:00', 0);
"

run_sql "
INSERT INTO ldbc.person_knows_person VALUES
    (1001, 1002),
    (1002, 1003);
"

echo "✓ ldbc_snb schema ready"

echo ""
echo "Step 5: denormalized_flights schema (travel database)"
echo "  Creating table: flights (denormalized with airport properties)"

run_sql "DROP TABLE IF EXISTS travel.flights;"

run_sql "
CREATE TABLE travel.flights (
    origin_airport String,
    dest_airport String,
    flight_date Date,
    flight_number String,
    departure_time String,
    arrival_time String
) ENGINE = MergeTree()
ORDER BY (origin_airport, dest_airport, flight_date);
"

run_sql "
INSERT INTO travel.flights VALUES
    ('JFK', 'LAX', '2024-06-01', 'UA123', '08:00', '11:30'),
    ('LAX', 'SFO', '2024-06-01', 'UA456', '12:00', '13:30'),
    ('SFO', 'SEA', '2024-06-02', 'UA789', '14:00', '16:00');
"

echo "✓ denormalized_flights schema ready"

echo ""
echo "Step 6: pattern_comp schema (brahmand database)"
echo "  Creating tables: pattern_comp_users, pattern_comp_follows"

run_sql "DROP TABLE IF EXISTS brahmand.pattern_comp_users;"
run_sql "DROP TABLE IF EXISTS brahmand.pattern_comp_follows;"

run_sql "
CREATE TABLE brahmand.pattern_comp_users (
    user_id UInt32,
    name String
) ENGINE = MergeTree()
ORDER BY user_id;
"

run_sql "
CREATE TABLE brahmand.pattern_comp_follows (
    follower_id UInt32,
    followed_id UInt32
) ENGINE = MergeTree()
ORDER BY (follower_id, followed_id);
"

run_sql "
INSERT INTO brahmand.pattern_comp_users VALUES
    (5001, 'Pattern User 1'),
    (5002, 'Pattern User 2'),
    (5003, 'Pattern User 3');
"

run_sql "
INSERT INTO brahmand.pattern_comp_follows VALUES
    (5001, 5002),
    (5002, 5003);
"

echo "✓ pattern_comp schema ready"

echo ""
echo "Step 7: zeek_logs schema (security database)"
echo "  Creating tables: ip_addresses, dns, conn"

run_sql "DROP TABLE IF EXISTS security.ip_addresses;"
run_sql "DROP TABLE IF EXISTS security.dns;"
run_sql "DROP TABLE IF EXISTS security.conn;"

run_sql "
CREATE TABLE security.ip_addresses (
    ip String
) ENGINE = MergeTree()
ORDER BY ip;
"

run_sql "
CREATE TABLE security.dns (
    id_orig_h String,
    query String,
    ts DateTime,
    qtype_name String
) ENGINE = MergeTree()
ORDER BY (id_orig_h, query);
"

run_sql "
CREATE TABLE security.conn (
    id_orig_h String,
    id_resp_h String,
    ts DateTime,
    duration Float32,
    proto String
) ENGINE = MergeTree()
ORDER BY (id_orig_h, id_resp_h);
"

run_sql "
INSERT INTO security.ip_addresses VALUES
    ('192.168.1.100'),
    ('192.168.1.101'),
    ('8.8.8.8');
"

run_sql "
INSERT INTO security.dns VALUES
    ('192.168.1.100', 'example.com', '2024-07-01 10:00:00', 'A'),
    ('192.168.1.101', 'google.com', '2024-07-01 10:01:00', 'A');
"

run_sql "
INSERT INTO security.conn VALUES
    ('192.168.1.100', '8.8.8.8', '2024-07-01 10:00:05', 0.5, 'tcp'),
    ('192.168.1.101', '8.8.8.8', '2024-07-01 10:01:05', 0.3, 'tcp');
"

echo "✓ zeek_logs schema ready"

echo ""
echo "=========================================="
echo "✓ All schemas setup complete!"
echo "=========================================="
echo ""
echo "Verification:"
echo ""

# Verify table counts
run_sql "
SELECT 
    'social_benchmark' as schema,
    'brahmand' as database,
    'users_bench' as table,
    count() as rows
FROM brahmand.users_bench
UNION ALL
SELECT 'test_fixtures', 'brahmand', 'users', count() FROM brahmand.users
UNION ALL
SELECT 'ldbc_snb', 'ldbc', 'person', count() FROM ldbc.person
UNION ALL
SELECT 'denormalized_flights', 'travel', 'flights', count() FROM travel.flights
UNION ALL
SELECT 'pattern_comp', 'brahmand', 'pattern_comp_users', count() FROM brahmand.pattern_comp_users
UNION ALL
SELECT 'zeek_logs', 'security', 'ip_addresses', count() FROM security.ip_addresses
FORMAT PrettyCompact
"

echo ""
echo "Setup complete! Start ClickGraph with:"
echo "  export GRAPH_CONFIG_PATH='./schemas/test/unified_test_multi_schema.yaml'"
echo "  cargo run --bin clickgraph"
echo ""
echo "Then test with:"
echo "  curl -s http://localhost:8080/schemas | jq"
echo "  curl -X POST http://localhost:8080/query -H 'Content-Type: application/json' \\"
echo "    -d '{\"query\":\"USE social_benchmark MATCH (u:User) RETURN u.name LIMIT 5\"}'"
