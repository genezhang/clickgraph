#!/bin/bash
# Setup test data for multi-tenant schema (parameterized views with tenant_id)
# Database: db_multi_tenant
# Schema: schemas/dev/social_multi_tenant.yaml
#
# Tables (base):
#   - users (node: User) — with tenant_id
#   - posts (node: Post) — with tenant_id
#   - user_follows (edge: FOLLOWS, User→User) — with tenant_id
#   - post_likes (edge: LIKED, User→Post) — with tenant_id
#
# Parameterized views:
#   - users_by_tenant, posts_by_tenant, follows_by_tenant, likes_by_tenant
#
# Tenants: acme (users 1-3, posts 1-3), globex (users 4-5, posts 4-5)

set -e

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-test_user}"
CH_PASS="${CLICKHOUSE_PASSWORD:-test_pass}"

run_sql() {
    echo "$1" | curl -s "${CH_URL}/?user=${CH_USER}&password=${CH_PASS}" --data-binary @-
}

echo "=== Setting up db_multi_tenant ==="

run_sql "CREATE DATABASE IF NOT EXISTS db_multi_tenant"

# Users table (with tenant_id)
run_sql "CREATE TABLE IF NOT EXISTS db_multi_tenant.users (
    user_id UInt64,
    tenant_id String,
    full_name String,
    email_address String,
    registration_date Date,
    is_active UInt8,
    country String,
    city String
) ENGINE = Memory"

# Posts table (with tenant_id)
run_sql "CREATE TABLE IF NOT EXISTS db_multi_tenant.posts (
    post_id UInt64,
    tenant_id String,
    user_id UInt64,
    content String,
    created_at DateTime
) ENGINE = Memory"

# FOLLOWS edge (with tenant_id)
run_sql "CREATE TABLE IF NOT EXISTS db_multi_tenant.user_follows (
    tenant_id String,
    follower_id UInt64,
    followed_id UInt64,
    follow_date Date
) ENGINE = Memory"

# LIKED edge (with tenant_id)
run_sql "CREATE TABLE IF NOT EXISTS db_multi_tenant.post_likes (
    tenant_id String,
    user_id UInt64,
    post_id UInt64,
    liked_at DateTime
) ENGINE = Memory"

# Parameterized views
run_sql "CREATE VIEW IF NOT EXISTS db_multi_tenant.users_by_tenant AS
    SELECT * FROM db_multi_tenant.users WHERE tenant_id = {tenant_id:String}"

run_sql "CREATE VIEW IF NOT EXISTS db_multi_tenant.posts_by_tenant AS
    SELECT * FROM db_multi_tenant.posts WHERE tenant_id = {tenant_id:String}"

run_sql "CREATE VIEW IF NOT EXISTS db_multi_tenant.follows_by_tenant AS
    SELECT * FROM db_multi_tenant.user_follows WHERE tenant_id = {tenant_id:String}"

run_sql "CREATE VIEW IF NOT EXISTS db_multi_tenant.likes_by_tenant AS
    SELECT * FROM db_multi_tenant.post_likes WHERE tenant_id = {tenant_id:String}"

# Clear existing data
for t in users posts user_follows post_likes; do
    run_sql "TRUNCATE TABLE IF EXISTS db_multi_tenant.$t"
done

echo "Tables and views created. Inserting data..."

# Tenant: acme (users 1-3)
# Tenant: globex (users 4-5)
run_sql "INSERT INTO db_multi_tenant.users VALUES
(1, 'acme', 'Alice Smith', 'alice@acme.com', '2023-01-01', 1, 'USA', 'New York'),
(2, 'acme', 'Bob Jones', 'bob@acme.com', '2023-02-15', 1, 'USA', 'Chicago'),
(3, 'acme', 'Carol White', 'carol@acme.com', '2023-03-20', 1, 'UK', 'London'),
(4, 'globex', 'David Brown', 'david@globex.com', '2023-04-10', 1, 'Canada', 'Toronto'),
(5, 'globex', 'Eve Davis', 'eve@globex.com', '2023-05-05', 1, 'Germany', 'Berlin')"

# Posts: acme (posts 1-3 by users 1-2), globex (posts 4-5 by users 4-5)
run_sql "INSERT INTO db_multi_tenant.posts VALUES
(1, 'acme', 1, 'Hello from Acme!', '2024-01-01 10:00:00'),
(2, 'acme', 1, 'Acme rocks', '2024-01-15 14:00:00'),
(3, 'acme', 2, 'Graph at Acme', '2024-02-01 09:00:00'),
(4, 'globex', 4, 'Globex rules', '2024-02-20 16:00:00'),
(5, 'globex', 5, 'Berlin calling', '2024-03-01 11:00:00')"

# Follows within tenants
run_sql "INSERT INTO db_multi_tenant.user_follows VALUES
('acme', 1, 2, '2023-01-15'),
('acme', 1, 3, '2023-02-01'),
('acme', 2, 1, '2023-02-20'),
('acme', 2, 3, '2023-03-01'),
('acme', 3, 1, '2023-03-15'),
('globex', 4, 5, '2023-05-01'),
('globex', 5, 4, '2023-05-10')"

# Likes within tenants
run_sql "INSERT INTO db_multi_tenant.post_likes VALUES
('acme', 1, 3, '2024-02-02 10:00:00'),
('acme', 2, 1, '2024-01-02 12:00:00'),
('acme', 3, 1, '2024-01-03 14:00:00'),
('acme', 3, 2, '2024-01-16 15:00:00'),
('globex', 4, 5, '2024-03-02 10:00:00'),
('globex', 5, 4, '2024-02-21 11:00:00')"

echo ""
echo "=== Data loaded ==="
echo "Users:   $(run_sql 'SELECT count() FROM db_multi_tenant.users')"
echo "Posts:   $(run_sql 'SELECT count() FROM db_multi_tenant.posts')"
echo "Follows: $(run_sql 'SELECT count() FROM db_multi_tenant.user_follows')"
echo "Likes:   $(run_sql 'SELECT count() FROM db_multi_tenant.post_likes')"
echo ""
echo "Tenants:"
echo "  acme:   $(run_sql "SELECT count() FROM db_multi_tenant.users WHERE tenant_id='acme'") users, $(run_sql "SELECT count() FROM db_multi_tenant.posts WHERE tenant_id='acme'") posts"
echo "  globex: $(run_sql "SELECT count() FROM db_multi_tenant.users WHERE tenant_id='globex'") users, $(run_sql "SELECT count() FROM db_multi_tenant.posts WHERE tenant_id='globex'") posts"
echo ""
echo "Start server with:"
echo "  GRAPH_CONFIG_PATH=schemas/dev/social_multi_tenant.yaml cargo run --bin clickgraph"
echo ""
echo "In browser, set tenant before querying:"
echo "  CALL sys.set('tenant_id', 'acme')"
