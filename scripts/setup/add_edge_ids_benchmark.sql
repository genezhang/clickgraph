-- Add edge_id columns to benchmark tables for optimal variable-length path performance
-- This migration adds single-column edge IDs to avoid tuple() overhead

-- Step 1: Recreate user_follows_bench with follow_id
DROP TABLE IF EXISTS user_follows_bench_temp;
CREATE TABLE user_follows_bench_temp (
    follow_id UInt64,
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = Memory;

-- Copy data with auto-generated follow_id (row_number)
INSERT INTO user_follows_bench_temp
SELECT 
    row_number() OVER () as follow_id,
    follower_id,
    followed_id,
    follow_date
FROM user_follows_bench;

-- Swap tables
DROP TABLE IF EXISTS user_follows_bench;
RENAME TABLE user_follows_bench_temp TO user_follows_bench;

-- Step 2: Verify posts_bench already has post_id (used as edge_id for AUTHORED)
-- No changes needed - post_id serves as edge_id

-- Step 3: Add friendship_id to friendships table (if exists)
-- Note: This table may not exist in current setup
CREATE TABLE IF NOT EXISTS friendships (
    friendship_id UInt64,
    user1_id UInt32,
    user2_id UInt32,
    since_date Date
) ENGINE = Memory;

SELECT 'Edge IDs added successfully!' as status;
SELECT 
    'user_follows_bench' as table_name,
    count(*) as row_count,
    max(follow_id) as max_edge_id
FROM user_follows_bench;
