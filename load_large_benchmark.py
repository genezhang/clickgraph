#!/usr/bin/env python3
"""
Generate and Load Large Benchmark Dataset - INCREMENTAL VERSION
===============================================================
This version loads data in smaller chunks using ClickHouse's native generators.
Generates 5M users, 50M follows, 25M posts.
"""
import subprocess
import sys

def log(msg):
    print(msg, flush=True)

def run_sql(sql):
    """Execute SQL directly in ClickHouse"""
    cmd = [
        "docker", "exec", "-i", "clickhouse",
        "clickhouse-client", "--database=brahmand", "--multiquery"
    ]
    proc = subprocess.run(cmd, input=sql.encode('utf-8'), capture_output=True)
    if proc.returncode != 0:
        log(f"ERROR: {proc.stderr.decode('utf-8')}")
        return False
    return True

def generate_users_incremental(total_users=5_000_000, chunk_size=100_000):
    """Generate users in chunks using ClickHouse native functions"""
    log(f"Generating {total_users:,} users in chunks of {chunk_size:,}...")
    
    # Create table
    create_sql = """
DROP TABLE IF EXISTS users_bench;
CREATE TABLE users_bench (
    user_id UInt32,
    full_name String,
    email_address String,
    registration_date Date,
    is_active UInt8,
    country String,
    city String
) ENGINE = Memory;
"""
    if not run_sql(create_sql):
        return False
    
    # Generate using ClickHouse's native random functions
    for chunk_start in range(1, total_users + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_users + 1)
        chunk_count = chunk_end - chunk_start
        
        insert_sql = f"""
INSERT INTO users_bench 
SELECT 
    {chunk_start - 1} + number AS user_id,
    concat(randomPrintableASCII(15)) AS full_name,
    concat(lower(randomPrintableASCII(10)), '@example.com') AS email_address,
    toDate('2020-01-01') + toIntervalDay(rand() % 1827) AS registration_date,
    rand() % 2 AS is_active,
    arrayElement(['USA','UK','Canada','Germany','France','Japan','Australia','Brazil','India','China'], (rand() % 10) + 1) AS country,
    arrayElement(['New York','London','Toronto','Berlin','Paris','Tokyo','Sydney','Mumbai','Beijing','Seoul'], (rand() % 10) + 1) AS city
FROM numbers({chunk_count});
"""
        if not run_sql(insert_sql):
            return False
        
        if chunk_start % 500_000 == 1:
            log(f"  Users: {chunk_end:,} / {total_users:,} ({chunk_end/total_users*100:.0f}%)")
    
    log(f"[OK] Users complete: {total_users:,} rows")
    return True

def generate_follows_incremental(total_follows=50_000_000, num_users=5_000_000, chunk_size=1_000_000):
    """Generate follows in chunks"""
    log(f"Generating {total_follows:,} follows in chunks of {chunk_size:,}...")
    
    # Create table
    create_sql = """
DROP TABLE IF EXISTS user_follows_bench;
CREATE TABLE user_follows_bench (
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = Memory;
"""
    if not run_sql(create_sql):
        return False
    
    for chunk_start in range(0, total_follows, chunk_size):
        chunk_count = min(chunk_size, total_follows - chunk_start)
        
        # Use different rand seeds to get different values
        insert_sql = f"""
INSERT INTO user_follows_bench 
SELECT 
    1 + (rand() % {num_users}) AS follower_id,
    1 + (rand(1) % {num_users}) AS followed_id,
    toDate('2021-01-01') + toIntervalDay(rand(2) % 1461) AS follow_date
FROM numbers({chunk_count});
"""
        if not run_sql(insert_sql):
            return False
        
        if (chunk_start + chunk_count) % 5_000_000 == 0:
            log(f"  Follows: {chunk_start + chunk_count:,} / {total_follows:,} ({(chunk_start + chunk_count)/total_follows*100:.0f}%)")
    
    # Clean up self-follows
    cleanup_sql = "DELETE FROM user_follows_bench WHERE follower_id = followed_id;"
    run_sql(cleanup_sql)
    
    log(f"[OK] Follows complete (~{total_follows:,} rows after cleanup)")
    return True

def generate_posts_incremental(total_posts=25_000_000, num_users=5_000_000, chunk_size=1_000_000):
    """Generate posts in chunks"""
    log(f"Generating {total_posts:,} posts in chunks of {chunk_size:,}...")
    
    # Create table
    create_sql = """
DROP TABLE IF EXISTS posts_bench;
CREATE TABLE posts_bench (
    post_id UInt32,
    author_id UInt32,
    title String,
    content String,
    post_date Date
) ENGINE = Memory;
"""
    if not run_sql(create_sql):
        return False
    
    for chunk_start in range(1, total_posts + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_posts + 1)
        chunk_count = chunk_end - chunk_start
        
        insert_sql = f"""
INSERT INTO posts_bench 
SELECT 
    {chunk_start - 1} + number AS post_id,
    (rand() % {num_users}) + 1 AS author_id,
    concat('Post ', toString({chunk_start - 1} + number)) AS title,
    concat('Content for post ', toString({chunk_start - 1} + number)) AS content,
    toDate('2022-01-01') + toIntervalDay(rand() % 1096) AS post_date
FROM numbers({chunk_count});
"""
        if not run_sql(insert_sql):
            return False
        
        if chunk_start % 5_000_000 == 1 or chunk_end > total_posts:
            log(f"  Posts: {chunk_end:,} / {total_posts:,} ({chunk_end/total_posts*100:.0f}%)")
    
    log(f"[OK] Posts complete: {total_posts:,} rows")
    return True

def verify_data():
    """Verify row counts"""
    log("\nVerifying data...")
    verify_sql = """
SELECT 'Users' AS table_name, count() AS row_count FROM users_bench
UNION ALL
SELECT 'Follows', count() FROM user_follows_bench
UNION ALL
SELECT 'Posts', count() FROM posts_bench
FORMAT PrettyCompact;
"""
    cmd = [
        "docker", "exec", "-i", "clickhouse",
        "clickhouse-client", "--database=brahmand"
    ]
    proc = subprocess.run(cmd, input=verify_sql.encode('utf-8'), capture_output=True)
    print(proc.stdout.decode('utf-8'))

def main():
    log("=" * 80)
    log("ClickGraph Large Benchmark - Incremental Loader")
    log("Target: 5M users, 50M follows, 25M posts")
    log("=" * 80)
    
    if not generate_users_incremental():
        log("FAILED: Users generation")
        return 1
    
    if not generate_follows_incremental():
        log("FAILED: Follows generation")
        return 1
    
    if not generate_posts_incremental():
        log("FAILED: Posts generation")
        return 1
    
    verify_data()
    
    log("=" * 80)
    log("[OK] Large benchmark data loaded successfully!")
    log("=" * 80)
    return 0

if __name__ == "__main__":
    sys.exit(main())
