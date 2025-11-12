#!/usr/bin/env python3
"""
Unified Benchmark Data Generator - All Scales
==============================================
Generates benchmark data using ClickHouse native functions for all scales.
Supports scale factors from 1 (1K users) to 5000 (5M users).

Scale Factor Guide:
- scale_factor=1:     1K users,    5K follows,    2K posts   (Small - dev testing)
- scale_factor=10:   10K users,   50K follows,    5K posts   (Medium - integration)
- scale_factor=100: 100K users,  500K follows,   50K posts   (Large - stress test)
- scale_factor=1000: 1M users,    5M follows,  500K posts   (XLarge - production)
- scale_factor=5000: 5M users,   50M follows,   25M posts   (XXLarge - enterprise)

Usage:
    python setup_benchmark_unified.py --scale 1       # Small (1K users)
    python setup_benchmark_unified.py --scale 10      # Medium (10K users)
    python setup_benchmark_unified.py --scale 100     # Large (100K users)
    python setup_benchmark_unified.py --scale 1000    # XLarge (1M users)
    python setup_benchmark_unified.py --scale 5000    # XXLarge (5M users)
    
    # Custom scale
    python setup_benchmark_unified.py --scale 50      # 50K users
"""
import argparse
import subprocess
import sys
import time

def log(msg):
    """Print with timestamp and flush"""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

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

def create_tables():
    """Create benchmark tables"""
    log("Creating tables...")
    
    create_sql = """
DROP TABLE IF EXISTS user_follows_bench;
DROP TABLE IF EXISTS posts_bench;
DROP TABLE IF EXISTS post_likes_bench;
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

CREATE TABLE user_follows_bench (
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = Memory;

CREATE TABLE posts_bench (
    post_id UInt32,
    author_id UInt32,
    post_title String,
    post_content String,
    post_date DateTime
) ENGINE = Memory;

CREATE TABLE post_likes_bench (
    user_id UInt32,
    post_id UInt32,
    like_date DateTime
) ENGINE = Memory;
"""
    return run_sql(create_sql)

def generate_users(scale_factor, chunk_size=100_000):
    """Generate users using ClickHouse native functions"""
    num_users = scale_factor * 1000
    log(f"Generating {num_users:,} users (scale_factor={scale_factor})...")
    log(f"  Target ratios: 1 user : 100 follows : 50 posts (realistic social network)")
    
    # For small datasets, generate all at once
    if num_users <= chunk_size:
        insert_sql = f"""
INSERT INTO users_bench 
SELECT 
    number AS user_id,
    concat(randomPrintableASCII(15)) AS full_name,
    concat(lower(randomPrintableASCII(10)), '@example.com') AS email_address,
    toDate('2020-01-01') + toIntervalDay(rand() % 1827) AS registration_date,
    rand() % 2 AS is_active,
    arrayElement(['USA','UK','Canada','Germany','France','Japan','Australia','Brazil','India','China'], (rand() % 10) + 1) AS country,
    arrayElement(['New York','London','Toronto','Berlin','Paris','Tokyo','Sydney','Mumbai','Beijing','Seoul'], (rand() % 10) + 1) AS city
FROM numbers({num_users});
"""
        if not run_sql(insert_sql):
            return False
        log(f"  [OK] Users complete: {num_users:,} rows")
        return True
    
    # For large datasets, generate in chunks
    for chunk_start in range(0, num_users, chunk_size):
        chunk_count = min(chunk_size, num_users - chunk_start)
        
        insert_sql = f"""
INSERT INTO users_bench 
SELECT 
    {chunk_start} + number AS user_id,
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
        
        if (chunk_start + chunk_count) % 500_000 == 0 or chunk_start + chunk_count == num_users:
            log(f"  Users: {chunk_start + chunk_count:,} / {num_users:,} ({(chunk_start + chunk_count)/num_users*100:.0f}%)")
    
    log(f"  [OK] Users complete: {num_users:,} rows")
    return True

def generate_follows(scale_factor, chunk_size=1_000_000):
    """Generate follows using ClickHouse native functions"""
    num_users = scale_factor * 1000
    num_follows = scale_factor * 100000  # 100x multiplier (realistic social network)
    log(f"Generating {num_follows:,} follows (~100 per user avg)...")
    
    # For small datasets, generate all at once
    if num_follows <= chunk_size:
        insert_sql = f"""
INSERT INTO user_follows_bench 
SELECT 
    rand() % {num_users} AS follower_id,
    rand(1) % {num_users} AS followed_id,
    toDate('2021-01-01') + toIntervalDay(rand(2) % 1461) AS follow_date
FROM numbers({num_follows})
WHERE follower_id != followed_id;
"""
        if not run_sql(insert_sql):
            return False
        log(f"  [OK] Follows complete: ~{num_follows:,} rows")
        return True
    
    # For large datasets, generate in chunks
    for chunk_start in range(0, num_follows, chunk_size):
        chunk_count = min(chunk_size, num_follows - chunk_start)
        
        insert_sql = f"""
INSERT INTO user_follows_bench 
SELECT 
    rand() % {num_users} AS follower_id,
    rand(1) % {num_users} AS followed_id,
    toDate('2021-01-01') + toIntervalDay(rand(2) % 1461) AS follow_date
FROM numbers({chunk_count})
WHERE follower_id != followed_id;
"""
        if not run_sql(insert_sql):
            return False
        
        if (chunk_start + chunk_count) % 5_000_000 == 0 or chunk_start + chunk_count == num_follows:
            log(f"  Follows: {chunk_start + chunk_count:,} / {num_follows:,} ({(chunk_start + chunk_count)/num_follows*100:.0f}%)")
    
    log(f"  [OK] Follows complete: ~{num_follows:,} rows")
    return True

def generate_posts(scale_factor, chunk_size=1_000_000):
    """Generate posts using ClickHouse native functions"""
    num_users = scale_factor * 1000
    num_posts = scale_factor * 50000  # 50x multiplier (realistic content production)
    log(f"Generating {num_posts:,} posts (~50 per user avg)...")
    
    # For small datasets, generate all at once
    if num_posts <= chunk_size:
        insert_sql = f"""
INSERT INTO posts_bench 
SELECT 
    number AS post_id,
    rand() % {num_users} AS author_id,
    arrayElement(['Tech thoughts', 'Weekend plans', 'Recipes', 'Travel', 'Books', 'Movies', 'Career', 'Fitness', 'Music'], (rand(1) % 9) + 1) AS post_title,
    concat(randomPrintableASCII(100)) AS post_content,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(rand(2) % 94608000) AS post_date
FROM numbers({num_posts});
"""
        if not run_sql(insert_sql):
            return False
        log(f"  [OK] Posts complete: {num_posts:,} rows")
        return True
    
    # For large datasets, generate in chunks
    for chunk_start in range(0, num_posts, chunk_size):
        chunk_count = min(chunk_size, num_posts - chunk_start)
        
        insert_sql = f"""
INSERT INTO posts_bench 
SELECT 
    {chunk_start} + number AS post_id,
    rand() % {num_users} AS author_id,
    arrayElement(['Tech thoughts', 'Weekend plans', 'Recipes', 'Travel', 'Books', 'Movies', 'Career', 'Fitness', 'Music'], (rand(1) % 9) + 1) AS post_title,
    concat(randomPrintableASCII(100)) AS post_content,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(rand(2) % 94608000) AS post_date
FROM numbers({chunk_count});
"""
        if not run_sql(insert_sql):
            return False
        
        if (chunk_start + chunk_count) % 5_000_000 == 0 or chunk_start + chunk_count == num_posts:
            log(f"  Posts: {chunk_start + chunk_count:,} / {num_posts:,} ({(chunk_start + chunk_count)/num_posts*100:.0f}%)")
    
    log(f"  [OK] Posts complete: {num_posts:,} rows")
    return True

def verify_data():
    """Verify data was loaded correctly"""
    log("Verifying data...")
    
    verify_sql = """
SELECT 
    'users_bench' as table_name, 
    count(*) as row_count 
FROM users_bench
UNION ALL
SELECT 
    'user_follows_bench' as table_name, 
    count(*) as row_count 
FROM user_follows_bench
UNION ALL
SELECT 
    'posts_bench' as table_name, 
    count(*) as row_count 
FROM posts_bench
FORMAT Pretty;
"""
    
    cmd = [
        "docker", "exec", "-i", "clickhouse",
        "clickhouse-client", "--database=brahmand", "--multiquery"
    ]
    proc = subprocess.run(cmd, input=verify_sql.encode('utf-8'), capture_output=True)
    if proc.returncode == 0:
        log("Data verification:")
        print(proc.stdout.decode('utf-8'))
        return True
    else:
        log(f"ERROR: Verification failed: {proc.stderr.decode('utf-8')}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Generate unified benchmark data for all scales',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scale Factor Examples:
  1     -> 1K users (Small - dev testing)
  10    -> 10K users (Medium - integration)
  100   -> 100K users (Large - stress test)
  1000  -> 1M users (XLarge - production scale)
  5000  -> 5M users (XXLarge - enterprise scale)
"""
    )
    parser.add_argument('--scale', type=int, default=1,
                       help='Scale factor (default: 1 = 1K users)')
    parser.add_argument('--chunk-size', type=int, default=100_000,
                       help='Chunk size for batch inserts (default: 100K)')
    
    args = parser.parse_args()
    
    num_users = args.scale * 1000
    num_follows = args.scale * 5000
    num_posts = args.scale * 2000
    
    log("=" * 70)
    log("Unified Benchmark Data Generator")
    log("=" * 70)
    log(f"Scale Factor: {args.scale}")
    log(f"Target Data Size:")
    log(f"  - Users:   {num_users:,}")
    log(f"  - Follows: {num_follows:,}")
    log(f"  - Posts:   {num_posts:,}")
    log("=" * 70)
    
    start_time = time.time()
    
    # Create tables
    if not create_tables():
        log("FAILED: Could not create tables")
        sys.exit(1)
    
    # Generate data
    if not generate_users(args.scale, args.chunk_size):
        log("FAILED: Could not generate users")
        sys.exit(1)
    
    if not generate_follows(args.scale, args.chunk_size):
        log("FAILED: Could not generate follows")
        sys.exit(1)
    
    if not generate_posts(args.scale, args.chunk_size):
        log("FAILED: Could not generate posts")
        sys.exit(1)
    
    # Verify
    if not verify_data():
        log("FAILED: Could not verify data")
        sys.exit(1)
    
    elapsed = time.time() - start_time
    log("=" * 70)
    log(f"SUCCESS! Data generation complete in {elapsed:.1f} seconds")
    log("=" * 70)
    
    # Print next steps
    log("")
    log("Next Steps:")
    log("1. Load schema: curl -X POST http://localhost:8080/schemas/load \\")
    log("                     -H 'Content-Type: application/json' \\")
    log("                     -d '{\"schema_file\": \"social_network_benchmark.yaml\"}'")
    log("")
    log(f"2. Run benchmark: python tests/python/test_benchmark_suite.py --scale {args.scale}")
    log("")

if __name__ == "__main__":
    main()
