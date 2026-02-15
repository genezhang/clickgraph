#!/usr/bin/env python3
"""
Direct-Connect Benchmark Data Generator - All Scales
=====================================================
Generates benchmark data using ClickHouse Python driver (no Docker required).
Data is persisted to $HOME/clickhouse_social for repeatable benchmark runs.

Supports scale factors from 1 (1K users) to 5000 (5M users).

Scale Factor Guide:
- scale_factor=1:     1K users,    5K follows,    2K posts   (Small - dev testing)
- scale_factor=10:   10K users,   50K follows,    5K posts   (Medium - integration)
- scale_factor=100: 100K users,  500K follows,   50K posts   (Large - stress test)
- scale_factor=1000: 1M users,    5M follows,  500K posts   (XLarge - production)
- scale_factor=5000: 5M users,   50M follows,   25M posts   (XXLarge - enterprise)

Tables Generated (5):
1. users_bench - User nodes
2. user_follows_bench - FOLLOWS relationships (User → User)
3. posts_bench - Post nodes
4. authored_bench - AUTHORED relationships (User → Post) with authored_date
5. post_likes_bench - LIKED relationships (User → Post) with like_date

Usage:
    python setup_unified_direct.py --scale 1       # Small (1K users)
    python setup_unified_direct.py --scale 10      # Medium (10K users)
    python setup_unified_direct.py --scale 100     # Large (100K users)
    python setup_unified_direct.py --scale 1000    # XLarge (1M users)
    python setup_unified_direct.py --scale 5000    # XXLarge (5M users)
    
    # With options
    python setup_unified_direct.py --scale 10 --engine MergeTree
    python setup_unified_direct.py --scale 50 --no-drop     # Preserve existing data
"""
import argparse
import sys
import time
import os
from clickhouse_driver import Client

def log(msg):
    """Print with timestamp and flush"""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

class BenchmarkDataGenerator:
    def __init__(self, host='localhost', port=18123, user='test_user', password='test_pass', database='social'):
        """Initialize ClickHouse connection"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.client = None
        self.connect()
    
    def connect(self):
        """Establish connection to ClickHouse"""
        try:
            log(f"Connecting to ClickHouse at {self.host}:{self.port}...")
            # Connect without database first (database may not exist yet)
            self.client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                settings={'max_insert_threads': 4}
            )
            # Test connection
            result = self.client.execute('SELECT 1')
            log("✓ Connected to ClickHouse")
        except Exception as e:
            log(f"✗ Failed to connect to ClickHouse: {e}")
            log("Make sure ClickHouse server is running and accessible at:")
            log(f"  Host: {self.host}")
            log(f"  Port: {self.port}")
            log("\nStart ClickHouse with:")
            log("  clickhouse-server --config-file=/etc/clickhouse-server/config.xml")
            sys.exit(1)
    
    def create_database(self):
        """Create database if it doesn't exist"""
        try:
            log(f"Ensuring database '{self.database}' exists...")
            self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            log(f"✓ Database '{self.database}' ready")
        except Exception as e:
            log(f"✗ Failed to create database: {e}")
            sys.exit(1)
    
    def run_sql(self, sql):
        """Execute SQL statement"""
        try:
            self.client.execute(sql)
            return True
        except Exception as e:
            log(f"ERROR: {e}")
            return False
    
    def execute_with_database(self, sql):
        """Execute SQL with database context"""
        try:
            self.client.execute(f"USE {self.database}; {sql}")
            return True
        except Exception as e:
            log(f"ERROR: {e}")
            return False
    
    def create_tables(self, engine='MergeTree', drop_existing=True):
        """Create benchmark tables with specified engine"""
        log(f"Creating tables with ENGINE = {engine}...")
        
        if drop_existing:
            log("Dropping existing tables...")
            # Drop tables one by one (driver doesn't support multi-statements)
            drop_statements = [
                f"DROP TABLE IF EXISTS {self.database}.post_likes_bench",
                f"DROP TABLE IF EXISTS {self.database}.authored_bench",
                f"DROP TABLE IF EXISTS {self.database}.posts_bench",
                f"DROP TABLE IF EXISTS {self.database}.user_follows_bench",
                f"DROP TABLE IF EXISTS {self.database}.users_bench",
            ]
            for stmt in drop_statements:
                if not self.run_sql(stmt):
                    return False
        
        # Create tables one by one
        create_statements = [
            f"""CREATE TABLE IF NOT EXISTS {self.database}.users_bench (
    user_id UInt32,
    full_name String,
    email_address String,
    registration_date Date,
    is_active UInt8,
    country String,
    city String
) ENGINE = {engine}
ORDER BY user_id""",
            f"""CREATE TABLE IF NOT EXISTS {self.database}.user_follows_bench (
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = {engine}
ORDER BY (follower_id, followed_id)""",
            f"""CREATE TABLE IF NOT EXISTS {self.database}.posts_bench (
    post_id UInt32,
    author_id UInt32,
    post_title String,
    post_content String,
    post_date DateTime
) ENGINE = {engine}
ORDER BY (author_id, post_id)""",
            f"""CREATE TABLE IF NOT EXISTS {self.database}.authored_bench (
    user_id UInt32,
    post_id UInt32,
    authored_date DateTime
) ENGINE = {engine}
ORDER BY (user_id, post_id)""",
            f"""CREATE TABLE IF NOT EXISTS {self.database}.post_likes_bench (
    user_id UInt32,
    post_id UInt32,
    like_date DateTime
) ENGINE = {engine}
ORDER BY (post_id, user_id)""",
        ]
        
        for stmt in create_statements:
            if not self.run_sql(stmt):
                return False
        
        log("✓ Tables created")
        return True
    
    def truncate_tables(self):
        """Clear existing data from benchmark tables"""
        log("Clearing existing benchmark data...")
        truncate_statements = [
            f"TRUNCATE TABLE {self.database}.post_likes_bench",
            f"TRUNCATE TABLE {self.database}.authored_bench",
            f"TRUNCATE TABLE {self.database}.posts_bench",
            f"TRUNCATE TABLE {self.database}.user_follows_bench",
            f"TRUNCATE TABLE {self.database}.users_bench",
        ]
        for stmt in truncate_statements:
            if not self.run_sql(stmt):
                return False
        return True
    
    def generate_users(self, scale_factor, chunk_size=100_000):
        """Generate users using ClickHouse native functions"""
        num_users = scale_factor * 1000
        log(f"Generating {num_users:,} users (scale_factor={scale_factor})...")
        
        # For small datasets, generate all at once
        if num_users <= chunk_size:
            insert_sql = f"""
INSERT INTO {self.database}.users_bench 
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
            if not self.run_sql(insert_sql):
                return False
            log(f"  ✓ Users complete: {num_users:,} rows")
            return True
        
        # For large datasets, generate in chunks
        for chunk_start in range(0, num_users, chunk_size):
            chunk_count = min(chunk_size, num_users - chunk_start)
            
            insert_sql = f"""
INSERT INTO {self.database}.users_bench 
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
            if not self.run_sql(insert_sql):
                return False
            
            if (chunk_start + chunk_count) % 500_000 == 0 or chunk_start + chunk_count == num_users:
                log(f"  Users: {chunk_start + chunk_count:,} / {num_users:,} ({(chunk_start + chunk_count)/num_users*100:.0f}%)")
        
        log(f"  ✓ Users complete: {num_users:,} rows")
        return True
    
    def generate_follows(self, scale_factor, chunk_size=1_000_000):
        """Generate follows using ClickHouse native functions"""
        num_users = scale_factor * 1000
        num_follows = scale_factor * 100000  # 100x multiplier (realistic social network)
        log(f"Generating {num_follows:,} follows (~100 per user avg)...")
        
        # For small datasets, generate all at once
        if num_follows <= chunk_size:
            insert_sql = f"""
INSERT INTO {self.database}.user_follows_bench 
SELECT 
    rand() % {num_users} AS follower_id,
    rand(1) % {num_users} AS followed_id,
    toDate('2021-01-01') + toIntervalDay(rand(2) % 1461) AS follow_date
FROM numbers({num_follows})
WHERE follower_id != followed_id;
"""
            if not self.run_sql(insert_sql):
                return False
            log(f"  ✓ Follows complete: ~{num_follows:,} rows")
            return True
        
        # For large datasets, generate in chunks
        for chunk_start in range(0, num_follows, chunk_size):
            chunk_count = min(chunk_size, num_follows - chunk_start)
            
            insert_sql = f"""
INSERT INTO {self.database}.user_follows_bench 
SELECT 
    rand() % {num_users} AS follower_id,
    rand(1) % {num_users} AS followed_id,
    toDate('2021-01-01') + toIntervalDay(rand(2) % 1461) AS follow_date
FROM numbers({chunk_count})
WHERE follower_id != followed_id;
"""
            if not self.run_sql(insert_sql):
                return False
            
            if (chunk_start + chunk_count) % 5_000_000 == 0 or chunk_start + chunk_count == num_follows:
                log(f"  Follows: {chunk_start + chunk_count:,} / {num_follows:,} ({(chunk_start + chunk_count)/num_follows*100:.0f}%)")
        
        log(f"  ✓ Follows complete: ~{num_follows:,} rows")
        return True
    
    def generate_posts(self, scale_factor, chunk_size=1_000_000):
        """Generate posts using ClickHouse native functions"""
        num_users = scale_factor * 1000
        num_posts = scale_factor * 20000  # 20x multiplier (~6 months activity)
        log(f"Generating {num_posts:,} posts (~20 per user avg)...")
        
        # For small datasets, generate all at once
        if num_posts <= chunk_size:
            insert_sql = f"""
INSERT INTO {self.database}.posts_bench 
SELECT 
    number AS post_id,
    rand() % {num_users} AS author_id,
    arrayElement(['Tech thoughts', 'Weekend plans', 'Recipes', 'Travel', 'Books', 'Movies', 'Career', 'Fitness', 'Music'], (rand(1) % 9) + 1) AS post_title,
    concat(randomPrintableASCII(100)) AS post_content,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(rand(2) % 94608000) AS post_date
FROM numbers({num_posts});
"""
            if not self.run_sql(insert_sql):
                return False
            log(f"  ✓ Posts complete: {num_posts:,} rows")
            return True
        
        # For large datasets, generate in chunks
        for chunk_start in range(0, num_posts, chunk_size):
            chunk_count = min(chunk_size, num_posts - chunk_start)
            
            insert_sql = f"""
INSERT INTO {self.database}.posts_bench 
SELECT 
    {chunk_start} + number AS post_id,
    rand() % {num_users} AS author_id,
    arrayElement(['Tech thoughts', 'Weekend plans', 'Recipes', 'Travel', 'Books', 'Movies', 'Career', 'Fitness', 'Music'], (rand(1) % 9) + 1) AS post_title,
    concat(randomPrintableASCII(100)) AS post_content,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(rand(2) % 94608000) AS post_date
FROM numbers({chunk_count});
"""
            if not self.run_sql(insert_sql):
                return False
            
            if (chunk_start + chunk_count) % 5_000_000 == 0 or chunk_start + chunk_count == num_posts:
                log(f"  Posts: {chunk_start + chunk_count:,} / {num_posts:,} ({(chunk_start + chunk_count)/num_posts*100:.0f}%)")
        
        log(f"  ✓ Posts complete: {num_posts:,} rows")
        return True
    
    def generate_authored(self, chunk_size=1_000_000):
        """Generate AUTHORED relationships from posts_bench"""
        log("Generating AUTHORED edges from posts...")
        
        # Get total posts count first
        query = f"SELECT COUNT(*) FROM {self.database}.posts_bench"
        total_posts = self.client.execute(query)[0][0]
        
        # Generate AUTHORED edges in chunks by inserting from posts_bench
        for chunk_start in range(0, total_posts, chunk_size):
            insert_sql = f"""
INSERT INTO {self.database}.authored_bench
SELECT 
    author_id AS user_id,
    post_id,
    post_date AS authored_date
FROM {self.database}.posts_bench
LIMIT {chunk_size} OFFSET {chunk_start}
"""
            if not self.run_sql(insert_sql):
                return False
            
            chunk_end = min(chunk_start + chunk_size, total_posts)
            if chunk_end % 5_000_000 == 0 or chunk_end == total_posts:
                log(f"  AUTHORED: {chunk_end:,} / {total_posts:,} ({chunk_end/total_posts*100:.0f}%)")
        
        log(f"  ✓ AUTHORED complete: {total_posts:,} edges")
        return True
    
    def generate_liked(self, scale_factor, chunk_size=1_000_000):
        """Generate LIKED relationships from random user-post pairs"""
        num_users = scale_factor * 1000
        num_posts = scale_factor * 20000
        num_likes = scale_factor * 50000  # ~10 likes per post on average
        log(f"Generating {num_likes:,} LIKED edges (~10 per post avg)...")
        
        # For small datasets, generate all at once
        if num_likes <= chunk_size:
            insert_sql = f"""
INSERT INTO {self.database}.post_likes_bench
SELECT 
    rand() % {num_users} AS user_id,
    rand(1) % {num_posts} AS post_id,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(rand(2) % 94608000) AS like_date
FROM numbers({num_likes});
"""
            if not self.run_sql(insert_sql):
                return False
            log(f"  ✓ LIKED complete: ~{num_likes:,} edges")
            return True
        
        # For large datasets, generate in chunks
        for chunk_start in range(0, num_likes, chunk_size):
            chunk_count = min(chunk_size, num_likes - chunk_start)
            
            insert_sql = f"""
INSERT INTO {self.database}.post_likes_bench
SELECT 
    rand() % {num_users} AS user_id,
    rand(1) % {num_posts} AS post_id,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(rand(2) % 94608000) AS like_date
FROM numbers({chunk_count});
"""
            if not self.run_sql(insert_sql):
                return False
            
            if (chunk_start + chunk_count) % 5_000_000 == 0 or chunk_start + chunk_count == num_likes:
                log(f"  LIKED: {chunk_start + chunk_count:,} / {num_likes:,} ({(chunk_start + chunk_count)/num_likes*100:.0f}%)")
        
        log(f"  ✓ LIKED complete: ~{num_likes:,} edges")
        return True
    
    def verify_data(self):
        """Verify data was loaded correctly"""
        log("Verifying data...")
        
        verify_sql = f"""
SELECT 
    'users_bench' as table_name, 
    count() as row_count 
FROM {self.database}.users_bench
UNION ALL
SELECT 
    'user_follows_bench' as table_name, 
    count() as row_count 
FROM {self.database}.user_follows_bench
UNION ALL
SELECT 
    'posts_bench' as table_name, 
    count() as row_count 
FROM {self.database}.posts_bench
UNION ALL
SELECT 
    'authored_bench' as table_name, 
    count() as row_count 
FROM {self.database}.authored_bench
UNION ALL
SELECT 
    'post_likes_bench' as table_name, 
    count() as row_count 
FROM {self.database}.post_likes_bench
"""
        
        try:
            results = self.client.execute(verify_sql)
            log("Data verification:")
            for row in results:
                log(f"  {row[0]:25} {row[1]:>15,} rows")
            return True
        except Exception as e:
            log(f"ERROR: Verification failed: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(
        description='Generate unified benchmark data for all scales (direct ClickHouse connection)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scale Factor Examples:
  1     -> 1K users (Small - dev testing)
  10    -> 10K users (Medium - integration)
  100   -> 100K users (Large - stress test)
  1000  -> 1M users (XLarge - production scale)
  5000  -> 5M users (XXLarge - enterprise scale)

Connection:
  Uses environment variables:
    CLICKHOUSE_HOST (default: localhost)
    CLICKHOUSE_PORT (default: 18123 - benchmark port)
    CLICKHOUSE_USER (default: test_user)
    CLICKHOUSE_PASSWORD (default: test_pass)
    CLICKHOUSE_DATABASE (default: social)
  
  Or use command-line options: --host, --port, --user, --password, --database
"""
    )
    
    # Connection arguments
    parser.add_argument('--host', default=os.getenv('CLICKHOUSE_HOST', 'localhost'),
                       help='ClickHouse server host (default: localhost)')
    parser.add_argument('--port', type=int, default=int(os.getenv('CLICKHOUSE_PORT', '18123')),
                       help='ClickHouse HTTP port (default: 18123 for benchmark)')
    parser.add_argument('--user', default=os.getenv('CLICKHOUSE_USER', 'test_user'),
                       help='ClickHouse user (default: test_user)')
    parser.add_argument('--password', default=os.getenv('CLICKHOUSE_PASSWORD', 'test_pass'),
                       help='ClickHouse password (default: test_pass)')
    parser.add_argument('--database', default=os.getenv('CLICKHOUSE_DATABASE', 'social'),
                       help='ClickHouse database (default: social)')
    
    # Data generation arguments
    parser.add_argument('--scale', type=int, default=1,
                       help='Scale factor (default: 1 = 1K users)')
    parser.add_argument('--engine', type=str, default='MergeTree',
                       choices=['Memory', 'MergeTree'],
                       help='Table engine: Memory (fast, non-persistent) or MergeTree (persistent)')
    parser.add_argument('--chunk-size', type=int, default=100_000,
                       help='Chunk size for batch inserts (default: 100K)')
    parser.add_argument('--no-drop', action='store_true',
                       help='Do not drop existing tables (preserve data)')
    
    args = parser.parse_args()
    
    num_users = args.scale * 1000
    num_follows = args.scale * 100000
    num_posts = args.scale * 20000
    
    log("=" * 70)
    log("Unified Benchmark Data Generator (Direct Connection)")
    log("=" * 70)
    log(f"Connection: {args.host}:{args.port} (user: {args.user}, db: {args.database})")
    log(f"Scale Factor: {args.scale}")
    log(f"Table Engine: {args.engine}")
    log(f"Target Data Size:")
    log(f"  - Users:   {num_users:,}")
    log(f"  - Follows: {num_follows:,}")
    log(f"  - Posts:   {num_posts:,}")
    log("=" * 70)
    
    start_time = time.time()
    
    # Initialize generator
    generator = BenchmarkDataGenerator(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database
    )
    
    # Create database
    generator.create_database()
    
    # Create tables
    drop_existing = not args.no_drop
    if not generator.create_tables(args.engine, drop_existing=drop_existing):
        log("FAILED: Could not create tables")
        sys.exit(1)
    
    # If preserving data, skip generation if tables have content
    if args.no_drop:
        try:
            count = generator.client.execute("SELECT count() FROM users_bench")[0][0]
            if count > 0:
                log(f"✓ Tables already contain data ({count:,} users). Skipping generation.")
                log("  (Use without --no-drop to regenerate)")
                elapsed = time.time() - start_time
                log("=" * 70)
                log(f"Already complete in {elapsed:.1f} seconds")
                log("=" * 70)
                generator.verify_data()
                return
        except:
            pass  # Tables might not exist yet
    
    # Generate data
    if not generator.generate_users(args.scale, args.chunk_size):
        log("FAILED: Could not generate users")
        sys.exit(1)
    
    if not generator.generate_follows(args.scale, args.chunk_size):
        log("FAILED: Could not generate follows")
        sys.exit(1)
    
    if not generator.generate_posts(args.scale, args.chunk_size):
        log("FAILED: Could not generate posts")
        sys.exit(1)
    
    if not generator.generate_authored(args.chunk_size):
        log("FAILED: Could not generate AUTHORED edges")
        sys.exit(1)
    
    if not generator.generate_liked(args.scale, args.chunk_size):
        log("FAILED: Could not generate LIKED edges")
        sys.exit(1)
    
    # Verify
    if not generator.verify_data():
        log("FAILED: Could not verify data")
        sys.exit(1)
    
    elapsed = time.time() - start_time
    log("=" * 70)
    log(f"SUCCESS! Data generation complete in {elapsed:.1f} seconds")
    log("=" * 70)
    
    # Print next steps
    log("")
    log("Next Steps:")
    log("1. Load schema into ClickGraph:")
    log("   curl -X POST http://localhost:8080/schemas/load \\")
    log("        -H 'Content-Type: application/json' \\")
    log("        -d '{\"schema_file\": \"social_benchmark.yaml\"}'")
    log("")
    log(f"2. Run benchmark queries:")
    log(f"   python benchmarks/social_network/queries/suite.py --scale {args.scale}")
    log("")

if __name__ == "__main__":
    main()
