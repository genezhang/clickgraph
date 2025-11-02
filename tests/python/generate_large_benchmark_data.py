#!/usr/bin/env python3
"""
Generate Large Benchmark Dataset for ClickGraph - STREAMING VERSION
====================================================================
Dataset: 5 million users, 50 million follows, 25 million posts

This script streams data batch-by-batch with unbuffered output.

Usage:
    python -u generate_large_benchmark_data.py > setup_large_benchmark_data.sql
    
Or pipe directly to ClickHouse:
    python -u generate_large_benchmark_data.py | docker exec -i clickhouse clickhouse-client --database=brahmand
"""
import random
import string
import sys
from datetime import datetime, timedelta

# Configuration - LARGE DATASET
NUM_USERS = 5_000_000      # 5 million users
NUM_FOLLOWS = 50_000_000   # 50 million follows
NUM_POSTS = 25_000_000     # 25 million posts

BATCH_SIZE = 10000  # Insert 10K rows at a time

def random_name(length=15):
    """Generate random name"""
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

def random_email(name):
    """Generate email from name"""
    return f"{name.lower()}@example.com"

def random_date(start_year=2020, end_year=2024):
    """Generate random date"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime('%Y-%m-%d')

def log_progress(msg):
    """Print progress to stderr"""
    print(msg, file=sys.stderr, flush=True)

def generate_users_sql():
    """Generate INSERT statements for users in batches - STREAMING"""
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Japan', 'Australia', 
                 'Brazil', 'India', 'China', 'South Korea', 'Mexico']
    cities = ['New York', 'London', 'Toronto', 'Berlin', 'Paris', 'Tokyo', 'Sydney',
              'São Paulo', 'Mumbai', 'Beijing', 'Seoul', 'Mexico City']
    
    print("-- Generating Users Table")
    print("DROP TABLE IF EXISTS users_bench;")
    print("""CREATE TABLE users_bench (
    user_id UInt32,
    full_name String,
    email_address String,
    registration_date Date,
    is_active UInt8,
    country String,
    city String
) ENGINE = Memory;""")
    print()
    sys.stdout.flush()
    
    log_progress(f"Starting user generation: {NUM_USERS:,} users in batches of {BATCH_SIZE:,}")
    
    # Generate in batches
    for batch_start in range(1, NUM_USERS + 1, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, NUM_USERS + 1)
        
        print(f"INSERT INTO users_bench (user_id, full_name, email_address, registration_date, is_active, country, city) VALUES")
        
        values = []
        for user_id in range(batch_start, batch_end):
            name = random_name()
            email = random_email(name)
            reg_date = random_date(2020, 2024)
            is_active = random.choice([0, 1])
            country = random.choice(countries)
            city = random.choice(cities)
            
            values.append(f"({user_id}, '{name}', '{email}', '{reg_date}', {is_active}, '{country}', '{city}')")
        
        print(',\n'.join(values) + ';')
        print()
        sys.stdout.flush()  # Force write after each batch
        
        # Progress indicator
        if batch_start % 100000 == 1:
            log_progress(f"Users: {batch_start:,} / {NUM_USERS:,} ({batch_start/NUM_USERS*100:.1f}%)")

def generate_follows_sql():
    """Generate INSERT statements for follows relationships - STREAMING"""
    print("-- Generating Follows Table")
    print("DROP TABLE IF EXISTS user_follows_bench;")
    print("""CREATE TABLE user_follows_bench (
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = Memory;""")
    print()
    sys.stdout.flush()
    
    log_progress(f"Starting follows generation: {NUM_FOLLOWS:,} relationships in batches of {BATCH_SIZE:,}")
    
    # Generate follows - allow some duplicates to avoid memory overhead
    follows_generated = 0
    batch_values = []
    
    while follows_generated < NUM_FOLLOWS:
        # Generate random follow relationship
        follower_id = random.randint(1, NUM_USERS)
        followed_id = random.randint(1, NUM_USERS)
        
        # Skip self-follows
        if follower_id == followed_id:
            continue
        
        follow_date = random_date(2021, 2024)
        batch_values.append(f"({follower_id}, {followed_id}, '{follow_date}')")
        follows_generated += 1
        
        # Write batch when ready
        if len(batch_values) >= BATCH_SIZE:
            print("INSERT INTO user_follows_bench (follower_id, followed_id, follow_date) VALUES")
            print(',\n'.join(batch_values) + ';')
            print()
            sys.stdout.flush()  # Force write after each batch
            batch_values = []
            
            # Progress indicator
            if follows_generated % 1000000 == 0:
                log_progress(f"Follows: {follows_generated:,} / {NUM_FOLLOWS:,} ({follows_generated/NUM_FOLLOWS*100:.1f}%)")
    
    # Write remaining batch
    if batch_values:
        print("INSERT INTO user_follows_bench (follower_id, followed_id, follow_date) VALUES")
        print(',\n'.join(batch_values) + ';')
        print()
        sys.stdout.flush()

def generate_posts_sql():
    """Generate INSERT statements for posts - STREAMING"""
    print("-- Generating Posts Table")
    print("DROP TABLE IF EXISTS posts_bench;")
    print("""CREATE TABLE posts_bench (
    post_id UInt32,
    author_id UInt32,
    title String,
    content String,
    post_date Date
) ENGINE = Memory;""")
    print()
    sys.stdout.flush()
    
    log_progress(f"Starting posts generation: {NUM_POSTS:,} posts in batches of {BATCH_SIZE:,}")
    
    # Generate in batches
    for batch_start in range(1, NUM_POSTS + 1, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, NUM_POSTS + 1)
        
        print("INSERT INTO posts_bench (post_id, author_id, title, content, post_date) VALUES")
        
        values = []
        for post_id in range(batch_start, batch_end):
            author_id = random.randint(1, NUM_USERS)
            title = f"Post {post_id} by User {author_id}"
            content = f"This is the content of post {post_id}"
            post_date = random_date(2022, 2024)
            
            values.append(f"({post_id}, {author_id}, '{title}', '{content}', '{post_date}')")
        
        print(',\n'.join(values) + ';')
        print()
        sys.stdout.flush()  # Force write after each batch
        
        # Progress indicator
        if batch_start % 1000000 == 1:
            log_progress(f"Posts: {batch_start:,} / {NUM_POSTS:,} ({batch_start/NUM_POSTS*100:.1f}%)")

def main():
    log_progress("=" * 80)
    log_progress("ClickGraph Large Benchmark Dataset Generator")
    log_progress(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_progress(f"Target: {NUM_USERS:,} users, {NUM_FOLLOWS:,} follows, {NUM_POSTS:,} posts")
    log_progress("=" * 80)
    
    print("-- ClickGraph Large Benchmark Dataset")
    print(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"-- Users: {NUM_USERS:,}")
    print(f"-- Follows: {NUM_FOLLOWS:,}")
    print(f"-- Posts: {NUM_POSTS:,}")
    print()
    sys.stdout.flush()
    
    # Generate all tables
    generate_users_sql()
    log_progress("✓ Users generation complete")
    
    generate_follows_sql()
    log_progress("✓ Follows generation complete")
    
    generate_posts_sql()
    log_progress("✓ Posts generation complete")
    
    # Verification queries
    print("-- Verification Queries")
    print("SELECT 'Users:' as table_name, count() as row_count FROM users_bench")
    print("UNION ALL")
    print("SELECT 'Follows:' as table_name, count() as row_count FROM user_follows_bench")
    print("UNION ALL")
    print("SELECT 'Posts:' as table_name, count() as row_count FROM posts_bench;")
    sys.stdout.flush()
    
    log_progress("=" * 80)
    log_progress(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_progress("=" * 80)

if __name__ == "__main__":
    main()
