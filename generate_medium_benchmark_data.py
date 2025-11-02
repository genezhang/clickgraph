#!/usr/bin/env python3
"""
Generate Medium-Scale Benchmark Data for ClickGraph
Creates 10,000 users and 50,000 follow relationships
"""
import random
import string
from datetime import datetime, timedelta

# Configuration
NUM_USERS = 10000
NUM_FOLLOWS = 50000
NUM_POSTS = 5000

def random_name(length=15):
    """Generate a random name"""
    return ''.join(random.choices(string.ascii_letters + ' ', k=length)).strip()

def random_email(name):
    """Generate email from name"""
    clean_name = name.replace(' ', '.').lower()
    return f"{clean_name}@example.com"

def random_date(start_year=2020, end_year=2024):
    """Generate random date"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime('%Y-%m-%d')

def generate_users_sql():
    """Generate INSERT statements for users"""
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Japan', 'Australia']
    cities = ['New York', 'London', 'Toronto', 'Berlin', 'Paris', 'Tokyo', 'Sydney']
    
    print("-- Insert users")
    print("INSERT INTO users_bench (user_id, full_name, email_address, registration_date, is_active, country, city) VALUES")
    
    values = []
    for user_id in range(1, NUM_USERS + 1):
        name = random_name()
        email = random_email(name)
        reg_date = random_date(2020, 2024)
        is_active = random.choice([0, 1])
        country = random.choice(countries)
        city = random.choice(cities)
        
        values.append(f"({user_id}, '{name}', '{email}', '{reg_date}', {is_active}, '{country}', '{city}')")
        
        # Print in batches of 1000 for better performance
        if user_id % 1000 == 0 or user_id == NUM_USERS:
            print(',\n'.join(values) + ';')
            if user_id < NUM_USERS:
                print("\nINSERT INTO users_bench (user_id, full_name, email_address, registration_date, is_active, country, city) VALUES")
            values = []

def generate_follows_sql():
    """Generate INSERT statements for follows"""
    print("\n-- Insert follows")
    print("INSERT INTO user_follows_bench (follower_id, followed_id, follow_date) VALUES")
    
    follows = set()
    values = []
    count = 0
    
    while len(follows) < NUM_FOLLOWS:
        follower = random.randint(1, NUM_USERS)
        followed = random.randint(1, NUM_USERS)
        
        # Don't allow self-follows or duplicates
        if follower != followed and (follower, followed) not in follows:
            follows.add((follower, followed))
            follow_date = random_date(2021, 2024)
            values.append(f"({follower}, {followed}, '{follow_date}')")
            count += 1
            
            # Print in batches of 1000
            if count % 1000 == 0 or count == NUM_FOLLOWS:
                print(',\n'.join(values) + ';')
                if count < NUM_FOLLOWS:
                    print("\nINSERT INTO user_follows_bench (follower_id, followed_id, follow_date) VALUES")
                values = []

def generate_posts_sql():
    """Generate INSERT statements for posts"""
    print("\n-- Insert posts")
    print("INSERT INTO posts_bench (post_id, author_id, content, created_at) VALUES")
    
    values = []
    for post_id in range(1, NUM_POSTS + 1):
        author_id = random.randint(1, NUM_USERS)
        content = f"Post content {post_id} - {random_name(30)}"
        created_at = random_date(2022, 2024)
        
        values.append(f"({post_id}, {author_id}, '{content}', '{created_at}')")
        
        # Print in batches of 1000
        if post_id % 1000 == 0 or post_id == NUM_POSTS:
            print(',\n'.join(values) + ';')
            if post_id < NUM_POSTS:
                print("\nINSERT INTO posts_bench (post_id, author_id, content, created_at) VALUES")
            values = []

def main():
    print("-- Medium Benchmark Data Generation")
    print(f"-- {NUM_USERS} users, {NUM_FOLLOWS} follows, {NUM_POSTS} posts")
    print(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Drop and recreate tables
    print("-- Drop existing tables")
    print("DROP TABLE IF EXISTS users_bench;")
    print("DROP TABLE IF EXISTS user_follows_bench;")
    print("DROP TABLE IF EXISTS posts_bench;")
    print()
    
    print("-- Create tables (ENGINE = Memory for Windows compatibility)")
    print("""
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
    content String,
    created_at Date
) ENGINE = Memory;
""")
    
    print()
    generate_users_sql()
    generate_posts_sql()
    generate_follows_sql()
    
    print("\n-- Verification queries")
    print("SELECT 'Users:', COUNT(*) FROM users_bench;")
    print("SELECT 'Follows:', COUNT(*) FROM user_follows_bench;")
    print("SELECT 'Posts:', COUNT(*) FROM posts_bench;")
    print("\n-- Medium benchmark data generation complete!")

if __name__ == "__main__":
    main()
