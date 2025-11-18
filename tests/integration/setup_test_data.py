#!/usr/bin/env python3
"""Setup test database tables manually."""

from clickhouse_driver import Client

# Connect to ClickHouse
client = Client(
    host='localhost',
    port=9000,
    user='test_user',
    password='test_pass'
)

# Create database
try:
    client.execute("CREATE DATABASE IF NOT EXISTS test_integration")
    print("✓ Database created/exists")
except Exception as e:
    print(f"✗ Database creation failed: {e}")

# Create users table
try:
    client.execute("""
        CREATE TABLE IF NOT EXISTS test_integration.users (
            user_id UInt32,
            name String,
            age UInt32
        ) ENGINE = Memory
    """)
    print("✓ Users table created")
except Exception as e:
    print(f"✗ Users table creation failed: {e}")

# Create follows table
try:
    client.execute("""
        CREATE TABLE IF NOT EXISTS test_integration.follows (
            follower_id UInt32,
            followed_id UInt32,
            since String
        ) ENGINE = Memory
    """)
    print("✓ Follows table created")
except Exception as e:
    print(f"✗ Follows table creation failed: {e}")

# Insert test data - users
try:
    client.execute("""
        INSERT INTO test_integration.users VALUES
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Charlie', 35),
            (4, 'Diana', 28),
            (5, 'Eve', 32)
    """)
    print("✓ Users data inserted")
except Exception as e:
    print(f"✗ Users data insertion failed: {e}")

# Insert test data - follows (forms paths for shortest path tests)
try:
    client.execute("""
        INSERT INTO test_integration.follows VALUES
            (1, 2, '2022-01-01'),
            (1, 3, '2022-02-01'),
            (2, 3, '2022-03-01'),
            (2, 4, '2022-04-01'),
            (3, 4, '2022-05-01'),
            (4, 5, '2022-06-01')
    """)
    print("✓ Follows data inserted")
    print("\nGraph structure:")
    print("Alice(1) -> Bob(2) -> Diana(4) -> Eve(5)")
    print("     \\-> Charlie(3) -/")
    print("Bob -> Charlie")
    print("\nShortest Alice->Eve: 3 hops (Alice->Charlie->Diana->Eve or Alice->Bob->Diana->Eve)")
except Exception as e:
    print(f"✗ Follows data insertion failed: {e}")

# Verify
try:
    result = client.execute("SELECT * FROM test_integration.users")
    print(f"\n✓ Verification: {len(result)} users in database")
    result = client.execute("SELECT * FROM test_integration.follows")
    print(f"✓ Verification: {len(result)} follow relationships in database")
except Exception as e:
    print(f"✗ Verification failed: {e}")
