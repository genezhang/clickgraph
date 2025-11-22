#!/usr/bin/env python3
"""Setup test data for ClickGraph zero-hop testing"""

import clickhouse_connect

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='test_user',
    password='test_pass'
)

print("Creating database and tables...")

# Create database
client.command('CREATE DATABASE IF NOT EXISTS test_integration')

# Drop existing tables
client.command('DROP TABLE IF EXISTS test_integration.users')
client.command('DROP TABLE IF EXISTS test_integration.follows')

# Create users table
client.command("""
    CREATE TABLE test_integration.users (
        user_id UInt32,
        name String,
        age UInt32
    ) ENGINE = Memory
""")

# Create follows table
client.command("""
    CREATE TABLE test_integration.follows (
        follower_id UInt32,
        followed_id UInt32,
        since String
    ) ENGINE = Memory
""")

print("Inserting test data...")

# Insert users
client.command("""
    INSERT INTO test_integration.users VALUES
        (1, 'Alice', 30),
        (2, 'Bob', 25),
        (3, 'Charlie', 35),
        (4, 'Diana', 28),
        (5, 'Eve', 32)
""")

# Insert follows relationships
# Alice → Bob → Charlie → Diana → Eve
# Alice → Charlie
# Bob → Diana
client.command("""
    INSERT INTO test_integration.follows VALUES
        (1, 2, '2023-01-01'),
        (1, 3, '2023-01-15'),
        (2, 3, '2023-02-01'),
        (3, 4, '2023-02-15'),
        (4, 5, '2023-03-01'),
        (2, 4, '2023-03-15')
""")

print("\n✅ Test data created successfully!\n")

# Verify data
users = client.query('SELECT * FROM test_integration.users ORDER BY user_id').result_rows
follows = client.query('SELECT * FROM test_integration.follows ORDER BY follower_id, followed_id').result_rows

print("Users:")
for user in users:
    print(f"  {user}")

print("\nFollows relationships:")
for follow in follows:
    print(f"  {follow[0]} → {follow[1]} (since {follow[2]})")

print("\nGraph structure:")
print("  Alice (1) → Bob (2), Charlie (3)")
print("  Bob (2) → Charlie (3), Diana (4)")
print("  Charlie (3) → Diana (4)")
print("  Diana (4) → Eve (5)")
print("\nNo cycles exist in this graph!")

client.close()
