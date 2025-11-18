#!/usr/bin/env python3
"""Check if the full path chain exists."""

from clickhouse_driver import Client

client = Client(
    host='localhost',
    port=9000,
    user='test_user',
    password='test_pass'
)

# Check all edges
print("All follow relationships:")
rows = client.execute("SELECT follower_id, followed_id FROM test_integration.follows ORDER BY follower_id, followed_id")
for row in rows:
    print(f"  {row[0]} -> {row[1]}")

print("\nUsers:")
rows = client.execute("SELECT user_id, name FROM test_integration.users ORDER BY user_id")
user_map = {}
for row in rows:
    print(f"  {row[0]}: {row[1]}")
    user_map[row[0]] = row[1]

print("\nTranslated edges:")
rows = client.execute("SELECT follower_id, followed_id FROM test_integration.follows ORDER BY follower_id, followed_id")
for row in rows:
    from_name = user_map.get(row[0], f"Unknown({row[0]})")
    to_name = user_map.get(row[1], f"Unknown({row[1]})")
    print(f"  {from_name} -> {to_name}")
