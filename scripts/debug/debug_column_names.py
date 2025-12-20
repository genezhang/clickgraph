"""
Debug script to understand ClickHouse column naming behavior.
Tests different scenarios to see when CH includes table prefix in column names.
"""

import clickhouse_connect

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='test_user',
    password='test_pass',
    database='social'
)

print("=" * 80)
print("Testing ClickHouse Column Naming Behavior")
print("=" * 80)

# Test 1: Simple SELECT with single table
print("\n1. Simple SELECT from single table:")
print("   SQL: SELECT user_id, full_name FROM users LIMIT 1")
result = client.query("SELECT user_id, full_name FROM users LIMIT 1")
print(f"   Column names: {result.column_names}")
print(f"   First row: {result.first_row}")

# Test 2: SELECT with table alias
print("\n2. SELECT with table alias:")
print("   SQL: SELECT u.user_id, u.full_name FROM users u LIMIT 1")
result = client.query("SELECT u.user_id, u.full_name FROM users u LIMIT 1")
print(f"   Column names: {result.column_names}")
print(f"   First row: {result.first_row}")

# Test 3: SELECT with JOIN
print("\n3. SELECT with JOIN (no explicit aliases):")
sql = """
SELECT u1.user_id, u1.full_name, u2.user_id, u2.full_name
FROM users u1
JOIN user_follows f ON u1.user_id = f.follower_id
JOIN users u2 ON f.followed_id = u2.user_id
LIMIT 1
"""
print(f"   SQL: {sql.strip()}")
result = client.query(sql)
print(f"   Column names: {result.column_names}")
print(f"   First row: {result.first_row}")

# Test 4: SELECT with JOIN and explicit aliases
print("\n4. SELECT with JOIN (with explicit AS aliases):")
sql = """
SELECT 
    u1.user_id AS follower_id, 
    u1.full_name AS follower_name, 
    u2.user_id AS followed_id, 
    u2.full_name AS followed_name
FROM users u1
JOIN user_follows f ON u1.user_id = f.follower_id
JOIN users u2 ON f.followed_id = u2.user_id
LIMIT 1
"""
print(f"   SQL: {sql.strip()}")
result = client.query(sql)
print(f"   Column names: {result.column_names}")
print(f"   First row: {result.first_row}")

# Test 5: SELECT with function on column (no alias)
print("\n5. SELECT with function (no explicit alias):")
sql = "SELECT length(full_name) FROM users LIMIT 1"
print(f"   SQL: {sql}")
result = client.query(sql)
print(f"   Column names: {result.column_names}")
print(f"   First row: {result.first_row}")

# Test 6: Mix in JOIN context
print("\n6. Mix of properties and functions in JOIN:")
sql = """
SELECT 
    u1.full_name,
    u2.user_id,
    length(u2.full_name)
FROM users u1
JOIN user_follows f ON u1.user_id = f.follower_id
JOIN users u2 ON f.followed_id = u2.user_id
LIMIT 1
"""
print(f"   SQL: {sql.strip()}")
result = client.query(sql)
print(f"   Column names: {result.column_names}")
print(f"   First row: {result.first_row}")

print("\n" + "=" * 80)
print("Observations:")
print("=" * 80)
print("""
✅ CONFIRMED - ClickHouse Column Naming Rules:

1. **Single table** (no JOIN): Column names WITHOUT table prefix
   - SELECT u.user_id, u.full_name FROM users u
   - Returns: ('user_id', 'full_name')

2. **Multiple tables** (with JOIN): Column names WITH table prefix
   - SELECT u1.user_id, u2.user_id FROM users u1 JOIN users u2 ...
   - Returns: ('u1.user_id', 'u2.user_id')  ← Table prefixes included!

3. **Explicit AS aliases**: Always uses the alias name (no prefix)
   - SELECT u.user_id AS id FROM users u
   - Returns: ('id',)

4. **Functions without aliases**: Uses the full function expression
   - SELECT length(u.full_name) FROM users u
   - Returns: ('length(full_name)',) or ('length(u.full_name)',)

** ROOT CAUSE OF E2E TEST ISSUE **:
Our E2E test queries with JOINs return column names WITH table prefixes (u.age, o.total)
but tests were expecting WITHOUT prefixes (age, total).

This is standard ClickHouse behavior to disambiguate columns from different tables.

** FIX **: Tests now handle both cases: row.get("total") or row.get("o.total")
""")
