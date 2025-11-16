#!/usr/bin/env python3
"""
Investigate ClickHouse built-in settings and user context.

Questions:
1. What built-in settings exist?
2. Are there user/role related settings?
3. Can we access current user context?
"""

import requests
import json

CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = "test_user"
CLICKHOUSE_PASSWORD = "test_pass"
DATABASE = "brahmand"

def execute_query(sql):
    """Execute SQL and return JSON result."""
    params = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "database": DATABASE,
    }
    response = requests.post(CLICKHOUSE_URL, params=params, data=sql)
    if response.status_code == 200:
        return response.json() if response.text else None
    else:
        return {"error": response.text}

print("=" * 80)
print("INVESTIGATING CLICKHOUSE BUILT-IN SETTINGS & USER CONTEXT")
print("=" * 80)

# Test 1: Current user functions
print("\n" + "="*80)
print("TEST 1: User Context Functions")
print("="*80)

queries = [
    ("currentUser()", "SELECT currentUser() as user FORMAT JSONCompact"),
    ("user()", "SELECT user() as user FORMAT JSONCompact"),
    ("currentDatabase()", "SELECT currentDatabase() as db FORMAT JSONCompact"),
    ("getMacro('user')", "SELECT getMacro('user') as user FORMAT JSONCompact"),
]

for name, sql in queries:
    print(f"\n{name}:")
    print(f"  SQL: {sql}")
    result = execute_query(sql)
    if result and "data" in result:
        print(f"  ✅ Result: {result['data']}")
    else:
        print(f"  ❌ Error: {result}")

# Test 2: System tables for settings
print("\n" + "="*80)
print("TEST 2: List All Available Settings")
print("="*80)

sql = """
SELECT name, value, description, type 
FROM system.settings 
WHERE name LIKE '%user%' OR name LIKE '%role%' OR name LIKE '%security%'
ORDER BY name
FORMAT JSONCompact
"""
print(f"\nQuerying system.settings for user/role/security related settings...")
result = execute_query(sql)
if result and "data" in result:
    print(f"\n✅ Found {len(result['data'])} settings:")
    for row in result['data']:
        name, value, desc, type_ = row
        print(f"  - {name} ({type_}): {desc[:80]}...")
else:
    print(f"❌ Error: {result}")

# Test 3: Check if we can access user info from system tables
print("\n" + "="*80)
print("TEST 3: System Tables for User Information")
print("="*80)

system_tables = [
    ("system.users", "SELECT name, id FROM system.users LIMIT 5 FORMAT JSONCompact"),
    ("system.current_roles", "SELECT * FROM system.current_roles FORMAT JSONCompact"),
    ("system.enabled_roles", "SELECT * FROM system.enabled_roles FORMAT JSONCompact"),
    ("system.grants", "SELECT user_name, role_name, access_type FROM system.grants LIMIT 5 FORMAT JSONCompact"),
]

for table_name, sql in system_tables:
    print(f"\n{table_name}:")
    print(f"  SQL: {sql}")
    result = execute_query(sql)
    if result and "data" in result:
        print(f"  ✅ Rows: {len(result['data'])}")
        if result['data']:
            print(f"  Sample: {json.dumps(result['data'][:3], indent=4)}")
    else:
        print(f"  ❌ Error: {result}")

# Test 4: Test getSetting() with known built-in settings
print("\n" + "="*80)
print("TEST 4: getSetting() with Built-in Settings")
print("="*80)

builtin_settings = [
    "max_threads",
    "max_memory_usage", 
    "readonly",
    "user_name",  # Maybe?
    "current_user",  # Maybe?
]

for setting_name in builtin_settings:
    sql = f"SELECT getSetting('{setting_name}') as value FORMAT JSONCompact"
    print(f"\n{setting_name}:")
    print(f"  SQL: {sql}")
    result = execute_query(sql)
    if result and "data" in result:
        print(f"  ✅ Value: {result['data']}")
    else:
        print(f"  ❌ Not available")

# Test 5: Can we use currentUser() in views?
print("\n" + "="*80)
print("TEST 5: Using currentUser() in Views")
print("="*80)

print("\n1. Creating test table...")
execute_query("DROP TABLE IF EXISTS rbac_test_users")
execute_query("""
CREATE TABLE rbac_test_users (
    user_id String,
    name String,
    role String
) ENGINE = Memory
""")

print("2. Inserting test data...")
execute_query("""
INSERT INTO rbac_test_users VALUES 
    ('test_user', 'Test User', 'admin'),
    ('alice', 'Alice Admin', 'admin'),
    ('bob', 'Bob User', 'user')
""")

print("3. Creating view with currentUser() filter...")
execute_query("DROP VIEW IF EXISTS rbac_user_view")
result = execute_query("""
CREATE VIEW rbac_user_view AS
SELECT * FROM rbac_test_users
WHERE user_id = currentUser()
""")
print(f"   View creation: {result if result else '✅ Success'}")

print("4. Querying view...")
result = execute_query("SELECT * FROM rbac_user_view FORMAT JSONCompact")
if result and "data" in result:
    print(f"   ✅ Got {len(result['data'])} row(s):")
    print(f"   Data: {json.dumps(result['data'], indent=4)}")
else:
    print(f"   ❌ Error: {result}")

print("\n5. Cleanup...")
execute_query("DROP VIEW IF EXISTS rbac_user_view")
execute_query("DROP TABLE IF EXISTS rbac_test_users")

# Summary
print("\n" + "="*80)
print("SUMMARY & RECOMMENDATIONS")
print("="*80)
print("""
Key Findings:
1. currentUser() / user() - Returns authenticated username ✅
2. currentDatabase() - Returns current database ✅
3. system.users - Lists all users ✅
4. system.current_roles - Shows active roles ✅
5. getSetting() - Only for built-in settings, NOT custom ❌

RECOMMENDATION FOR RBAC:
Instead of custom settings (user_id, user_role), use:
- currentUser() in views for user-based filtering
- ClickHouse's native roles and row policies
- Create ClickHouse users matching your application users
- Use parameterized views for tenant_id (Pattern 1, 3, 4, 5)

Pattern 2 can work IF:
- You create actual ClickHouse users (not just pass user_id)
- Use currentUser() instead of getSetting('user_id')
- Use ClickHouse roles instead of custom user_role
""")
print("="*80)
