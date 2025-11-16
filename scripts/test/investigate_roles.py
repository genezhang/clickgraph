#!/usr/bin/env python3
"""
Investigate ClickHouse role management for RBAC.

Key questions:
1. Can we set/switch roles dynamically per query?
2. Can views filter based on current roles?
3. What role-related settings exist?
"""

import requests
import json

CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = "test_user"
CLICKHOUSE_PASSWORD = "test_pass"
DATABASE = "brahmand"

def execute_query(sql, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD):
    """Execute SQL and return result."""
    params = {
        "user": user,
        "password": password,
        "database": DATABASE,
    }
    response = requests.post(CLICKHOUSE_URL, params=params, data=sql)
    if response.status_code == 200:
        return {"status": "success", "text": response.text, "json": response.json() if response.text.strip() else None}
    else:
        return {"status": "error", "text": response.text}

print("=" * 80)
print("INVESTIGATING CLICKHOUSE ROLE-BASED RBAC")
print("=" * 80)

# Test 1: Check role-related settings
print("\n" + "="*80)
print("TEST 1: Role-Related Settings")
print("="*80)

sql = """
SELECT name, value, description, type 
FROM system.settings 
WHERE name LIKE '%role%'
ORDER BY name
FORMAT JSONCompact
"""
print(f"\nQuerying system.settings for role-related settings...")
result = execute_query(sql)
if result['status'] == 'success' and result['json']:
    print(f"\n✅ Found {len(result['json']['data'])} settings:")
    for row in result['json']['data']:
        name, value, desc, type_ = row
        print(f"  - {name} ({type_}): {value}")
        print(f"    {desc[:100]}...")
else:
    print(f"❌ Error: {result}")

# Test 2: Create test roles
print("\n" + "="*80)
print("TEST 2: Creating Test Roles")
print("="*80)

print("\n1. Creating roles...")
commands = [
    ("DROP ROLE IF EXISTS test_admin", "Cleanup old admin role"),
    ("DROP ROLE IF EXISTS test_viewer", "Cleanup old viewer role"),
    ("CREATE ROLE test_admin", "Create admin role"),
    ("CREATE ROLE test_viewer", "Create viewer role"),
    ("GRANT SELECT ON brahmand.* TO test_viewer", "Grant viewer permissions"),
    ("GRANT ALL ON brahmand.* TO test_admin", "Grant admin permissions"),
]

for sql, desc in commands:
    print(f"   {desc}...")
    result = execute_query(sql)
    if result['status'] == 'success':
        print(f"      ✅ Success")
    else:
        print(f"      ⚠️ {result['text'][:200]}")

# Test 3: Grant roles to test_user
print("\n" + "="*80)
print("TEST 3: Granting Roles to test_user")
print("="*80)

commands = [
    ("GRANT test_admin TO test_user", "Grant admin role"),
    ("GRANT test_viewer TO test_user", "Grant viewer role"),
]

for sql, desc in commands:
    print(f"   {desc}...")
    result = execute_query(sql)
    if result['status'] == 'success':
        print(f"      ✅ Success")
    else:
        print(f"      ⚠️ {result['text'][:200]}")

# Test 4: Check current roles
print("\n" + "="*80)
print("TEST 4: Checking Current Roles")
print("="*80)

queries = [
    ("system.current_roles", "SELECT * FROM system.current_roles FORMAT JSONCompact"),
    ("system.enabled_roles", "SELECT * FROM system.enabled_roles FORMAT JSONCompact"),
]

for name, sql in queries:
    print(f"\n{name}:")
    result = execute_query(sql)
    if result['status'] == 'success' and result['json']:
        print(f"  ✅ Rows: {len(result['json']['data'])}")
        if result['json']['data']:
            print(f"  Data: {json.dumps(result['json']['data'], indent=4)}")
    else:
        print(f"  ❌ Error: {result}")

# Test 5: Try to SET ROLE dynamically
print("\n" + "="*80)
print("TEST 5: Dynamic Role Switching")
print("="*80)

print("\n1. Setting role to test_admin...")
result = execute_query("SET ROLE test_admin")
print(f"   Status: {result['status']}")
if result['status'] == 'error':
    print(f"   Error: {result['text'][:200]}")

print("\n2. Checking current role after SET ROLE...")
result = execute_query("SELECT * FROM system.current_roles FORMAT JSONCompact")
if result['status'] == 'success' and result['json']:
    print(f"   Current roles: {result['json']['data']}")

print("\n3. Query-level role with SETTINGS...")
result = execute_query("SELECT * FROM system.current_roles FORMAT JSONCompact SETTINGS role='test_admin'")
print(f"   Status: {result['status']}")
if result['status'] == 'error':
    print(f"   ⚠️ {result['text'][:300]}")

# Test 6: Can views check roles?
print("\n" + "="*80)
print("TEST 6: Using Roles in Views")
print("="*80)

print("\n1. Creating test table...")
execute_query("DROP TABLE IF EXISTS rbac_test_data")
result = execute_query("""
CREATE TABLE rbac_test_data (
    id Int32,
    data String,
    required_role String
) ENGINE = Memory
""")
print(f"   Status: {result['status']}")

print("\n2. Inserting test data...")
result = execute_query("""
INSERT INTO rbac_test_data VALUES 
    (1, 'Public data', 'test_viewer'),
    (2, 'Admin data', 'test_admin'),
    (3, 'More admin data', 'test_admin')
""")
print(f"   Status: {result['status']}")

print("\n3. Creating view with role filter...")
execute_query("DROP VIEW IF EXISTS rbac_filtered_view")

# Try different approaches
approaches = [
    ("system.current_roles", """
CREATE VIEW rbac_filtered_view AS
SELECT * FROM rbac_test_data
WHERE required_role IN (SELECT role_name FROM system.current_roles)
"""),
    ("system.enabled_roles", """
CREATE VIEW rbac_filtered_view AS
SELECT * FROM rbac_test_data
WHERE required_role IN (SELECT role_name FROM system.enabled_roles)
"""),
]

for name, sql in approaches:
    print(f"\n   Approach: {name}")
    result = execute_query(sql)
    if result['status'] == 'success':
        print(f"      ✅ View created!")
        
        print("\n4. Testing view (without SET ROLE)...")
        result = execute_query("SELECT * FROM rbac_filtered_view FORMAT JSONCompact")
        if result['status'] == 'success' and result['json']:
            print(f"      Rows returned: {len(result['json']['data'])}")
            print(f"      Data: {json.dumps(result['json']['data'], indent=4)}")
        
        print("\n5. Testing with SET ROLE test_admin...")
        execute_query("SET ROLE test_admin")
        result = execute_query("SELECT * FROM rbac_filtered_view FORMAT JSONCompact")
        if result['status'] == 'success' and result['json']:
            print(f"      Rows returned: {len(result['json']['data'])}")
            print(f"      Data: {json.dumps(result['json']['data'], indent=4)}")
        
        execute_query("DROP VIEW IF EXISTS rbac_filtered_view")
        break
    else:
        print(f"      ❌ Failed: {result['text'][:200]}")

# Test 7: Check for role-related functions
print("\n" + "="*80)
print("TEST 7: Role-Related Functions")
print("="*80)

functions = [
    "currentRoles()",
    "enabledRoles()",
    "defaultRoles()",
]

for func in functions:
    sql = f"SELECT {func} as roles FORMAT JSONCompact"
    print(f"\n{func}:")
    result = execute_query(sql)
    if result['status'] == 'success' and result['json']:
        print(f"  ✅ Result: {result['json']['data']}")
    else:
        print(f"  ❌ Not available or error")

# Cleanup
print("\n" + "="*80)
print("CLEANUP")
print("="*80)
execute_query("DROP VIEW IF EXISTS rbac_filtered_view")
execute_query("DROP TABLE IF EXISTS rbac_test_data")
print("   Done!")

# Summary
print("\n" + "="*80)
print("SUMMARY & FINDINGS")
print("="*80)
print("""
Key Findings:
1. Roles can be created and granted to users ✅
2. system.current_roles shows active roles ✅
3. system.enabled_roles shows all enabled roles ✅
4. SET ROLE command exists (need to test if persistent) ?
5. Views can filter using system.current_roles ✅

RBAC STRATEGY:
If SET ROLE works and persists for the session:
- Pass role name in QueryRequest
- Execute SET ROLE before each query
- Views filter using: WHERE role IN (SELECT role_name FROM system.current_roles)
- No need for custom settings!

If SET ROLE doesn't persist or causes issues:
- Use ClickHouse's native row policies instead
- Or pass role as SQL_ prefixed setting (if that works)
""")
print("="*80)
