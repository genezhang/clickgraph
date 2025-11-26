#!/usr/bin/env python3
"""
Test RBAC with database-managed user (not users.xml).
"""

import requests
import json

CLICKHOUSE_URL = "http://localhost:8123"
ADMIN_USER = "default"  # Use default admin to create users
ADMIN_PASSWORD = ""
DATABASE = "brahmand"

def execute_as_admin(sql):
    """Execute SQL as admin."""
    params = {"user": ADMIN_USER, "password": ADMIN_PASSWORD, "database": DATABASE}
    response = requests.post(CLICKHOUSE_URL, params=params, data=sql)
    return {"status": response.status_code, "text": response.text}

def execute_as_user(sql, user, password=""):
    """Execute SQL as specific user."""
    params = {"user": user, "password": password, "database": DATABASE}
    response = requests.post(CLICKHOUSE_URL, params=params, data=sql)
    if response.status_code == 200:
        try:
            return {"status": "success", "json": response.json() if response.text.strip() else None, "text": response.text}
        except:
            return {"status": "success", "text": response.text}
    else:
        return {"status": "error", "text": response.text}

print("=" * 80)
print("TESTING RBAC WITH DATABASE-MANAGED USER")
print("=" * 80)

# Step 1: Create database-managed user
print("\n" + "="*80)
print("SETUP: Create DB User with Roles")
print("="*80)

print("\n1. Cleanup old test objects...")
for sql in [
    "DROP USER IF EXISTS app_user",
    "DROP ROLE IF EXISTS admin_role",
    "DROP ROLE IF EXISTS viewer_role",
]:
    result = execute_as_admin(sql)
    print(f"   {sql}: {result['status']}")

print("\n2. Create roles...")
for sql in [
    "CREATE ROLE admin_role",
    "CREATE ROLE viewer_role",
]:
    result = execute_as_admin(sql)
    print(f"   {sql}: {result['status']}")

print("\n3. Grant permissions to roles...")
for sql in [
    "GRANT SELECT, INSERT, CREATE TABLE ON brahmand.* TO viewer_role",
    "GRANT ALL ON brahmand.* TO admin_role",
]:
    result = execute_as_admin(sql)
    print(f"   {sql}: {result['status']}")

print("\n4. Create database-managed user...")
result = execute_as_admin("CREATE USER app_user IDENTIFIED WITH plaintext_password BY 'password123'")
print(f"   CREATE USER: {result['status']}")

print("\n5. Grant roles to user...")
for sql in [
    "GRANT admin_role TO app_user",
    "GRANT viewer_role TO app_user",
]:
    result = execute_as_admin(sql)
    print(f"   {sql}: {result['status']}")

# Step 2: Test SET ROLE
print("\n" + "="*80)
print("TEST: SET ROLE Command")
print("="*80)

print("\n1. Check default roles for app_user...")
result = execute_as_user("SELECT * FROM system.current_roles FORMAT JSONCompact", "app_user", "password123")
print(f"   Current roles: {result.get('json', {}).get('data', []) if result['status'] == 'success' else result['text']}")

print("\n2. Execute SET ROLE admin_role...")
result = execute_as_user("SET ROLE admin_role", "app_user", "password123")
print(f"   Status: {result['status']}")
if result['status'] == 'error':
    print(f"   Error: {result['text'][:300]}")

print("\n3. Check current roles after SET ROLE...")
result = execute_as_user("SELECT * FROM system.current_roles FORMAT JSONCompact", "app_user", "password123")
if result['status'] == 'success':
    print(f"   ✅ Current roles: {result['json']['data']}")
else:
    print(f"   ❌ Error: {result['text'][:200]}")

print("\n4. Execute SET ROLE viewer_role...")
result = execute_as_user("SET ROLE viewer_role", "app_user", "password123")
print(f"   Status: {result['status']}")

print("\n5. Check current roles after switching...")
result = execute_as_user("SELECT * FROM system.current_roles FORMAT JSONCompact", "app_user", "password123")
if result['status'] == 'success':
    print(f"   ✅ Current roles: {result['json']['data']}")
else:
    print(f"   ❌ Error: {result['text'][:200]}")

# Step 3: Test role-filtered view
print("\n" + "="*80)
print("TEST: Role-Filtered Views")
print("="*80)

print("\n1. Create test table (as admin)...")
execute_as_admin("DROP TABLE IF EXISTS rbac_data")
result = execute_as_admin("""
CREATE TABLE rbac_data (
    id Int32,
    content String,
    required_role String
) ENGINE = Memory
""")
print(f"   Status: {result['status']}")

print("\n2. Insert test data...")
result = execute_as_admin("""
INSERT INTO rbac_data VALUES 
    (1, 'Viewer content', 'viewer_role'),
    (2, 'Admin content', 'admin_role'),
    (3, 'More admin stuff', 'admin_role')
""")
print(f"   Status: {result['status']}")

print("\n3. Create role-filtered view...")
execute_as_admin("DROP VIEW IF EXISTS rbac_filtered")
result = execute_as_admin("""
CREATE VIEW rbac_filtered AS
SELECT * FROM rbac_data
WHERE required_role IN (SELECT role_name FROM system.current_roles)
""")
print(f"   Status: {result['status']}")

print("\n4. Query view with viewer_role...")
execute_as_user("SET ROLE viewer_role", "app_user", "password123")
result = execute_as_user("SELECT * FROM rbac_filtered FORMAT JSONCompact", "app_user", "password123")
if result['status'] == 'success' and result['json']:
    print(f"   ✅ Rows: {len(result['json']['data'])}")
    print(f"   Data: {json.dumps(result['json']['data'], indent=4)}")
else:
    print(f"   ❌ Error: {result}")

print("\n5. Query view with admin_role...")
execute_as_user("SET ROLE admin_role", "app_user", "password123")
result = execute_as_user("SELECT * FROM rbac_filtered FORMAT JSONCompact", "app_user", "password123")
if result['status'] == 'success' and result['json']:
    print(f"   ✅ Rows: {len(result['json']['data'])}")
    print(f"   Data: {json.dumps(result['json']['data'], indent=4)}")
else:
    print(f"   ❌ Error: {result}")

print("\n6. Query view with ALL roles...")
execute_as_user("SET ROLE admin_role, viewer_role", "app_user", "password123")
result = execute_as_user("SELECT * FROM rbac_filtered FORMAT JSONCompact", "app_user", "password123")
if result['status'] == 'success' and result['json']:
    print(f"   ✅ Rows: {len(result['json']['data'])}")
    print(f"   Data: {json.dumps(result['json']['data'], indent=4)}")
else:
    print(f"   ❌ Error: {result}")

# Cleanup
print("\n" + "="*80)
print("CLEANUP")
print("="*80)
execute_as_admin("DROP VIEW IF EXISTS rbac_filtered")
execute_as_admin("DROP TABLE IF EXISTS rbac_data")
print("   Done!")

print("\n" + "="*80)
print("CONCLUSION")
print("="*80)
print("""
If SET ROLE works correctly:
✅ RBAC implementation is SIMPLE:
   1. Add 'role' field to QueryRequest
   2. Execute "SET ROLE <role>" before each query
   3. Views use: WHERE x IN (SELECT role_name FROM system.current_roles)
   4. No custom settings needed!

Performance: SET ROLE requires 1 extra round-trip per query, same as 
the session settings approach we were considering.
""")
print("="*80)
