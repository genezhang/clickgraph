#!/usr/bin/env python3
"""
Test if a read-only (permission-wise) user can use SET ROLE.

The key distinction:
- users.xml storage = read-only (can't modify user definition)
- Database permissions = what operations the user can perform

Question: Can test_user use SET ROLE even though they're defined in users.xml?
"""

import requests

CLICKHOUSE_URL = "http://localhost:8123"
params = {"user": "test_user", "password": "test_pass", "database": "brahmand"}

print("=" * 80)
print("TESTING: Can users.xml users use SET ROLE?")
print("=" * 80)

print("\n1. Check what roles exist...")
r = requests.post(CLICKHOUSE_URL, params=params, data='SELECT name FROM system.roles FORMAT JSONCompact')
if r.status_code == 200:
    roles = r.json()['data']
    print(f"   Available roles: {[r[0] for r in roles]}")
else:
    print(f"   Error: {r.text}")

print("\n2. Check role_grants (which users have which roles)...")
r = requests.post(CLICKHOUSE_URL, params=params, 
                  data='SELECT user_name, role_name, granted_role_name FROM system.role_grants FORMAT JSONCompact')
if r.status_code == 200:
    grants = r.json()
    print(f"   Role grants: {grants['data']}")
else:
    print(f"   Error: {r.text[:200]}")

print("\n3. Try to SET ROLE without having it granted...")
r = requests.post(CLICKHOUSE_URL, params=params, data='SET ROLE app_admin')
if r.status_code == 200:
    print(f"   ✅ SET ROLE succeeded!")
else:
    print(f"   ❌ Expected error (role not granted): {r.text[:200]}")

print("\n4. Check current roles...")
r = requests.post(CLICKHOUSE_URL, params=params, data='SELECT * FROM system.current_roles FORMAT JSONCompact')
if r.status_code == 200:
    print(f"   Current roles: {r.json()['data']}")

print("\n" + "=" * 80)
print("KEY INSIGHT")
print("=" * 80)
print("""
The issue is NOT about read-only vs admin permissions!

The problem is:
- test_user is defined in users.xml (read-only storage)
- We CANNOT GRANT roles to users.xml users (storage limitation)
- We CAN ONLY grant roles to database-managed users

To use SET ROLE, a user needs:
1. Roles to be granted to them (via GRANT role TO user)
2. This requires the user to be in database storage, not users.xml

In production:
- Use database-managed users (CREATE USER ... IDENTIFIED WITH ...)
- Grant roles to those users
- Those users can then SET ROLE (regardless of their permissions)
- A "viewer" with only SELECT permission can still SET ROLE if roles are granted

CONCLUSION:
- SET ROLE is NOT an admin-only command
- ANY user can use SET ROLE if roles are granted to them
- The limitation is just the storage backend (users.xml vs database)
""")
print("=" * 80)

print("\n5. Can we use system.enabled_roles in views without SET ROLE?")
print("   (Alternative approach if SET ROLE doesn't work)")

# Create test view that checks system.enabled_roles
r = requests.post(CLICKHOUSE_URL, params=params, data='DROP TABLE IF EXISTS role_test')
r = requests.post(CLICKHOUSE_URL, params=params, 
                  data='CREATE TABLE role_test (id Int32, data String, for_role String) ENGINE=Memory')
r = requests.post(CLICKHOUSE_URL, params=params,
                  data="INSERT INTO role_test VALUES (1, 'data1', 'app_admin'), (2, 'data2', 'app_viewer')")

r = requests.post(CLICKHOUSE_URL, params=params, data='DROP VIEW IF EXISTS role_filtered_view')
r = requests.post(CLICKHOUSE_URL, params=params,
                  data='CREATE VIEW role_filtered_view AS SELECT * FROM role_test WHERE for_role IN (SELECT role_name FROM system.enabled_roles)')

print("\n   Created view with system.enabled_roles filter...")
r = requests.post(CLICKHOUSE_URL, params=params, 
                  data='SELECT * FROM role_filtered_view FORMAT JSONCompact')
if r.status_code == 200:
    result = r.json()
    print(f"   Rows without SET ROLE: {len(result['data'])}")
    print(f"   Data: {result['data']}")
else:
    print(f"   Error: {r.text[:200]}")

# Cleanup
requests.post(CLICKHOUSE_URL, params=params, data='DROP VIEW IF EXISTS role_filtered_view')
requests.post(CLICKHOUSE_URL, params=params, data='DROP TABLE IF EXISTS role_test')

print("\n" + "=" * 80)
print("RECOMMENDATION FOR PHASE 2")
print("=" * 80)
print("""
Since we can't fully test SET ROLE in our current setup:

Option A: Document the limitation
- Add 'role' field to QueryRequest
- Implement SET ROLE logic
- Document: "Requires database-managed users with granted roles"
- Test in production/integration environment

Option B: Skip RBAC for Phase 2
- Focus ONLY on parameterized views (tenant_id, view_parameters)
- Document: "RBAC requires ClickHouse native roles (see docs)"
- Let users handle RBAC via ClickHouse's native system

Option C: Hybrid approach
- Keep tenant_id + view_parameters (parameterized views)
- Add optional 'role' field for future use
- Document but don't heavily test until proper env

My recommendation: Option C (add field, document, move forward)
""")
print("=" * 80)
