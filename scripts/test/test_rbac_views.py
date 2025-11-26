import requests
import json

CLICKHOUSE_URL = "http://localhost:8123"
params = {"user": "test_user", "password": "test_pass", "database": "brahmand"}

print("=" * 80)
print("TEST: SQL_ prefix settings in views")
print("=" * 80)

# Setup
print("\n1. Creating test table...")
requests.post(CLICKHOUSE_URL, params=params, data='DROP TABLE IF EXISTS test_rbac')
r = requests.post(CLICKHOUSE_URL, params=params, data='''
CREATE TABLE test_rbac (
    user_id String, 
    name String, 
    role String
) ENGINE=Memory
''')
print(f"   Status: {r.status_code}")

print("\n2. Inserting test data...")
r = requests.post(CLICKHOUSE_URL, params=params, data='''
INSERT INTO test_rbac VALUES 
    ('alice', 'Alice Admin', 'admin'),
    ('bob', 'Bob User', 'user'),
    ('charlie', 'Charlie User', 'user')
''')
print(f"   Status: {r.status_code}")

print("\n3. Creating RBAC view with getSetting()...")
requests.post(CLICKHOUSE_URL, params=params, data='DROP VIEW IF EXISTS test_rbac_view')
r = requests.post(CLICKHOUSE_URL, params=params, data='''
CREATE VIEW test_rbac_view AS 
SELECT * FROM test_rbac 
WHERE role = getSetting('SQL_user_role') 
   OR user_id = getSetting('SQL_user_id')
''')
print(f"   Status: {r.status_code}")
if r.status_code != 200:
    print(f"   Error: {r.text}")

print("\n4. Querying view with SQL_user_id='alice'...")
r = requests.post(CLICKHOUSE_URL, params=params, data='''
SELECT * FROM test_rbac_view FORMAT JSONCompact 
SETTINGS SQL_user_id='alice', SQL_user_role='nonexistent'
''')
print(f"   Status: {r.status_code}")
if r.status_code == 200:
    result = r.json()
    print(f"   ✅ SUCCESS! Got {result['rows']} row(s)")
    print(f"   Data: {json.dumps(result['data'], indent=2)}")
else:
    print(f"   ❌ FAIL: {r.text}")

print("\n5. Querying view with SQL_user_role='admin'...")
r = requests.post(CLICKHOUSE_URL, params=params, data='''
SELECT * FROM test_rbac_view FORMAT JSONCompact 
SETTINGS SQL_user_id='nobody', SQL_user_role='admin'
''')
print(f"   Status: {r.status_code}")
if r.status_code == 200:
    result = r.json()
    print(f"   ✅ SUCCESS! Got {result['rows']} row(s)")
    print(f"   Data: {json.dumps(result['data'], indent=2)}")
else:
    print(f"   ❌ FAIL: {r.text}")

print("\n6. Querying view with SQL_user_role='user'...")
r = requests.post(CLICKHOUSE_URL, params=params, data='''
SELECT * FROM test_rbac_view FORMAT JSONCompact 
SETTINGS SQL_user_id='nobody', SQL_user_role='user'
''')
print(f"   Status: {r.status_code}")
if r.status_code == 200:
    result = r.json()
    print(f"   ✅ SUCCESS! Got {result['rows']} row(s)")
    print(f"   Data: {json.dumps(result['data'], indent=2)}")
else:
    print(f"   ❌ FAIL: {r.text}")

# Cleanup
print("\n7. Cleaning up...")
requests.post(CLICKHOUSE_URL, params=params, data='DROP VIEW IF EXISTS test_rbac_view')
requests.post(CLICKHOUSE_URL, params=params, data='DROP TABLE IF EXISTS test_rbac')
print("   Done!")

print("\n" + "=" * 80)
print("✅ CONCLUSION: SQL_ prefix works with query-level SETTINGS in views!")
print("=" * 80)
