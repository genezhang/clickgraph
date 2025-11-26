import requests

CLICKHOUSE_URL = "http://localhost:8123"
params = {"user": "test_user", "password": "test_pass", "database": "brahmand"}

# Test 1: SQL_ prefix with SETTINGS clause
print("Test 1: SQL_ prefix with SETTINGS clause")
sql = "SELECT getSetting('SQL_user_id') as user_id FORMAT JSONCompact SETTINGS SQL_user_id='alice'"
print(f"SQL: {sql}")
r = requests.post(CLICKHOUSE_URL, params=params, data=sql)
print(f"Status: {r.status_code}")
print(f"Response: {r.text}\n")

# Test 2: SQL_ prefix with SET statement
print("Test 2: SQL_ prefix with SET statement")
sql = "SET SQL_user_id = 'bob'"
print(f"SQL: {sql}")
r = requests.post(CLICKHOUSE_URL, params=params, data=sql)
print(f"Status: {r.status_code}")
print(f"Response: {r.text}\n")

# Test 3: Read back the SET value
print("Test 3: Read back after SET")
sql = "SELECT getSetting('SQL_user_id') as user_id FORMAT JSONCompact"
print(f"SQL: {sql}")
r = requests.post(CLICKHOUSE_URL, params=params, data=sql)
print(f"Status: {r.status_code}")
print(f"Response: {r.text}\n")
