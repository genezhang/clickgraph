"""
Test parameterized views with ClickGraph HTTP API
Tests multi-tenancy feature end-to-end
"""
import requests
import json

BASE_URL = "http://localhost:8080"

# Load the schema first
print("1. Loading multi-tenant schema...")
with open('schemas/test/multi_tenant.yaml', 'r') as f:
    schema_yaml = f.read()

load_response = requests.post(
    f"{BASE_URL}/schemas/load",
    json={
        "schema_name": "multi_tenant_test",
        "config_content": schema_yaml
    }
)
print(f"   Schema load status: {load_response.status_code}")
if load_response.status_code != 200:
    print(f"   Error: {load_response.text}")
    exit(1)

print("   ✓ Schema loaded successfully\n")

# Test 1: Query with tenant_id = 'acme'
print("2. Test 1: Query ACME tenant users")
query1 = {
    "query": "MATCH (u:User) RETURN u.user_id, u.name, u.email",
    "schema_name": "multi_tenant_test",
    "view_parameters": {
        "tenant_id": "acme"
    }
}

response1 = requests.post(f"{BASE_URL}/query", json=query1)
print(f"   Status: {response1.status_code}")
if response1.status_code == 200:
    result = response1.json()
    print(f"   Found {len(result.get('results', []))} users:")
    for row in result.get('results', []):
        print(f"      - {row}")
    print("   ✓ Test 1 passed\n")
else:
    print(f"   ✗ Error: {response1.text}\n")

# Test 2: Query with tenant_id = 'globex'
print("3. Test 2: Query GLOBEX tenant users")
query2 = {
    "query": "MATCH (u:User) RETURN u.user_id, u.name, u.email",
    "schema_name": "multi_tenant_test",
    "view_parameters": {
        "tenant_id": "globex"
    }
}

response2 = requests.post(f"{BASE_URL}/query", json=query2)
print(f"   Status: {response2.status_code}")
if response2.status_code == 200:
    result = response2.json()
    print(f"   Found {len(result.get('results', []))} users:")
    for row in result.get('results', []):
        print(f"      - {row}")
    print("   ✓ Test 2 passed\n")
else:
    print(f"   ✗ Error: {response2.text}\n")

# Test 3: Query friendships for 'acme' tenant
print("4. Test 3: Query ACME tenant friendships")
query3 = {
    "query": "MATCH (u1:User)-[:FRIENDS_WITH]->(u2:User) RETURN u1.name, u2.name",
    "schema_name": "multi_tenant_test",
    "view_parameters": {
        "tenant_id": "acme"
    }
}

response3 = requests.post(f"{BASE_URL}/query", json=query3)
print(f"   Status: {response3.status_code}")
if response3.status_code == 200:
    result = response3.json()
    print(f"   Found {len(result.get('results', []))} friendships:")
    for row in result.get('results', []):
        print(f"      - {row['u1.name']} -> {row['u2.name']}")
    print("   ✓ Test 3 passed\n")
else:
    print(f"   ✗ Error: {response3.text}\n")

# Test 4: SQL-only mode to see generated SQL
print("5. Test 4: Check generated SQL")
query4 = {
    "query": "MATCH (u:User) WHERE u.country = 'USA' RETURN u.name",
    "schema_name": "multi_tenant_test",
    "view_parameters": {
        "tenant_id": "acme"
    },
    "sql_only": True
}

response4 = requests.post(f"{BASE_URL}/query", json=query4)
print(f"   Status: {response4.status_code}")
if response4.status_code == 200:
    result = response4.json()
    print(f"   Generated SQL:")
    print(f"   {result.get('sql', 'N/A')}")
    print("   ✓ Test 4 passed\n")
else:
    print(f"   ✗ Error: {response4.text}\n")

# Test 5: Missing view_parameters (should fail or use default)
print("6. Test 5: Query without view_parameters (expect error or empty)")
query5 = {
    "query": "MATCH (u:User) RETURN u.name LIMIT 1",
    "schema_name": "multi_tenant_test"
}

response5 = requests.post(f"{BASE_URL}/query", json=query5)
print(f"   Status: {response5.status_code}")
print(f"   Response: {response5.text[:200]}")
print()

print("=" * 60)
print("✓ All parameterized view tests completed!")
print("=" * 60)
