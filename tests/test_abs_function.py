"""
Test abs() function in WHERE clause
"""
import requests
import json

BASE_URL = "http://localhost:8080"

# First, load the schema
schema_yaml = """
node_types:
  User:
    table: test_param_func.users
    id_column: id
    properties:
      name: name
      age: age
      email: email
"""

print("Loading schema...")
response = requests.post(
    f"{BASE_URL}/schemas/load",
    json={
        "schema_name": "test_abs_schema",
        "config_content": schema_yaml,
        "validate_schema": False
    }
)
print(f"Schema load status: {response.status_code}")

# Test the query
print("\nTesting query with abs() in WHERE clause...")
query = """
MATCH (u:User)
WHERE abs(u.age - $targetAge) < $tolerance
RETURN u.name, u.age
ORDER BY u.age
"""

payload = {
    "query": query,
    "parameters": {"targetAge": 30, "tolerance": 5},
    "schema_name": "test_abs_schema"
}

print(f"Query: {query}")
print(f"Parameters: {payload['parameters']}")

response = requests.post(f"{BASE_URL}/query", json=payload)
print(f"\nResponse status: {response.status_code}")
print(f"Response body: {response.text}")

if response.status_code == 200:
    print("✅ Query succeeded!")
    print(json.dumps(response.json(), indent=2))
else:
    print(f"❌ Query failed with status {response.status_code}")
