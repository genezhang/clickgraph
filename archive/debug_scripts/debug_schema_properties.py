"""
Debug script to check what schema is actually being used
"""
import requests
import json

# Query using the test schema
response = requests.post(
    "http://localhost:8080/query",
    json={
        "query": "MATCH (n:User) RETURN n.name LIMIT 1",
        "schema_name": "test_integration"
    }
)

print("Status:", response.status_code)
if response.status_code == 200:
    print("Response:", json.dumps(response.json(), indent=2))
else:
    print("Error:", response.text)

# Also try with a direct property check
print("\n---Trying to access both name and age---")
response2 = requests.post(
    "http://localhost:8080/query",
    json={
        "query": "MATCH (n:User) RETURN n.name, n.age LIMIT 1",
        "schema_name": "test_integration"
    }
)

print("Status:", response2.status_code)
if response2.status_code == 200:
    print("Success! Response:", json.dumps(response2.json(), indent=2))
else:
    print("Error:", response2.text)
