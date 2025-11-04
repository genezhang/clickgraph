#!/usr/bin/env python3
"""Load schema and test multiple relationships"""

import requests
import json
import time

# First load the schema
print("Loading schema...")

load_response = requests.post(
    "http://localhost:8080/schemas/load",
    json={
        "schema_name": "test_multi_rel_schema",
        "config_path": "test_multi_rel_schema.yaml",
        "validate_schema": False
    }
)

print(f"Schema load status: {load_response.status_code}")
if load_response.status_code != 200:
    print(f"Schema load error: {load_response.text}")
    exit(1)

time.sleep(1)

# Now test the query
query = """
MATCH (u:User)-[:FOLLOWS|FRIENDS_WITH]->(target:User)
RETURN u.name, target.name
LIMIT 5
"""

print(f"\nTesting query...")
print(f"Query: {query}\n")

response = requests.post(
    "http://localhost:8080/query",
    json={
        "query": query,
        "schema_name": "test_multi_rel_schema"  # Specify the schema we loaded
    }
)

print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    result = response.json()
    print(f"Result: {json.dumps(result, indent=2)}")
else:
    print(f"Error: {response.text}")
