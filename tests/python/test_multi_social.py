#!/usr/bin/env python3
"""Test multiple relationships with social_network schema"""

import requests
import json
import time

# Load social_network schema
print("Loading social_network schema...")

# Read the schema YAML file (path relative to project root)
import os
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
schema_path = project_root / "schemas" / "demo" / "social_network.yaml"

with open(schema_path, "r", encoding="utf-8") as f:
    schema_content = f.read()

load_response = requests.post(
    "http://localhost:8080/schemas/load",
    json={
        "schema_name": "social_network",
        "config_content": schema_content,
        "validate_schema": False
    }
)

print(f"Schema load status: {load_response.status_code}")
if load_response.status_code != 200:
    print(f"Error: {load_response.text}")
    exit(1)

time.sleep(1)

# Test query with FOLLOWS relationship
query = """
MATCH (u:User)-[:FOLLOWS]->(target:User)
RETURN u.name, target.name
LIMIT 5
"""

print(f"\nTesting query: {query}\n")

response = requests.post(
    "http://localhost:8080/query",
    json={"query": query}
)

print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    result = response.json()
    print(f"Generated SQL:\n{result.get('generated_sql', 'N/A')}\n")
    print(f"Result: {json.dumps(result, indent=2)}")
else:
    print(f"Error: {response.text}")
