#!/usr/bin/env python3
"""Load test schema into running server."""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import yaml

# Load the test schema
with open("test_integration.yaml", "r") as f:
    schema_content = f.read()

# Parse to get schema_name
schema = yaml.safe_load(schema_content)
print(f"Loading schema: {schema.get('name', 'N/A')}")

# Load via API
response = requests.post(
    f"{CLICKGRAPH_URL}/schemas/load",
    json={
        "schema_name": schema.get("name", "test_graph_schema"),
        "config_content": schema_content
    }
)

if response.status_code == 200:
    print("✓ Schema loaded successfully")
    print(f"Response: {response.json()}")
else:
    print(f"✗ Failed to load schema: {response.status_code}")
    print(f"Response: {response.text}")
