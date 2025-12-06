#!/usr/bin/env python3
"""Load test schema into running server as both test_graph_schema and default."""

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

# Load as test_graph_schema
response = requests.post(
    f"{CLICKGRAPH_URL}/schemas/load",
    json={
        "schema_name": "test_graph_schema",
        "config_content": schema_content
    }
)

if response.status_code == 200:
    print("✓ Schema loaded as 'test_graph_schema'")
else:
    print(f"✗ Failed: {response.text}")

# Also load as "default" to work around hardcoded schema lookup
response2 = requests.post(
    f"{CLICKGRAPH_URL}/schemas/load",
    json={
        "schema_name": "default",
        "config_content": schema_content
    }
)

if response2.status_code == 200:
    print("✓ Schema loaded as 'default' (workaround)")
else:
    print(f"✗ Failed to load as default: {response2.text}")
