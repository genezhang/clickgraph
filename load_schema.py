#!/usr/bin/env python3
"""Load a graph schema into ClickGraph server"""

import requests
import json

# Read the YAML schema file
with open('ecommerce_simple.yaml', 'r') as f:
    yaml_content = f.read()

# Prepare the JSON payload
payload = {
    "schema_name": "ecommerce_demo",
    "config_content": yaml_content,
    "validate_schema": False
}

# Send POST request
response = requests.post(
    'http://localhost:8080/schemas/load',
    json=payload
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")

if response.status_code == 200:
    print("\n✅ Schema loaded successfully!")
else:
    print("\n❌ Failed to load schema")
