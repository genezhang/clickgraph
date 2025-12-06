#!/usr/bin/env python3
"""Test simple query to debug schema issues."""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

# Shortest path query
response = requests.post(
    f"{CLICKGRAPH_URL}/query",
    json={
        "query": """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]-(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Eve'
            RETURN a.name, b.name
        """,
        "schema_name": "test_graph_schema"
    }
)

print(f"Status: {response.status_code}")
if response.status_code == 200:
    print(f"Response: {json.dumps(response.json(), indent=2)}")
else:
    print(f"Error: {response.text}")
