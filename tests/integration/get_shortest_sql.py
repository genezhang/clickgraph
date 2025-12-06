#!/usr/bin/env python3
"""Get the SQL for the shortest path query."""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

response = requests.post(
    f"{CLICKGRAPH_URL}/query",
    json={
        "query": """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]-(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Eve'
            RETURN a.name, b.name
        """,
        "schema_name": "test_graph_schema",
        "sql_only": True
    }
)

print(f"Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"\nGenerated SQL:")
    print(data.get('generated_sql', 'NO SQL'))
else:
    print(f"Error: {response.text}")
