#!/usr/bin/env python3
"""Get SQL for multi-relationship query to debug duplicate JOIN"""

import requests
import json

# Test query with multiple relationship types
query = """
MATCH (u:User)-[:FOLLOWS|FRIENDS_WITH]->(target:User)
RETURN u.name, target.name
LIMIT 5
"""

print(f"Testing query:\n{query}\n")

response = requests.post(
    "http://localhost:8080/query",
    json={
        "query": query,
        "sql_only": True
    }
)

print(f"Status Code: {response.status_code}\n")

if response.status_code == 200:
    result = response.json()
    sql = result.get('generated_sql', 'N/A')
    print("Generated SQL:")
    print("=" * 80)
    print(sql)
    print("=" * 80)
else:
    print(f"Error: {response.text}")
