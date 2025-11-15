#!/usr/bin/env python3
"""Test undirected relationship SQL generation"""
import requests

query = "MATCH (a:User)-[r:FOLLOWS]-(b:User) WHERE a.name = 'Bob' RETURN b.name ORDER BY b.name"

response = requests.post('http://localhost:8080/query', json={
    'query': query,
    'schema_name': 'test_graph_schema',
    'sql_only': True
})

print("Generated SQL:")
print("=" * 80)
if response.status_code == 200:
    result = response.json()
    if 'sql' in result:
        print(result['sql'])
    else:
        print(result)
else:
    print(f"Error {response.status_code}: {response.text}")
