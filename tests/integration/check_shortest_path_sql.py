import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

# Test the basic shortest path query
response = requests.post(f'{CLICKGRAPH_URL}/query', json={
    'query': '''
        MATCH path = shortestPath((a:User)-[:FOLLOWS*]-(b:User))
        WHERE a.name = "Alice" AND b.name = "Eve"
        RETURN a.name, b.name
    ''',
    'schema_name': 'test_graph_schema',
    'sql_only': True
})

print("Status:", response.status_code)
data = response.json()
print("\nGenerated SQL:")
print(data['generated_sql'])
