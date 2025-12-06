import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

# Get SQL only
response = requests.post(f'{CLICKGRAPH_URL}/query', json={
    'query': 'MATCH (a:User) WHERE a.name = "Alice" RETURN a.name',
    'schema_name': 'test_graph_schema',
    'sql_only': True
})

print("Status:", response.status_code)
print("SQL Generated:")
print(response.text)
