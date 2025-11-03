import requests
import json

# Test SQL generation with sql_only mode
response = requests.post(
    'http://localhost:8081/query',
    json={
        'schema_name': 'test_graph_schema',
        'query': 'MATCH (u:User) WHERE u.name = "Alice" RETURN u.name',
        'sql_only': True
    }
)

print(f"Status: {response.status_code}")
if response.status_code == 200:
    result = response.json()
    print("\nGenerated SQL:")
    print(result.get('sql', 'No SQL found'))
else:
    print(f"Error: {response.text}")
