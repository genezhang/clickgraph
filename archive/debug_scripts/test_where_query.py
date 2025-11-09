import requests
import json

# Test WHERE query
query = 'MATCH (u:User) WHERE u.name = "Alice" RETURN u.name'
response = requests.post('http://localhost:8080/query', json={
    'query': query,
    'schema_name': 'test_graph_schema'
})

print(f"Status: {response.status_code}")
if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
else:
    print(f"Error: {response.text}")
