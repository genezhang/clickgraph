import requests
import json

response = requests.post(
    'http://localhost:8080/query',
    json={
        'schema_name': 'test_graph_schema',
        'query': 'MATCH (u:User) WHERE u.name = "Alice" RETURN u.name, u.age'
    }
)

print(f"Status: {response.status_code}")
print(f"Response type: {type(response.json())}")
print(f"Response: {json.dumps(response.json(), indent=2)}")

if isinstance(response.json(), list) and len(response.json()) > 0:
    print(f"\nFirst row keys: {response.json()[0].keys()}")
