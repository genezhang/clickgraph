import requests
import json

query = 'MATCH (n:User) RETURN n.name'
response = requests.post('http://localhost:8080/query', json={
    'query': query,
    'schema_name': 'test_graph_schema'
})

print(f"Status: {response.status_code}")
result = response.json()
print(f"Response type: {type(result)}")
print(f"Response structure:")
print(json.dumps(result, indent=2))

if isinstance(result, dict) and 'results' in result:
    print(f"\nResults type: {type(result['results'])}")
    if result['results']:
        print(f"First result: {result['results'][0]}")
        print(f"First result type: {type(result['results'][0])}")
