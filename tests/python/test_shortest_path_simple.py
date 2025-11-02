import requests
import json

# Test shortest path query
query = {'query': 'MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = "Alice Johnson" AND b.name = "Bob Smith" RETURN p'}
response = requests.post('http://localhost:8080/query', json=query)
print('Shortest path query test:')
print('Status:', response.status_code)
if response.status_code == 200:
    print('Results:', json.dumps(response.json(), indent=2))
else:
    print('Error:', response.text[:500])