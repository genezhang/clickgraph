import requests
import json

# Test simple relationship query
query = 'MATCH (u1:User)-[:FOLLOWS]->(u2:User) RETURN u1.name, u2.name'
url = 'http://localhost:8081/query'
headers = {'Content-Type': 'application/json'}
data = {'query': query, 'sql_only': True}

print('Testing simple relationship query...')
print(f'Query: {query}')

try:
    response = requests.post(url, headers=headers, json=data, timeout=10)
    print(f'Status: {response.status_code}')
    print(f'Raw response: {response.text}')
    if response.status_code == 200:
        result = response.json()
        print('Full response:')
        print(json.dumps(result, indent=2))
    else:
        print(f'Error: {response.text}')
except Exception as e:
    print(f'Exception: {e}')