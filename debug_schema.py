import requests
import json

# Check what the social_network schema actually contains
r = requests.get('http://localhost:8080/schemas')
schemas = r.json()['schemas']

print("Available schemas:")
for schema in schemas:
    print(f"  - {schema['name']}: {schema['node_count']} nodes, {schema['relationship_count']} relationships")

# Try to query and see what happens
print("\nTesting social_network schema query...")
r = requests.post('http://localhost:8080/query', json={
    'query': 'MATCH (u:User) RETURN u LIMIT 1',
    'schema_name': 'social_network',
    'sql_only': True
})

print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(f"Generated SQL:\n{r.json()['generated_sql']}")
else:
    print(f"Error: {r.text}")
