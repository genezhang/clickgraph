import requests

# Get SQL only
response = requests.post('http://localhost:8080/query', json={
    'query': 'MATCH (a:User) WHERE a.name = "Alice" RETURN a.name',
    'schema_name': 'test_graph_schema',
    'sql_only': True
})

print("Status:", response.status_code)
print("SQL Generated:")
print(response.text)
