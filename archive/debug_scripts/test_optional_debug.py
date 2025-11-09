import requests

query = """
MATCH (a:User) WHERE a.name='Alice'
OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
RETURN a.name, b.name
ORDER BY b.name
"""

response = requests.post('http://localhost:8080/query', json={
    'schema_name': 'test_graph_schema',
    'query': query
})

print("Status:", response.status_code)
print("Response:", response.json())
