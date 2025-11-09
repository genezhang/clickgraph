import requests

query = """
MATCH (a:User)
WHERE a.name = 'Eve'
OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
RETURN a.name, b.name
"""

resp = requests.post('http://localhost:8080/query', json={
    'query': query,
    'sql_only': True,
    'schema_name': 'test_graph_schema'
})

result = resp.json()
if 'generated_sql' in result:
    print("Generated SQL:")
    print(result['generated_sql'])
else:
    print("Error:", result)
