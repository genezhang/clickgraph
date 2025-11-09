import requests

# Test standalone OPTIONAL MATCH (no prior MATCH)
query = """
OPTIONAL MATCH (a:User)-[:FOLLOWS]->(b:User)
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
    print("\nLEFT JOIN found:", "LEFT JOIN" in result['generated_sql'])
    print("INNER JOIN found:", "INNER JOIN" in result['generated_sql'])
else:
    print("Error:", result)
