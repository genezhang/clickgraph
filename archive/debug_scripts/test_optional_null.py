import requests

# Test the failing case - Eve doesn't follow anyone
query = """
MATCH (a:User)
WHERE a.name = 'Eve'
OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
RETURN a.name, b.name
"""

response = requests.post(
    "http://localhost:8080/query",
    json={"query": query, "schema_name": "test_graph_schema", "sql_only": True}
)

if response.status_code == 200:
    result = response.json()
    print("Generated SQL:")
    print("=" * 80)
    print(result.get('generated_sql', ''))
    print("=" * 80)
else:
    print(f"Error {response.status_code}: {response.text}")

# Now execute it
response2 = requests.post(
    "http://localhost:8080/query",
    json={"query": query, "schema_name": "test_graph_schema"}
)

if response2.status_code == 200:
    result2 = response2.json()
    print("\nQuery Results:")
    print(result2.get('results', []))
else:
    print(f"Error {response2.status_code}: {response2.text}")
