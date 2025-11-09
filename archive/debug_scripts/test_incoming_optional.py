import requests

query = """
MATCH (a:User)
WHERE a.name = 'Alice'
OPTIONAL MATCH (b:User)-[:FOLLOWS]->(a)
RETURN a.name, b.name
"""

response = requests.post(
    "http://localhost:8080/query",
    json={"query": query, "sql_only": True},
    headers={"X-Graph-Schema": "test_graph_schema"}
)

print("Status:", response.status_code)
print("\n=== Response ===")
print(response.json())
