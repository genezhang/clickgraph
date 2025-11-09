import requests
import json

# Load schema
with open("tests/integration/test_integration.yaml", "r") as f:
    schema_yaml = f.read()

print("Loading schema...")
response = requests.post(
    "http://localhost:8080/schemas/load",
    headers={"Content-Type": "application/x-yaml"},
    data=schema_yaml
)
print(f"Schema load: {response.status_code}")
if response.status_code != 200:
    print(response.text)
    exit(1)

# Run query
query = """
MATCH (a:User)
WHERE a.name = 'Eve'
OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
RETURN a.name, b.name
"""

print(f"\nRunning query: {query}")
response = requests.post(
    "http://localhost:8080/query",
    json={"query": query, "schema_name": "test_graph_schema"}
)

if response.status_code == 200:
    result = response.json()
    print("\nSQL Generated:")
    if "sql" in result:
        print(result["sql"])
    print("\nResults:")
    print(json.dumps(result.get("results"), indent=2))
else:
    print(f"Error: {response.status_code}")
    print(response.text)
