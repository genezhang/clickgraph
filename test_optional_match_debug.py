import requests
import json

# Test that OPTIONAL MATCH is being parsed
query1 = """
MATCH (a:User)
WHERE a.name = 'Alice'
RETURN a.name, a.age
"""

query2 = """
MATCH (a:User)
WHERE a.name = 'Alice'
OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
RETURN a.name, b.name
"""

print("=" * 60)
print("Query 1 (MATCH only):")
print(query1)
resp1 = requests.post('http://localhost:8080/query', json={
    'schema_name': 'test_graph_schema',
    'query': query1
})
print(f"Status: {resp1.status_code}")
if resp1.status_code == 200:
    print(f"Results: {json.dumps(resp1.json(), indent=2)}")
else:
    print(f"Error: {resp1.text}")

print("\n" + "=" * 60)
print("Query 2 (MATCH + OPTIONAL MATCH):")
print(query2)
resp2 = requests.post('http://localhost:8080/query', json={
    'schema_name': 'test_graph_schema',
    'query': query2
})
print(f"Status: {resp2.status_code}")
if resp2.status_code == 200:
    result = resp2.json()
    print(f"Results: {json.dumps(result, indent=2)}")
    # Check if b.name is in results
    if 'results' in result and len(result['results']) > 0:
        first_row = result['results'][0]
        if 'b.name' in first_row or (isinstance(first_row, list) and len(first_row) > 1):
            print("✅ b.name IS in results!")
        else:
            print(f"❌ b.name NOT in results. Keys: {first_row.keys() if isinstance(first_row, dict) else 'list format'}")
else:
    print(f"Error: {resp2.text}")
