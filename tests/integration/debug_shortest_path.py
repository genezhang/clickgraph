import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

# Test basic single-hop query first
print("=== Testing single-hop FOLLOWS from Alice ===")
r = requests.post(f'{CLICKGRAPH_URL}/query', json={
    'query': 'MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = "Alice" RETURN a.name, b.name',
    'schema_name': 'test_graph_schema'
})
print(f"Status: {r.status_code}")
try:
    print(f"Response: {r.json()}")
except:
    print(f"Raw response: {r.text}")
print()

# Test if Alice exists
print("=== Testing if Alice exists ===")
r = requests.post(f'{CLICKGRAPH_URL}/query', json={
    'query': 'MATCH (a:User) WHERE a.name = "Alice" RETURN a.name',
    'schema_name': 'test_graph_schema'
})
print(f"Status: {r.status_code}")
try:
    print(f"Response: {r.json()}")
except:
    print(f"Raw response: {r.text}")
print()

# Test if Eve exists
print("=== Testing if Eve exists ===")
r = requests.post(f'{CLICKGRAPH_URL}/query', json={
    'query': 'MATCH (a:User) WHERE a.name = "Eve" RETURN a.name',
    'schema_name': 'test_graph_schema'
})
print(f"Status: {r.status_code}")
try:
    print(f"Response: {r.json()}")
except:
    print(f"Raw response: {r.text}")
print()

# Test shortest path
print("=== Testing shortestPath from Alice to Eve ===")
r = requests.post(f'{CLICKGRAPH_URL}/query', json={
    'query': 'MATCH path = shortestPath((a:User)-[:FOLLOWS*]-(b:User)) WHERE a.name = "Alice" AND b.name = "Eve" RETURN a.name, b.name',
    'schema_name': 'test_graph_schema'
})
print(f"Status: {r.status_code}")
try:
    print(f"Response: {r.json()}")
except:
    print(f"Raw response: {r.text}")
