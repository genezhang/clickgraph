import requests
import json

# Test 1: Simple node match
query1 = "MATCH (a:User) WHERE a.name = 'Alice Johnson' RETURN a.name LIMIT 5"
r1 = requests.post('http://localhost:8080/query', json={'query': query1})
print(f"Test 1 - Simple match: {r1.status_code}")
if r1.status_code == 200:
    print(json.dumps(r1.json(), indent=2))
else:
    print(f"Error: {r1.text}")
print()

# Test 2: Path variable query
query2 = "MATCH p = (a:User)-[:FOLLOWS*1..2]-(b:User) WHERE a.name = 'Alice Johnson' RETURN p LIMIT 3"
r2 = requests.post('http://localhost:8080/query', json={'query': query2})
print(f"Test 2 - Path variable: {r2.status_code}")
if r2.status_code == 200:
    print(json.dumps(r2.json(), indent=2))
else:
    print(f"Error: {r2.text}")
