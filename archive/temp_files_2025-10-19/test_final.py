import requests
import json

# Test basic path query that should work
query = "MATCH p = (a:User)-[:FOLLOWS*1..2]-(b:User) RETURN p, length(p) LIMIT 5"
r = requests.post('http://localhost:8080/query', json={'query': query})

print(f"Status: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    print(f"Results: {len(data)} rows")
    print(json.dumps(data[0], indent=2) if data else "No data")
else:
    print(f"Error: {r.text}")
