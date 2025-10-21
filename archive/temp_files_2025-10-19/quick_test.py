import requests

r = requests.post(
    'http://localhost:8080/query',
    json={'query': "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice Johnson' RETURN a.name"}
)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(f"Results: {r.json()}")
