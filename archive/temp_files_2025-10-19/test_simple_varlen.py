"""Simple test without WHERE clause"""
import requests

query = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN a.name, b.name"

resp = requests.post('http://localhost:8080/query', json={'query': query}, timeout=5)

print("Status:", resp.status_code)
if resp.status_code == 200:
    result = resp.json()
    print("Results:", result)
else:
    print("Error:", resp.text)
