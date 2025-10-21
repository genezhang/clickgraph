"""Test with ID columns only (no property mapping needed)"""
import requests

query = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN a.user_id, b.user_id"

resp = requests.post('http://localhost:8080/query', json={'query': query}, timeout=5)

print("Status:", resp.status_code)
if resp.status_code == 200:
    result = resp.json()
    print("SUCCESS! Results:", result)
else:
    print("Error:", resp.text)
