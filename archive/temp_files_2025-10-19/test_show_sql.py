"""Test to see the generated SQL"""
import requests
import json

query = "MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User)) WHERE a.name = 'Alice Johnson' RETURN a.name, b.name"

resp = requests.post('http://localhost:8080/query', 
                    json={'query': query, 'include_sql': True},
                    timeout=5)

print("Status:", resp.status_code)
print("\nResponse text:")
print(resp.text)
