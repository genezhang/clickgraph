#!/usr/bin/env python3
"""Test the simplest possible query to see if basic JOINs work"""
import requests
import json

query = """
MATCH (u:User)-[:FOLLOWS]->(target:User)
WHERE u.name = 'Alice Johnson'
RETURN u.name, target.name
"""

print("Testing simple MATCH query...")
print(f"Query: {query}")

response = requests.post(
    "http://localhost:8080/query",
    json={"query": query},
    headers={"Content-Type": "application/json"}
)

print(f"\nStatus: {response.status_code}")

if response.status_code == 200:
    result = response.json()
    print("[OK] Query succeeded!")
    print(f"Results: {json.dumps(result, indent=2)[:500]}")
else:
    print(f"[FAIL] Query failed")
    print(f"Error: {response.text[:500]}")
