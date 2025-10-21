#!/usr/bin/env python3
"""Test WHERE clauses and capture SQL output"""
import requests

query = "MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User)) WHERE a.name = 'Alice Johnson' AND b.name = 'Frank Wilson' RETURN a.name, b.name"

try:
    response = requests.post("http://localhost:8080/query", json={"query": query}, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Response type: {type(data)}")
        print(f"Results: {data}")
        print(f"Row count: {len(data) if isinstance(data, list) else 'N/A'}")
    else:
        print(f"Error: {response.text}")
except Exception as e:
    print(f"Connection error: {e}")
