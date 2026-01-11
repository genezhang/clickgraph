#!/usr/bin/env python3
"""Quick script to see generated SQL for a Cypher query"""
import sys
import requests

if len(sys.argv) < 2:
    print("Usage: get_sql.py '<cypher query>'")
    sys.exit(1)

query = sys.argv[1]

# POST to /query with sql_only flag
response = requests.post(
    'http://localhost:8080/query',
    json={"query": query},
    headers={'X-SQL-Only': 'true'}  # Try custom header
)

print(f"Status: {response.status_code}")
print(response.text)
