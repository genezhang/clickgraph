#!/usr/bin/env python3
"""Test multiple relationship types with schema-only approach."""

import requests
import json

# Test query with single relationship first (to verify ViewScan fix works)
query = """
MATCH (u:User)-[:FOLLOWS]->(target:User)
RETURN u.name, target.name
LIMIT 5
"""

print("Testing Single Relationship Type ([:FOLLOWS])")
print("=" * 80)
print(f"Query: {query}")
print()

try:
    response = requests.post(
        "http://localhost:8080/query",
        json={"query": query, "sql_only": True},  # Get SQL to debug
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        if isinstance(result, list):
            print(f"[OK] SUCCESS! Returned {len(result)} results")
            if len(result) > 0:
                print(f"Sample result: {json.dumps(result[0], indent=2)}")
        else:
            print(f"Result type: {type(result)}")
            print(f"Result: {json.dumps(result, indent=2)[:500]}")
    else:
        error = response.json() if response.headers.get('content-type') == 'application/json' else response.text
        print(f"[FAIL] FAILED")
        print(f"Error: {error}")
        
except Exception as e:
    print(f"[FAIL] EXCEPTION: {str(e)}")
    import traceback
    traceback.print_exc()
