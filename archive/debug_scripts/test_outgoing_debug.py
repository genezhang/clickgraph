#!/usr/bin/env python3
"""Debug outgoing relationship with source filter."""

import requests
import json

query = """
MATCH (a:User)-[r:FOLLOWS]->(b:User) 
WHERE a.name = 'Alice' 
RETURN b.name 
ORDER BY b.name
"""

try:
    response = requests.post(
        "http://localhost:8080/query",
        json={"query": query, "schema_name": "test_graph_schema"},
        headers={"Content-Type": "application/json"},
        timeout=10
    )

    print("Status:", response.status_code)
    
    if response.status_code == 200:
        data = response.json()
        print("\nResponse JSON:")
        print(json.dumps(data, indent=2))
        
        results = data.get("results", [])
        print(f"\n✅ Got {len(results)} rows")
        print("Expected: 2 rows (Bob, Charlie)")
        
    else:
        print(f"\n❌ Error: {response.status_code}")
        print(response.text[:500])
        
except Exception as e:
    print(f"❌ Error: {e}")
