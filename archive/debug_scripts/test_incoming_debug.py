#!/usr/bin/env python3
"""Debug script for incoming relationship query issue."""

import requests
import json

# Query that's failing
query = """
MATCH (a:User)<-[r:FOLLOWS]-(b:User) 
WHERE a.name = 'Charlie' 
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
    print("\nResponse Text:")
    print(response.text[:500])  # First 500 chars
    
    if response.status_code == 200:
        data = response.json()
        print("\nResponse JSON:")
        print(json.dumps(data, indent=2))
        
        results = data.get("results", [])
        print(f"\n✅ Got {len(results)} rows")
        print("Expected: 2 rows (Alice, Bob)")
        print("\nRows:")
        for i, row in enumerate(results, 1):
            print(f"  {i}. {row}")
    else:
        print(f"\n❌ Error response")
        
except Exception as e:
    print(f"❌ Error: {e}")
