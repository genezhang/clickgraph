#!/usr/bin/env python3
"""Quick test for multiple relationship types"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

def test_query(query_str, description):
    """Send query and print result"""
    print(f"\n{description}")
    print("=" * 80)
    print(f"Query:\n{query_str}\n")
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": query_str}
    )
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"Result: {json.dumps(result, indent=2)}")
    else:
        print(f"Error: {response.text}")
    
    return response

# Test multiple relationship types
query = """
MATCH (u:User)-[:FOLLOWS|FRIENDS_WITH]->(target:User)
RETURN u.name, target.name
LIMIT 5
"""

test_query(query, "Testing Multiple Relationship Types ([:FOLLOWS|FRIENDS_WITH])")
