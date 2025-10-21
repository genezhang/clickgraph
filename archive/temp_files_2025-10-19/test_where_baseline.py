#!/usr/bin/env python3
"""Test WHERE filter with shortestPath on baseline code"""

import requests
import json

# Test 1: Basic variable-length path (should work)
query1 = """
MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)
RETURN a.name as start_name, b.name as end_name
LIMIT 5
"""

# Test 2: Variable-length path WITH WHERE filter (the problematic one)
query2 = """
MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)
WHERE a.name = 'Alice Johnson'
RETURN a.name as start_name, b.name as end_name
"""

# Test 3: shortestPath (if implemented)
query3 = """
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
WHERE a.name = 'Alice Johnson' AND b.name = 'Charlie Brown'
RETURN a.name, b.name, length(p)
"""

def test_query(query, description):
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"{'='*60}")
    print(f"Query: {query.strip()}")
    
    try:
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": query},
            timeout=10
        )
        
        print(f"\nStatus: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Success! Rows returned: {len(result)}")
            print(f"Data: {json.dumps(result[:3], indent=2)}")  # First 3 rows
        else:
            print(f"Error Response: {response.text}")
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    print("Testing WHERE filters on baseline code...")
    
    test_query(query1, "Basic variable-length path (NO WHERE)")
    test_query(query2, "Variable-length path WITH WHERE filter")
    test_query(query3, "shortestPath with WHERE filter")
