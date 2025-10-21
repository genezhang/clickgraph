#!/usr/bin/env python3
"""Test that property mappings are correctly loaded and used in CTEs"""

import requests
import json

def test_query(query, description):
    print(f"\n{'='*60}")
    print(f"Test: {description}")
    print(f"{'='*60}")
    print(f"Query: {query}")
    
    response = requests.post(
        "http://localhost:8080/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Success")
        print(f"Results: {json.dumps(result, indent=2)}")
        return True
    else:
        print(f"✗ Failed")
        print(f"Error: {response.text}")
        return False

if __name__ == "__main__":
    print("Testing Property Mappings in Variable-Length Paths\n")
    
    # Test 1: Simple variable-length path with property selection
    test_query(
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN a.name, b.name LIMIT 5",
        "Variable-length path with property selection"
    )
    
    # Test 2: WHERE clause filtering (the original failing case)
    test_query(
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice' RETURN a.name, b.name",
        "Variable-length path with WHERE clause on properties"
    )
    
    # Test 3: Both nodes filtered
    test_query(
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice' AND b.name = 'Charlie' RETURN a.name, b.name",
        "Variable-length path with WHERE on both nodes"
    )
    
    print("\n" + "="*60)
    print("Testing complete!")
