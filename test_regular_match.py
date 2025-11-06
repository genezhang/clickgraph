#!/usr/bin/env python3
"""
Test if regular MATCH (without OPTIONAL) works for relationship queries
"""

import requests
import json

SERVER_URL = "http://localhost:8080"

def test_query(cypher, description):
    print("=" * 60)
    print(f"Test: {description}")
    print(f"\nCypher:\n{cypher}\n")
    
    try:
        response = requests.post(
            f"{SERVER_URL}/query",
            json={"query": cypher},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Success!")
            print(f"Rows: {len(result.get('data', []))}")
            print(f"Data: {json.dumps(result.get('data', []), indent=2)}")
            return True
        else:
            print(f"❌ Failed!")
            print(f"Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def main():
    print("\n" + "=" * 60)
    print("REGULAR MATCH (without OPTIONAL) TEST")
    print("Testing if relationship queries work at all")
    print("=" * 60 + "\n")
    
    # Test 1: Simple node query (should work)
    test_query(
        "MATCH (a:User) WHERE a.name = 'Alice' RETURN a.name, a.age",
        "Simple node query (baseline)"
    )
    
    # Test 2: Regular MATCH with relationship
    test_query(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN a.name, b.name",
        "Regular MATCH with relationship"
    )
    
    # Test 3: Same query but OPTIONAL MATCH
    test_query(
        "MATCH (a:User) WHERE a.name = 'Alice' OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.name, b.name",
        "OPTIONAL MATCH with relationship"
    )
    
    # Test 4: Regular MATCH without WHERE on start node
    test_query(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name LIMIT 5",
        "Regular MATCH all relationships (no WHERE)"
    )

if __name__ == "__main__":
    main()
