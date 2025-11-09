#!/usr/bin/env python3
"""
Test if two separate MATCH clauses work (like OPTIONAL MATCH pattern)
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
            print(f"Data: {json.dumps(result.get('data', []), indent=2)[:500]}")
            return True
        else:
            print(f"❌ Failed!")
            error_text = response.text
            # Extract just the SQL if present
            if "SELECT" in error_text:
                start = error_text.find("SELECT")
                end = error_text.find(". (UNKNOWN_TABLE)")
                if end > start:
                    sql = error_text[start:end]
                    print(f"Generated SQL:\n{sql}\n")
            print(f"Full Error: {error_text[:500]}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def main():
    print("\n" + "=" * 60)
    print("TWO SEPARATE MATCH CLAUSES TEST")
    print("Testing the OPTIONAL MATCH pattern structure")
    print("=" * 60 + "\n")
    
    # Test 1: Combined MATCH (should work - we saw this works)
    test_query(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN a.name, b.name",
        "Combined MATCH with relationship and WHERE"
    )
    
    # Test 2: Two separate MATCHes (like OPTIONAL MATCH structure)
    test_query(
        "MATCH (a:User) WHERE a.name = 'Alice' MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.name, b.name",
        "Two separate MATCH clauses (OPTIONAL MATCH pattern)"
    )
    
    # Test 3: OPTIONAL MATCH (currently broken)
    test_query(
        "MATCH (a:User) WHERE a.name = 'Alice' OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.name, b.name",
        "OPTIONAL MATCH (currently broken)"
    )

if __name__ == "__main__":
    main()
