#!/usr/bin/env python3
"""Test relationship query SQL generation after graph_context fix."""

import requests
import json

BASE_URL = "http://localhost:8080/query"

test_cases = [
    {
        "name": "Simple relationship query",
        "query": "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN a.name, b.name"
    },
    {
        "name": "Reverse direction",
        "query": "MATCH (a:User)<-[r:FOLLOWS]-(b:User) RETURN a.name, b.name"
    },
    {
        "name": "Count relationships",
        "query": "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN count(r) as total"
    },
    {
        "name": "With WHERE on node property",
        "query": "MATCH (a:User)-[r:FOLLOWS]->(b:User) WHERE a.email = 'alice@example.com' RETURN a.name, b.name"
    },
    {
        "name": "Multiple patterns",
        "query": "MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User) RETURN a.name, b.name, c.name"
    },
    {
        "name": "Undirected relationship",
        "query": "MATCH (a:User)-[r:FOLLOWS]-(b:User) RETURN a.name, b.name"
    },
]

def test_query(test_case):
    """Test a single query."""
    print(f"\n{'='*70}")
    print(f"TEST: {test_case['name']}")
    print(f"{'='*70}")
    print(f"Query: {test_case['query']}")
    
    try:
        response = requests.post(
            BASE_URL,
            json={"query": test_case["query"], "sql_only": True},
            timeout=5
        )
        
        if response.ok:
            result = response.json()
            sql = result.get("generated_sql", "")
            
            # Check if SQL contains correct table name
            if "user_follows" in sql:
                print("✓ PASS: Uses correct table 'user_follows'")
            else:
                print("✗ FAIL: Does not use 'user_follows' table")
            
            # Check if SQL contains invalid patterns
            if "FOLLOWS_r" in sql or "FOLLOWS_" in sql.replace("user_follows", ""):
                print("✗ FAIL: Contains invalid 'FOLLOWS_*' pattern")
            else:
                print("✓ PASS: No invalid patterns")
            
            print(f"\nGenerated SQL:")
            print(sql)
        else:
            print(f"✗ ERROR: HTTP {response.status_code}")
            print(response.text)
    
    except Exception as e:
        print(f"✗ EXCEPTION: {e}")

def main():
    """Run all tests."""
    print("Testing Relationship Query SQL Generation")
    print("After fix: Use table names from schema instead of label+alias")
    
    passed = 0
    failed = 0
    
    for test_case in test_cases:
        try:
            test_query(test_case)
            passed += 1
        except Exception as e:
            print(f"✗ Test failed with exception: {e}")
            failed += 1
    
    print(f"\n{'='*70}")
    print(f"SUMMARY: {passed} tests completed, {failed} failed")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
