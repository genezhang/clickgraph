#!/usr/bin/env python3
"""
Test script to verify WHERE filter placement in shortestPath queries.

Tests that:
1. Start node filters (a.name = 'Alice') appear in CTE base case
2. End node filters (b.name = 'David') appear in final SELECT WHERE clause
"""

import requests
import json

SERVER_URL = "http://localhost:8080/query"

def test_query(description, query, expected_in_sql=None, not_expected_in_sql=None):
    """Test a query and check if expected patterns appear in generated SQL"""
    print(f"\n{'='*80}")
    print(f"TEST: {description}")
    print(f"Query: {query}")
    print(f"{'='*80}")
    
    try:
        response = requests.post(
            SERVER_URL,
            json={"query": query, "sql_only": True},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code != 200:
            print(f"❌ ERROR: Server returned {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
        data = response.json()
        print(f"Response keys: {list(data.keys())}")
        print(f"Full response: {data}")
        sql = data.get("sql", data.get("generated_sql", ""))
        
        print(f"\nGenerated SQL:\n{sql}\n")
        
        # Check expectations
        success = True
        if expected_in_sql:
            for pattern in expected_in_sql:
                if pattern in sql:
                    print(f"[PASS] Found expected pattern: {pattern}")
                else:
                    print(f"[FAIL] MISSING expected pattern: {pattern}")
                    success = False
        
        if not_expected_in_sql:
            for pattern in not_expected_in_sql:
                if pattern not in sql:
                    print(f"[PASS] Correctly absent: {pattern}")
                else:
                    print(f"[FAIL] Found unexpected pattern: {pattern}")
                    success = False
        
        return success
        
    except Exception as e:
        print(f"[ERROR] EXCEPTION: {e}")
        return False

def main():
    print("=" * 80)
    print("WHERE FILTER PLACEMENT TEST SUITE")
    print("Testing that filters are properly placed in shortestPath CTEs")
    print("=" * 80)
    
    tests = [
        {
            "description": "Start node filter only (should appear in base case)",
            "query": "MATCH p = shortestPath((a:User)-[:FOLLOWS*1..3]->(b:User)) WHERE a.name = 'Alice Johnson' RETURN p",
            "expected": [
                "WHERE",  # Should have WHERE clause somewhere
                "Alice Johnson",  # Filter value should appear
            ],
        },
        {
            "description": "End node filter only (should appear in outer SELECT)",
            "query": "MATCH p = shortestPath((a:User)-[:FOLLOWS*1..3]->(b:User)) WHERE b.name = 'David Lee' RETURN p",
            "expected": [
                "WHERE",
                "David Lee",
            ],
        },
        {
            "description": "Both start and end node filters",
            "query": "MATCH p = shortestPath((a:User)-[:FOLLOWS*1..3]->(b:User)) WHERE a.name = 'Alice Johnson' AND b.name = 'David Lee' RETURN p",
            "expected": [
                "WHERE",
                "Alice Johnson",
                "David Lee",
            ],
        },
        {
            "description": "Variable length with filter (not shortestPath)",
            "query": "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice Johnson' RETURN b",
            "expected": [
                "WHERE",
                "Alice Johnson",
            ],
        },
    ]
    
    results = []
    for i, test in enumerate(tests, 1):
        print(f"\n\n{'#'*80}")
        print(f"# Test {i}/{len(tests)}")
        print(f"{'#'*80}")
        result = test_query(
            test["description"],
            test["query"],
            expected_in_sql=test.get("expected"),
            not_expected_in_sql=test.get("not_expected")
        )
        results.append((test["description"], result))
    
    # Print summary
    print(f"\n\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for desc, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"{status}: {desc}")
    
    print(f"\n{'='*80}")
    print(f"RESULT: {passed}/{total} tests passed ({100*passed//total}%)")
    print(f"{'='*80}")
    
    return passed == total

if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)
