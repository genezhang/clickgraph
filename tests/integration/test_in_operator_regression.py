#!/usr/bin/env python3
"""
Regression test suite for IN operator fix.

This test validates the fix for three related bugs:
1. Double-wrapping of IN operator lists: IN (('Alice', 'Bob')) 
2. Missing List handler in render_expr_to_sql_string
3. LIMIT 1 instead of window function for multiple start nodes

The fix ensures:
- Correct SQL generation: IN ('Alice', 'Bob')
- Window function: PARTITION BY start_id 
- Returns shortest path from EACH start node
"""

import requests
import sys

def test_query(name, query, expected_rows, expected_contains=None):
    """Helper to test a query."""
    print(f"\n{'='*60}")
    print(f"TEST: {name}")
    print(f"{'='*60}")
    
    response = requests.post('http://localhost:8080/query', json={
        'query': query,
        'schema_name': 'test_graph_schema'
    })
    
    if response.status_code != 200:
        print(f"FAIL: Status {response.status_code}")
        print(f"Error: {response.text[:200]}")
        return False
    
    data = response.json()
    results = data.get('results', [])
    print(f"Results: {len(results)} rows")
    for row in results:
        print(f"  {row}")
    
    if len(results) != expected_rows:
        print(f"FAIL: Expected {expected_rows} rows, got {len(results)}")
        return False
    
    if expected_contains:
        for expected in expected_contains:
            if expected not in str(results):
                print(f"FAIL: Expected to find '{expected}' in results")
                return False
    
    print(f"PASS")
    return True

def main():
    """Run all regression tests."""
    all_pass = True
    
    # Test 1: Original bug case - IN operator with multiple start nodes
    all_pass &= test_query(
        "IN operator with multiple start nodes (original bug)",
        "MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name IN ['Alice', 'Bob'] AND b.name = 'Eve' RETURN a.name, b.name",
        expected_rows=2,
        expected_contains=['Alice', 'Bob', 'Eve']
    )
    
    # Test 2: IN operator with single value
    all_pass &= test_query(
        "IN operator with single value",
        "MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name IN ['Alice'] AND b.name = 'Eve' RETURN a.name, b.name",
        expected_rows=1,
        expected_contains=['Alice', 'Eve']
    )
    
    # Test 3: IN operator with many values
    all_pass &= test_query(
        "IN operator with many values",
        "MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name IN ['Alice', 'Bob', 'Charlie'] AND b.name = 'Eve' RETURN a.name, b.name",
        expected_rows=3,
        expected_contains=['Alice', 'Bob', 'Charlie']
    )
    
    # Test 4: Regular shortest path without IN (baseline)
    all_pass &= test_query(
        "Shortest path without IN operator (baseline)",
        "MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice' AND b.name = 'Eve' RETURN a.name, b.name",
        expected_rows=1,
        expected_contains=['Alice', 'Eve']
    )
    
    # Test 5: IN operator on end node
    all_pass &= test_query(
        "IN operator on target nodes",
        "MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice' AND b.name IN ['Diana', 'Eve'] RETURN a.name, b.name",
        expected_rows=1,
        expected_contains=['Diana']
    )
    
    # Test 6: Variable-length path with IN (non-shortest path)
    all_pass &= test_query(
        "Variable-length path with IN (no shortest path)",
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name IN ['Alice', 'Bob'] RETURN DISTINCT a.name, b.name ORDER BY a.name, b.name LIMIT 5",
        expected_rows=4
    )
    
    print(f"\n{'='*60}")
    if all_pass:
        print(f"SUCCESS: All 6 regression tests passed")
    else:
        print(f"FAILURE: Some regression tests failed")
    print(f"{'='*60}")
    
    return 0 if all_pass else 1

if __name__ == '__main__':
    sys.exit(main())
