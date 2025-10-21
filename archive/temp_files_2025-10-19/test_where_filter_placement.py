#!/usr/bin/env python3
"""Test WHERE filter placement in shortestPath queries"""

import requests
import json
import re

def test_query_with_sql_logging(query, description):
    print(f"\n{'='*70}")
    print(f"Testing: {description}")
    print(f"{'='*70}")
    print(f"Query: {query.strip()}\n")
    
    try:
        # Enable debug logging to see generated SQL
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": query},
            timeout=10
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"[OK] Success! Rows returned: {len(result)}")
            if result:
                print(f"Sample data: {json.dumps(result[0], indent=2)}")
        else:
            print(f"[ERROR] Error: {response.text}")
            
    except Exception as e:
        print(f"[ERROR] Exception: {e}")

if __name__ == "__main__":
    print("="*70)
    print("WHERE FILTER PLACEMENT TEST")
    print("="*70)
    
    # Test 1: Filter on start node only
    test_query_with_sql_logging(
        """
        MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
        WHERE a.name = 'Alice Johnson'
        RETURN a.name as start, b.name as end, length(p) as hops
        LIMIT 3
        """,
        "Start node filter only (should be in CTE base case)"
    )
    
    # Test 2: Filter on end node only
    test_query_with_sql_logging(
        """
        MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
        WHERE b.name = 'Bob Smith'
        RETURN a.name as start, b.name as end, length(p) as hops
        LIMIT 3
        """,
        "End node filter only (should be in final SELECT)"
    )
    
    # Test 3: Filter on both start and end nodes
    test_query_with_sql_logging(
        """
        MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
        WHERE a.name = 'Alice Johnson' AND b.name = 'Charlie Brown'
        RETURN a.name as start, b.name as end, length(p) as hops
        """,
        "Both start and end filters (split placement)"
    )
    
    # Test 4: Filter on relationship property (if supported)
    test_query_with_sql_logging(
        """
        MATCH p = shortestPath((a:User)-[r:FOLLOWS*]->(b:User))
        WHERE a.name = 'Alice Johnson'
        RETURN a.name as start, b.name as end, length(p) as hops
        LIMIT 3
        """,
        "Start filter with relationship variable"
    )
    
    print("\n" + "="*70)
    print("Check the server logs (baseline_out.log) for generated SQL!")
    print("Look for WHERE clause placement in:")
    print("  1. CTE base case (first SELECT in WITH RECURSIVE)")
    print("  2. CTE recursive case (UNION ALL part)")
    print("  3. Final SELECT (outer query)")
    print("="*70)
