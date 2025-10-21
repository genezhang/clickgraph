#!/usr/bin/env python3
"""Quick SQL-only test to see generated SQL for WHERE filter queries"""

import requests
import json

def test_sql_only(query, description):
    print(f"\n{'='*70}")
    print(f"{description}")
    print(f"{'='*70}")
    print(f"Query: {query}\n")
    
    response = requests.post(
        "http://localhost:8080/query",
        json={"query": query, "sql_only": True},
        timeout=10
    )
    
    if response.status_code == 200:
        result = response.json()
        print("Generated SQL:")
        print(result.get('generated_sql', 'NO SQL'))
        print()
    else:
        print(f"Error: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    # Test 1: Start node filter only
    test_sql_only(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice Johnson' RETURN a.name, b.name",
        "TEST 1: Start node filter (should be in CTE base case)"
    )
    
    # Test 2: End node filter only  
    test_sql_only(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE b.name = 'Bob Smith' RETURN a.name, b.name",
        "TEST 2: End node filter (should be in CTE outer SELECT)"
    )
    
    # Test 3: Both filters
    test_sql_only(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice Johnson' AND b.name = 'Charlie Brown' RETURN a.name, b.name",
        "TEST 3: Both filters (should be split)"
    )
