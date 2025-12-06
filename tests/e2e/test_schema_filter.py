#!/usr/bin/env python3
"""
Test schema-level filter injection into generated SQL.

This test validates that filters defined in the schema YAML are properly
injected into the WHERE clause of generated SQL queries.
"""

import requests
import json
import sys
import os

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
ENDPOINT = f"{CLICKGRAPH_URL}/query"
HEADERS = {"Content-Type": "application/json"}

def query_sql_only(cypher: str, schema_name: str = None):
    """Execute a query in sql_only mode and return the generated SQL."""
    payload = {"query": cypher, "sql_only": True}
    if schema_name:
        payload["schema_name"] = schema_name
    
    resp = requests.post(ENDPOINT, headers=HEADERS, json=payload)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        print(resp.text)
        return None
    
    data = resp.json()
    return data.get("sql") or data.get("data", [[""]])[0][0]

def test_node_schema_filter():
    """Test that node-level schema filter appears in generated SQL."""
    print("\n=== Test 1: Node-level schema filter ===")
    
    cypher = "MATCH (u:User) RETURN u.name LIMIT 10"
    sql = query_sql_only(cypher, "filter_test")
    
    if sql is None:
        print("FAIL: Could not get SQL")
        return False
    
    print(f"Cypher: {cypher}")
    print(f"SQL:\n{sql}")
    
    # Check if the filter appears in the SQL
    if "is_active = 1" in sql or "is_active=1" in sql:
        print("PASS: Node schema filter found in SQL")
        return True
    else:
        print("FAIL: Node schema filter NOT found in SQL")
        return False

def test_relationship_schema_filter():
    """Test that relationship-level schema filter appears in generated SQL."""
    print("\n=== Test 2: Relationship-level schema filter ===")
    
    cypher = "MATCH (u1:User)-[r:FOLLOWS]->(u2:User) RETURN u1.name, u2.name LIMIT 10"
    sql = query_sql_only(cypher, "filter_test")
    
    if sql is None:
        print("FAIL: Could not get SQL")
        return False
    
    print(f"Cypher: {cypher}")
    print(f"SQL:\n{sql}")
    
    # Check for relationship filter
    if "toYear(follow_date)" in sql or "follow_date" in sql:
        print("PASS: Relationship schema filter found in SQL")
        return True
    else:
        print("FAIL: Relationship schema filter NOT found in SQL")
        return False

def test_combined_filters():
    """Test that both node and WHERE clause filters combine correctly."""
    print("\n=== Test 3: Combined schema + WHERE clause filters ===")
    
    cypher = "MATCH (u:User) WHERE u.country = 'USA' RETURN u.name LIMIT 10"
    sql = query_sql_only(cypher, "filter_test")
    
    if sql is None:
        print("FAIL: Could not get SQL")
        return False
    
    print(f"Cypher: {cypher}")
    print(f"SQL:\n{sql}")
    
    # Check for both filters
    has_schema_filter = "is_active = 1" in sql or "is_active=1" in sql
    has_query_filter = "country" in sql and "USA" in sql
    
    if has_schema_filter and has_query_filter:
        print("PASS: Both schema filter and query filter found in SQL")
        return True
    else:
        if not has_schema_filter:
            print("FAIL: Schema filter NOT found")
        if not has_query_filter:
            print("FAIL: Query filter NOT found")
        return False

def main():
    print("Schema Filter E2E Test")
    print("=" * 50)
    
    # Check server health
    try:
        resp = requests.get("http://localhost:8080/health", timeout=2)
        if resp.status_code != 200:
            print("Server not responding correctly. Start with:")
            print("GRAPH_CONFIG_PATH=./tests/fixtures/schemas/filter_test.yaml cargo run")
            sys.exit(1)
    except requests.exceptions.ConnectionError:
        print("Server not running. Start with:")
        print("GRAPH_CONFIG_PATH=./tests/fixtures/schemas/filter_test.yaml cargo run")
        sys.exit(1)
    
    results = []
    results.append(("Node schema filter", test_node_schema_filter()))
    results.append(("Relationship schema filter", test_relationship_schema_filter()))
    results.append(("Combined filters", test_combined_filters()))
    
    print("\n" + "=" * 50)
    print("Summary:")
    passed = sum(1 for _, r in results if r)
    total = len(results)
    for name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {status}: {name}")
    print(f"\nTotal: {passed}/{total} passed")
    
    sys.exit(0 if passed == total else 1)

if __name__ == "__main__":
    main()
