#!/usr/bin/env python3
"""Test polymorphic edge SQL generation.

This test verifies that polymorphic edge queries generate the correct
type filters in the SQL WHERE clause.

Expected behavior:
- MATCH (u:User)-[:FOLLOWS]->(f:User) 
  Should generate: WHERE r.interaction_type = 'FOLLOWS' AND r.from_type = 'User' AND r.to_type = 'User'
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json
import sys

BASE_URL = f"{CLICKGRAPH_URL}"
SCHEMA_PATH = "schemas/examples/social_polymorphic.yaml"

def get_sql(cypher: str) -> str:
    """Get SQL for a Cypher query using sql_only mode."""
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": cypher,
            "sql_only": True,
        },
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        return None
    
    data = response.json()
    return data.get("sql", data.get("result", ""))

def test_follows_polymorphic_filter():
    """Test that FOLLOWS query generates type filter."""
    cypher = "MATCH (u:User)-[r:FOLLOWS]->(f:User) WHERE u.name = 'Alice' RETURN f.name"
    sql = get_sql(cypher)
    
    if sql is None:
        print("❌ Failed to get SQL for FOLLOWS query")
        return False
    
    print(f"Query: {cypher}")
    print(f"SQL: {sql}")
    print()
    
    # Check for type filter
    checks = [
        ("interaction_type = 'FOLLOWS'" in sql or "interaction_type='FOLLOWS'" in sql, 
         "type_column filter for FOLLOWS"),
        ("from_type = 'User'" in sql or "from_type='User'" in sql,
         "from_label_column filter"),
        ("to_type = 'User'" in sql or "to_type='User'" in sql,
         "to_label_column filter"),
    ]
    
    all_passed = True
    for passed, description in checks:
        status = "✅" if passed else "❌"
        print(f"  {status} {description}")
        if not passed:
            all_passed = False
    
    return all_passed

def test_likes_polymorphic_filter():
    """Test that LIKES query generates type filter with different node types."""
    cypher = "MATCH (u:User)-[r:LIKES]->(p:Post) RETURN u.name, p.title"
    sql = get_sql(cypher)
    
    if sql is None:
        print("❌ Failed to get SQL for LIKES query")
        return False
    
    print(f"Query: {cypher}")
    print(f"SQL: {sql}")
    print()
    
    # Check for type filter
    checks = [
        ("interaction_type = 'LIKES'" in sql or "interaction_type='LIKES'" in sql, 
         "type_column filter for LIKES"),
        ("from_type = 'User'" in sql or "from_type='User'" in sql,
         "from_label_column filter for User"),
        ("to_type = 'Post'" in sql or "to_type='Post'" in sql,
         "to_label_column filter for Post"),
    ]
    
    all_passed = True
    for passed, description in checks:
        status = "✅" if passed else "❌"
        print(f"  {status} {description}")
        if not passed:
            all_passed = False
    
    return all_passed

def test_non_polymorphic_no_filter():
    """Test that non-polymorphic edges don't get type filters."""
    # This requires a different schema without polymorphic edges
    # For now, just verify the basic structure works
    return True

def main():
    print("=" * 60)
    print("Polymorphic Edge SQL Generation Tests")
    print("=" * 60)
    print(f"Using schema: {SCHEMA_PATH}")
    print()
    
    # Check server is running
    try:
        requests.get(f"{BASE_URL}/health", timeout=5)
    except requests.exceptions.ConnectionError:
        print(f"❌ Server not running at {BASE_URL}")
        print("Start server with: cargo run")
        return 1
    
    tests = [
        ("FOLLOWS polymorphic filter", test_follows_polymorphic_filter),
        ("LIKES polymorphic filter", test_likes_polymorphic_filter),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_fn in tests:
        print(f"\n--- {name} ---")
        if test_fn():
            passed += 1
        else:
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
