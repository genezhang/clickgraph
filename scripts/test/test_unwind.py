#!/usr/bin/env python3
"""
Test UNWIND → ARRAY JOIN SQL generation.

This test uses a standalone schema and the sql_only endpoint to verify
that UNWIND clauses are correctly converted to ARRAY JOIN statements.
"""

import requests
import json
import sys

BASE_URL = "http://localhost:8080"

# Simple test schema with array-valued property
TEST_SCHEMA = {
    "version": 1,
    "name": "test_unwind",
    "nodes": {
        "Entity": {
            "table": "entities",
            "id": "id",
            "properties": {
                "name": "name",
                "tags": "tags"  # Array(String) column
            }
        }
    },
    "relationships": {}
}

def test_unwind_literal_list():
    """Test UNWIND with a literal list."""
    query = "UNWIND [1, 2, 3] AS x RETURN x"
    
    print(f"\n=== Test: UNWIND literal list ===")
    print(f"Query: {query}")
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "sql_only": True,
            "schema_name": "test_unwind"
        },
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code == 200:
        sql = response.json().get("sql", "")
        print(f"Generated SQL:\n{sql}")
        
        # Check for ARRAY JOIN
        if "ARRAY JOIN" in sql.upper():
            print("✅ PASSED: Found ARRAY JOIN in SQL")
            return True
        else:
            print("❌ FAILED: No ARRAY JOIN found in SQL")
            return False
    else:
        print(f"❌ FAILED: HTTP {response.status_code}")
        print(f"Response: {response.text}")
        return False

def test_unwind_property_access():
    """Test UNWIND with property access (e.g., n.tags)."""
    query = "MATCH (e:Entity) UNWIND e.tags AS tag RETURN e.name, tag"
    
    print(f"\n=== Test: UNWIND property access ===")
    print(f"Query: {query}")
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "sql_only": True,
            "schema_name": "test_unwind"
        },
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code == 200:
        sql = response.json().get("sql", "")
        print(f"Generated SQL:\n{sql}")
        
        # Check for ARRAY JOIN
        if "ARRAY JOIN" in sql.upper():
            print("✅ PASSED: Found ARRAY JOIN in SQL")
            return True
        else:
            print("❌ FAILED: No ARRAY JOIN found in SQL")
            return False
    else:
        print(f"❌ FAILED: HTTP {response.status_code}")
        print(f"Response: {response.text}")
        return False

def load_test_schema():
    """Load the test schema into the server."""
    print("\n=== Loading test schema ===")
    
    response = requests.post(
        f"{BASE_URL}/schemas",
        json=TEST_SCHEMA,
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code in [200, 201]:
        print("✅ Schema loaded successfully")
        return True
    else:
        print(f"⚠️ Schema loading: HTTP {response.status_code}")
        print(f"Response: {response.text}")
        # Continue anyway - schema might already exist
        return True

def main():
    print("=" * 60)
    print("UNWIND → ARRAY JOIN Test Suite")
    print("=" * 60)
    
    # Check server is running
    try:
        health = requests.get(f"{BASE_URL}/health", timeout=5)
        if health.status_code != 200:
            print(f"❌ Server health check failed: {health.status_code}")
            sys.exit(1)
    except requests.exceptions.ConnectionError:
        print("❌ Server not running. Start with: cargo run --bin clickgraph")
        sys.exit(1)
    
    print("✅ Server is healthy")
    
    # Load test schema
    load_test_schema()
    
    # Run tests
    results = []
    results.append(("Literal list", test_unwind_literal_list()))
    results.append(("Property access", test_unwind_property_access()))
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"  {name}: {status}")
    
    print(f"\nTotal: {passed}/{total} passed")
    
    sys.exit(0 if passed == total else 1)

if __name__ == "__main__":
    main()
