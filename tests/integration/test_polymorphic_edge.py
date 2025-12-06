#!/usr/bin/env python3
"""
Integration tests for polymorphic edge query support.

Polymorphic edges are relationships stored in a single table with a type discriminator column.
Example: `interactions` table with `interaction_type` column that can be 'FOLLOWS', 'LIKES', etc.

This test validates that ClickGraph correctly generates filter clauses for polymorphic edges.
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json
import sys

# Test configuration
SERVER_URL = f"{CLICKGRAPH_URL}"

def test_polymorphic_edge_query(query: str, expected_filter: str, description: str) -> bool:
    """
    Test a query and verify the expected filter appears in generated SQL.
    
    Args:
        query: Cypher query to test
        expected_filter: String that should appear in generated SQL
        description: Test description
    
    Returns:
        True if test passes, False otherwise
    """
    try:
        response = requests.post(
            f"{SERVER_URL}/query",
            headers={"Content-Type": "application/json"},
            json={"query": query, "sql_only": True},
            timeout=10
        )
        
        if response.status_code != 200:
            print(f"  ❌ {description}: HTTP {response.status_code}")
            return False
        
        result = response.json()
        sql = result.get("generated_sql", "")
        
        if expected_filter in sql:
            print(f"  ✓ {description}")
            print(f"    Query: {query}")
            print(f"    Filter: {expected_filter}")
            return True
        else:
            print(f"  ❌ {description}: Filter not found")
            print(f"    Query: {query}")
            print(f"    Expected: {expected_filter}")
            print(f"    SQL: {sql}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"  ❌ {description}: Connection failed - is server running?")
        return False
    except Exception as e:
        print(f"  ❌ {description}: {e}")
        return False


def main():
    print("=" * 60)
    print("Polymorphic Edge Query Tests")
    print("=" * 60)
    print(f"\nServer: {SERVER_URL}")
    print("Schema: social_polymorphic.yaml")
    print()
    
    # Test cases
    tests = [
        # Basic relationship type filtering
        (
            "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u.name, f.name",
            "interaction_type = 'FOLLOWS'",
            "FOLLOWS relationship filter"
        ),
        (
            "MATCH (u:User)-[r:LIKES]->(p:Post) RETURN u.name, p.title",
            "interaction_type = 'LIKES'",
            "LIKES relationship filter"
        ),
        # Relationship without type (should still work, just no type filter)
        (
            "MATCH (u:User) RETURN u.name",
            "brahmand.users",
            "Simple node query (no edge filter)"
        ),
    ]
    
    passed = 0
    failed = 0
    
    print("Test Results:")
    print("-" * 60)
    
    for query, expected, description in tests:
        if test_polymorphic_edge_query(query, expected, description):
            passed += 1
        else:
            failed += 1
    
    print()
    print("=" * 60)
    print(f"Summary: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
