#!/usr/bin/env python3
"""
Test for denormalized node ID property mapping fix (Dec 14, 2025).

This test verifies that JOIN conditions correctly use database column names
instead of Cypher property names for denormalized edges.

Bug: Zeek schema has node_id: ip (Cypher property) 
     mapped to from_node_properties: {ip: "id.orig_h"} (DB column)
     JOIN was generating: ON src.ip = r.orig_h (WRONG - 'ip' doesn't exist)
     Should generate: ON src.orig_h = r.orig_h (CORRECT - uses DB column)

Fix: resolve_id_column() now checks from_properties/to_properties first (for denormalized)
     then falls back to property_mappings (for standalone nodes)
"""

import sys
import requests

CLICKGRAPH_URL = "http://localhost:8080"

# Use the zeek_merged_test schema (loaded by test_zeek_merged.py fixture)
SCHEMA_NAME = "zeek_merged_test"


def test_denormalized_join_uses_db_columns():
    """Verify JOIN uses DB columns (orig_h) not Cypher properties (ip)."""
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={
            "query": "MATCH (src:IP)-[:REQUESTED]->(d:Domain) RETURN src, d LIMIT 1",
            "schema_name": SCHEMA_NAME,
            "sql_only": True
        }
    )
    
    if response.status_code != 200:
        print(f"❌ Query failed: {response.text}")
        return False
    
    sql = response.json().get("sql", "")
    
    # Should use DB column 'orig_h' in JOIN, not Cypher property 'ip'
    if "src.orig_h" not in sql:
        print(f"❌ JOIN should use DB column 'orig_h':\n{sql}")
        return False
    
    # Should NOT use Cypher property name 'ip' in JOIN (would cause error)
    if "src.ip = r.orig_h" in sql:
        print(f"❌ JOIN should not use Cypher property 'ip':\n{sql}")
        return False
    
    print(f"✅ JOIN correctly uses DB columns")
    print(f"SQL (excerpt): ...{sql[sql.find('JOIN'):sql.find('JOIN')+100]}...")
    return True


def test_query_executes_without_error():
    """Verify query actually executes (not just generates SQL)."""
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={
            "query": "MATCH (src:IP)-[:REQUESTED]->(d:Domain) WHERE d.name = 'cdn.example.com' RETURN src, d",
            "schema_name": SCHEMA_NAME
        }
    )
    
    if response.status_code != 200:
        print(f"❌ Query execution failed: {response.text}")
        return False
    
    result = response.json()
    
    # Should have data structure (even if no rows)
    if "data" not in result:
        print(f"❌ Expected data in response: {result}")
        return False
    
    print(f"✅ Query executes successfully (returned {len(result.get('data', []))} rows)")
    return True


if __name__ == "__main__":
    print("Testing denormalized node ID property mapping fix...")
    print("=" * 70)
    
    # Run tests after zeek schema is loaded by pytest fixture
    success = True
    success &= test_denormalized_join_uses_db_columns()
    print()
    success &= test_query_executes_without_error()
    
    print("=" * 70)
    if success:
        print("✅ All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed")
        sys.exit(1)

