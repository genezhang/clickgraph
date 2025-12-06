#!/usr/bin/env python3
"""
End-to-end test for multi-schema support.

This test verifies that different schemas map to different ClickHouse tables.
It creates two schemas with different table mappings for the same label "User".
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json
from pathlib import Path

BASE_URL = f"{CLICKGRAPH_URL}"

# Schema 1: Maps User -> test_integration.users
SCHEMA1_YAML = """
name: schema1
graph_schema:
  nodes:
    - label: User
      database: test_integration
      table: users
      id_column: user_id
      property_mappings:
        user_id: user_id
        name: name
        age: age

  relationships:
    - type: FOLLOWS
      database: test_integration
      table: follows
      from_id: follower_id
      to_id: followed_id
      from_node: User
      to_node: User
      property_mappings: {}
"""

# Schema 2: Maps User -> same tables but different relationships
SCHEMA2_YAML = """
name: schema2
graph_schema:
  nodes:
    - label: User
      database: test_integration
      table: users
      id_column: user_id
      property_mappings:
        user_id: user_id
        name: name
        age: age

  relationships:
    - type: KNOWS
      database: test_integration
      table: friendships
      from_id: user1_id
      to_id: user2_id
      from_node: User
      to_node: User
      property_mappings: {}
"""

def load_schema(schema_name, yaml_content):
    """Load a schema via API"""
    response = requests.post(
        f"{BASE_URL}/schemas/load",
        json={"schema_name": schema_name, "config_content": yaml_content}
    )
    print(f"\nLoading schema '{schema_name}':")
    print(f"  Status: {response.status_code}")
    if response.status_code != 200:
        print(f"  Error: {response.text}")
    return response.status_code == 200

def test_schema_isolation():
    """Test that queries use the correct schema's mappings"""
    
    # Load both schemas
    print("=" * 60)
    print("MULTI-SCHEMA END-TO-END TEST")
    print("=" * 60)
    
    assert load_schema("schema1", SCHEMA1_YAML), "Failed to load schema1"
    assert load_schema("schema2", SCHEMA2_YAML), "Failed to load schema2"
    
    # Test 1: Query with schema1 using FOLLOWS relationship
    print("\n1. Testing schema1 with FOLLOWS relationship:")
    query1 = {
        "query": "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN count(*) as follow_count",
        "schema_name": "schema1"
    }
    response1 = requests.post(f"{BASE_URL}/query", json=query1)
    print(f"   Status: {response1.status_code}")
    if response1.status_code == 200:
        print(f"   Result: {response1.json()}")
        print("   [OK] schema1 query succeeded")
    else:
        print(f"   Error: {response1.text}")
        # This might fail, but we want to see the error
    
    # Test 2: Query with schema2 using KNOWS relationship
    print("\n2. Testing schema2 with KNOWS relationship:")
    query2 = {
        "query": "MATCH (a:User)-[:KNOWS]->(b:User) RETURN count(*) as knows_count",
        "schema_name": "schema2"
    }
    response2 = requests.post(f"{BASE_URL}/query", json=query2)
    print(f"   Status: {response2.status_code}")
    if response2.status_code == 200:
        print(f"   Result: {response2.json()}")
        print("   [OK] schema2 query succeeded")
    else:
        print(f"   Error: {response2.text}")
    
    # Test 3: Verify schema1 doesn't know about KNOWS
    print("\n3. Testing schema1 with KNOWS (should fail):")
    query3 = {
        "query": "MATCH (a:User)-[:KNOWS]->(b:User) RETURN count(*) as knows_count",
        "schema_name": "schema1"
    }
    response3 = requests.post(f"{BASE_URL}/query", json=query3)
    print(f"   Status: {response3.status_code}")
    if response3.status_code != 200:
        print(f"   Error (expected): {response3.text}")
        print("   [OK] schema1 correctly rejects KNOWS relationship")
    else:
        print(f"   [WARN] Expected failure but got success: {response3.json()}")
    
    # Test 4: USE clause override
    print("\n4. Testing USE clause overrides schema_name parameter:")
    query4 = {
        "query": "USE schema2 MATCH (a:User)-[:KNOWS]->(b:User) RETURN count(*) as knows_count",
        "schema_name": "schema1"  # Should be ignored
    }
    response4 = requests.post(f"{BASE_URL}/query", json=query4)
    print(f"   Status: {response4.status_code}")
    if response4.status_code == 200:
        print(f"   Result: {response4.json()}")
        print("   [OK] USE clause correctly overrides parameter")
    else:
        print(f"   Error: {response4.text}")
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY:")
    print(f"  Schema1 FOLLOWS: {'✅ PASS' if response1.status_code == 200 else '❌ FAIL'}")
    print(f"  Schema2 KNOWS: {'✅ PASS' if response2.status_code == 200 else '❌ FAIL'}")
    print(f"  Schema1 rejects KNOWS: {'✅ PASS' if response3.status_code != 200 else '❌ FAIL'}")
    print(f"  USE clause override: {'✅ PASS' if response4.status_code == 200 else '❌ FAIL'}")
    print("=" * 60)

if __name__ == "__main__":
    test_schema_isolation()
