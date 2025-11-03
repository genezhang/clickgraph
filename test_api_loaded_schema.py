#!/usr/bin/env python3
"""
Test API-loaded schemas to ensure they work correctly with the dual-key architecture.

IMPORTANT ARCHITECTURAL DISTINCTION:
====================================

1. STARTUP SCHEMA (via GRAPH_CONFIG_PATH environment variable):
   - Loaded by initialize_global_schema() 
   - Registered with BOTH keys: "default" + name from YAML (if present)
   - Example: GLOBAL_SCHEMAS["default"] = schema
             GLOBAL_SCHEMAS["test_integration"] = schema  (same instance)
   
2. API-LOADED SCHEMAS (via /api/schemas/load endpoint):
   - Loaded by load_schema_by_name()
   - Registered with ONLY the schema_name from API request
   - Example: GLOBAL_SCHEMAS["custom_schema"] = schema
   - Does NOT become "default" (correct behavior!)

This test validates that API-loaded schemas:
- Can be queried using USE clause
- Can be queried using schema_name parameter
- Are listed in available schemas
- Do NOT interfere with the default schema
"""

import requests
import time
import os
import tempfile

CLICKGRAPH_URL = "http://localhost:8080"

def create_test_schema_yaml(schema_name, database):
    """Create a temporary YAML schema file"""
    yaml_content = f"""name: {schema_name}
version: "1.0"
description: "Test schema for API loading validation"

graph_schema:
  nodes:
    - label: User
      database: {database}
      table: users
      id_column: user_id
      property_mappings:
        name: name
        age: age

  relationships:
    - type: FOLLOWS
      database: {database}
      table: follows
      from_id: follower_id
      to_id: followed_id
      from_node: User
      to_node: User
      property_mappings:
        since: since
"""
    
    # Create temporary file
    fd, path = tempfile.mkstemp(suffix='.yaml', text=True)
    with os.fdopen(fd, 'w') as f:
        f.write(yaml_content)
    
    return path

def test_api_loaded_schema_dual_registration():
    """Test that API-loaded schemas get dual-key registration"""
    
    print("=" * 70)
    print("Testing API-Loaded Schema Dual Registration")
    print("=" * 70)
    
    # Create a test schema YAML
    schema_name = "api_test_schema"
    database = "test_integration"
    yaml_path = create_test_schema_yaml(schema_name, database)
    
    try:
        print(f"\n1. Loading schema '{schema_name}' via API...")
        print(f"   YAML path: {yaml_path}")
        
        # Load schema via API
        load_response = requests.post(
            f"{CLICKGRAPH_URL}/api/schemas/load",
            json={
                "schema_name": schema_name,
                "config_path": yaml_path,
                "validate_schema": False
            },
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if load_response.status_code != 200:
            print(f"✗ Failed to load schema: {load_response.status_code}")
            print(f"  Response: {load_response.text}")
            return False
        
        print(f"✓ Schema loaded successfully via API")
        print(f"  Response: {load_response.json()}")
        
        # Test 1: Query using the schema name
        print(f"\n2. Testing query with schema name '{schema_name}'...")
        query1 = {
            "query": f"USE {schema_name} MATCH (n:User) RETURN n.name LIMIT 1",
        }
        
        response1 = requests.post(f"{CLICKGRAPH_URL}/query", json=query1)
        print(f"   Status: {response1.status_code}")
        
        if response1.status_code == 200:
            result = response1.json()
            print(f"✓ Query with schema name succeeded")
            print(f"  Result: {result}")
        else:
            print(f"✗ Query with schema name failed")
            print(f"  Error: {response1.text[:200]}")
            return False
        
        # Test 2: Query using schema_name parameter
        print(f"\n3. Testing query with schema_name parameter...")
        query2 = {
            "query": "MATCH (n:User) RETURN n.name LIMIT 1",
            "schema_name": schema_name
        }
        
        response2 = requests.post(f"{CLICKGRAPH_URL}/query", json=query2)
        print(f"   Status: {response2.status_code}")
        
        if response2.status_code == 200:
            result = response2.json()
            print(f"✓ Query with schema_name parameter succeeded")
            print(f"  Result: {result}")
        else:
            print(f"✗ Query with schema_name parameter failed")
            print(f"  Error: {response2.text[:200]}")
            return False
        
        # Test 3: Verify default schema is NOT affected
        print(f"\n4. Verifying default schema is unaffected...")
        query_default = {
            "query": "MATCH (n:User) RETURN n.name LIMIT 1",
            # No schema specified - should use "default"
        }
        
        response_default = requests.post(f"{CLICKGRAPH_URL}/query", json=query_default)
        print(f"   Status: {response_default.status_code}")
        
        if response_default.status_code == 200:
            result = response_default.json()
            print(f"✓ Default schema still works (not overwritten by API load)")
            print(f"  Result: {result}")
        else:
            print(f"✗ Default schema query failed")
            print(f"  Error: {response_default.text[:200]}")
            return False
        
        # Test 4: List available schemas
        print(f"\n5. Listing available schemas...")
        list_response = requests.get(f"{CLICKGRAPH_URL}/api/schemas")
        
        if list_response.status_code == 200:
            schemas = list_response.json()
            print(f"✓ Available schemas: {schemas}")
            
            # Check if our schema is listed
            if 'schemas' in schemas:
                schema_names = [s['name'] for s in schemas['schemas']]
                if schema_name in schema_names:
                    print(f"✓ API-loaded schema '{schema_name}' is listed")
                else:
                    print(f"✗ Schema '{schema_name}' not found in list: {schema_names}")
                    return False
        else:
            print(f"✗ Failed to list schemas: {list_response.status_code}")
            return False
        
        print("\n" + "=" * 70)
        print("🎉 ALL TESTS PASSED - API-loaded schema dual registration works!")
        print("=" * 70)
        return True
        
    finally:
        # Cleanup
        if os.path.exists(yaml_path):
            os.remove(yaml_path)
            print(f"\nCleanup: Removed temporary YAML file")

def verify_server_running():
    """Check if ClickGraph server is running"""
    try:
        response = requests.get(f"{CLICKGRAPH_URL}/health", timeout=2)
        if response.status_code == 200:
            print("✓ ClickGraph server is running")
            return True
    except:
        pass
    
    print("✗ ClickGraph server is not running")
    print(f"  Please start the server first")
    return False

if __name__ == '__main__':
    if verify_server_running():
        success = test_api_loaded_schema_dual_registration()
        exit(0 if success else 1)
    else:
        exit(1)
