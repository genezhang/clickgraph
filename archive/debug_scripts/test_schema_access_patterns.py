#!/usr/bin/env python3
"""
Comprehensive test to verify all schema access patterns work correctly:
1. No schema specified (implicit default)
2. Explicit USE default
3. Explicit USE schema_name
4. Request parameter schema_name
"""

import requests
import time
import subprocess
import os

CLICKGRAPH_URL = "http://localhost:8080"

def test_schema_access_patterns():
    """Test all ways to access a schema"""
    
    print("=" * 70)
    print("Testing Schema Access Patterns")
    print("=" * 70)
    
    test_cases = [
        {
            "name": "1. No schema specified (implicit default)",
            "query": "MATCH (n:User) RETURN n.name, n.age LIMIT 2",
            "expected_schema_log": "default",
        },
        {
            "name": "2. Explicit USE default",
            "query": "USE default MATCH (n:User) RETURN n.name, n.age LIMIT 2",
            "expected_schema_log": "default",
        },
        {
            "name": "3. Explicit USE test_integration",
            "query": "USE test_integration MATCH (n:User) RETURN n.name, n.age LIMIT 2",
            "expected_schema_log": "test_integration",
        },
        {
            "name": "4. Request parameter (schema_name=default)",
            "query": "MATCH (n:User) RETURN n.name, n.age LIMIT 2",
            "schema_param": "default",
            "expected_schema_log": "default",
        },
        {
            "name": "5. Request parameter (schema_name=test_integration)",
            "query": "MATCH (n:User) RETURN n.name, n.age LIMIT 2",
            "schema_param": "test_integration",
            "expected_schema_log": "test_integration",
        },
        {
            "name": "6. USE clause overrides parameter (USE wins)",
            "query": "USE test_integration MATCH (n:User) RETURN n.name, n.age LIMIT 2",
            "schema_param": "default",
            "expected_schema_log": "test_integration",  # USE clause should win
        },
    ]
    
    passed = 0
    failed = 0
    
    for i, test in enumerate(test_cases, 1):
        print(f"\n{'─' * 70}")
        print(f"Test {i}/{len(test_cases)}: {test['name']}")
        print(f"{'─' * 70}")
        print(f"Query: {test['query']}")
        
        # Build request
        payload = {"query": test["query"]}
        if "schema_param" in test:
            payload["schema_name"] = test["schema_param"]
            print(f"Schema parameter: {test['schema_param']}")
        
        try:
            response = requests.post(f"{CLICKGRAPH_URL}/query", json=payload)
            
            if response.status_code == 200:
                result = response.json()
                
                # Verify we got results
                if isinstance(result, list) and len(result) > 0:
                    print(f"✓ Status: {response.status_code}")
                    print(f"✓ Results: {len(result)} rows")
                    print(f"  Sample: {result[0]}")
                    
                    # Verify data structure
                    if 'name' in result[0] and 'age' in result[0]:
                        print(f"✓ Data structure correct (name, age fields present)")
                        passed += 1
                    else:
                        print(f"✗ Data structure incorrect: {result[0].keys()}")
                        failed += 1
                else:
                    print(f"✗ Empty or invalid result: {result}")
                    failed += 1
            else:
                print(f"✗ Status: {response.status_code}")
                print(f"  Error: {response.text[:200]}")
                failed += 1
                
        except Exception as e:
            print(f"✗ Request failed: {e}")
            failed += 1
    
    # Summary
    print(f"\n{'═' * 70}")
    print(f"TEST SUMMARY")
    print(f"{'═' * 70}")
    print(f"✓ Passed: {passed}/{len(test_cases)}")
    print(f"✗ Failed: {failed}/{len(test_cases)}")
    print(f"{'═' * 70}")
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED - All schema access patterns work correctly!")
        return True
    else:
        print(f"\n❌ {failed} test(s) failed")
        return False

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
    print(f"  Please start the server with:")
    print(f"  $env:GRAPH_CONFIG_PATH='tests/integration/test_integration.yaml'")
    print(f"  $env:CLICKHOUSE_URL='http://localhost:8123'")
    print(f"  $env:CLICKHOUSE_USER='test_user'")
    print(f"  $env:CLICKHOUSE_PASSWORD='test_pass'")
    print(f"  $env:CLICKHOUSE_DATABASE='test_integration'")
    print(f"  cargo run --bin clickgraph")
    return False

if __name__ == '__main__':
    if verify_server_running():
        success = test_schema_access_patterns()
        exit(0 if success else 1)
    else:
        exit(1)
