#!/usr/bin/env python3
"""
Test to verify that the schema is registered with both its actual name and "default" alias.
This tests the fix for the architecture issue where GLOBAL_GRAPH_SCHEMA and GLOBAL_SCHEMAS
could get out of sync.
"""

import requests
import time
import subprocess
import os

def start_server():
    """Start ClickGraph server in the background"""
    env = os.environ.copy()
    env['GRAPH_CONFIG_PATH'] = 'tests/integration/test_integration.yaml'
    env['CLICKHOUSE_URL'] = 'http://localhost:8123'
    env['CLICKHOUSE_USER'] = 'test_user'
    env['CLICKHOUSE_PASSWORD'] = 'test_pass'
    env['CLICKHOUSE_DATABASE'] = 'test_integration'
    
    print("Starting ClickGraph server...")
    proc = subprocess.Popen(
        ['cargo', 'run', '--bin', 'clickgraph', '--', '--http-port', '8080'],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for server to start
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get('http://localhost:8080/health', timeout=1)
            if response.status_code == 200:
                print("✓ Server started successfully")
                return proc
        except:
            pass
        time.sleep(1)
        print(f"  Waiting for server... ({i+1}/{max_retries})")
    
    raise Exception("Server failed to start")

def test_schema_registration():
    """Test that schema is accessible by both default and actual name"""
    
    # Test 1: Query using default schema (no schema specified)
    print("\n=== Test 1: Query using default schema ===")
    query = {
        "query": "MATCH (n:User) RETURN n.name LIMIT 1"
    }
    
    try:
        response = requests.post('http://localhost:8080/query', json=query)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Default schema query successful")
            print(f"  Result: {result}")
        else:
            print(f"✗ Query failed: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Request failed: {e}")
        return False
    
    # Test 2: Query using explicit schema name (test_integration)
    print("\n=== Test 2: Query using explicit schema name ===")
    query_with_schema = {
        "query": "USE test_integration MATCH (n:User) RETURN n.name LIMIT 1"
    }
    
    try:
        response = requests.post('http://localhost:8080/query', json=query_with_schema)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Named schema query successful")
            print(f"  Result: {result}")
        else:
            print(f"✗ Query failed: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Request failed: {e}")
        return False
    
    print("\n✓ All tests passed - dual schema registration working!")
    return True

if __name__ == '__main__':
    server_proc = None
    try:
        server_proc = start_server()
        success = test_schema_registration()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        exit(1)
    finally:
        if server_proc:
            print("\nStopping server...")
            server_proc.terminate()
            server_proc.wait(timeout=5)
