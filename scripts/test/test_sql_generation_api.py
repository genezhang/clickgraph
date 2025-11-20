#!/usr/bin/env python3
"""
Test script for the new SQL generation API endpoint
Tests POST /query/sql functionality
"""

import requests
import json

BASE_URL = "http://localhost:8080"

def test_simple_query():
    """Test basic query without RBAC"""
    print("\n=== Test 1: Simple Query ===")
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={
            "query": "MATCH (u:User) WHERE u.age > 25 RETURN u.name LIMIT 10"
        }
    )
    
    print(f"Status: {response.status_code}")
    result = response.json()
    print(f"Target Database: {result['target_database']}")
    print(f"Query Type: {result['metadata']['query_type']}")
    print(f"Cache Status: {result['metadata']['cache_status']}")
    print(f"SQL Statements ({len(result['sql'])}):")
    for i, sql in enumerate(result['sql'], 1):
        print(f"  {i}. {sql[:100]}...")
    print(f"Total Time: {result['metadata']['total_time_ms']:.2f}ms")
    
    assert response.status_code == 200
    assert result['target_database'] == 'clickhouse'
    assert len(result['sql']) == 1  # No RBAC, should be single statement
    print("✓ PASSED")

def test_query_with_rbac():
    """Test query with RBAC role"""
    print("\n=== Test 2: Query with RBAC ===")
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={
            "query": "MATCH (u:User) RETURN u.name LIMIT 5",
            "role": "analyst"
        }
    )
    
    print(f"Status: {response.status_code}")
    result = response.json()
    print(f"SQL Statements ({len(result['sql'])}):")
    for i, sql in enumerate(result['sql'], 1):
        print(f"  {i}. {sql}")
    
    assert response.status_code == 200
    assert len(result['sql']) == 2  # With RBAC, should be 2 statements
    assert result['sql'][0] == "SET ROLE analyst"
    assert "SELECT" in result['sql'][1]
    print("✓ PASSED")

def test_query_with_parameters():
    """Test query with parameters"""
    print("\n=== Test 3: Query with Parameters ===")
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={
            "query": "MATCH (u:User) WHERE u.age > $minAge RETURN u.name",
            "parameters": {"minAge": 30}
        }
    )
    
    print(f"Status: {response.status_code}")
    result = response.json()
    print(f"Parameters: {result.get('parameters')}")
    print(f"SQL: {result['sql'][0][:150]}...")
    
    assert response.status_code == 200
    assert result.get('parameters') == {"minAge": 30}
    print("✓ PASSED")

def test_query_with_view_parameters():
    """Test query with view parameters (multi-tenancy)"""
    print("\n=== Test 4: Query with View Parameters ===")
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={
            "query": "MATCH (u:User) RETURN u.name LIMIT 3",
            "view_parameters": {"tenant_id": "acme_corp"},
            "role": "data_scientist"
        }
    )
    
    print(f"Status: {response.status_code}")
    result = response.json()
    print(f"Role: {result.get('role')}")
    print(f"View Parameters: {result.get('view_parameters')}")
    print(f"SQL Statements: {len(result['sql'])}")
    
    assert response.status_code == 200
    assert result['sql'][0] == "SET ROLE data_scientist"
    print("✓ PASSED")

def test_cache_behavior():
    """Test that cache works correctly"""
    print("\n=== Test 5: Cache Behavior ===")
    
    # Use timestamp to ensure unique query
    import time
    unique_id = int(time.time() * 1000)
    query = f"MATCH (u:User) WHERE u.user_id = {unique_id} RETURN count(u)"
    
    # First request - should be cache MISS
    response1 = requests.post(
        f"{BASE_URL}/query/sql",
        json={"query": query}
    )
    result1 = response1.json()
    cache_status1 = result1['metadata']['cache_status']
    time1 = result1['metadata']['total_time_ms']
    
    # Second request - should be cache HIT
    response2 = requests.post(
        f"{BASE_URL}/query/sql",
        json={"query": query}
    )
    result2 = response2.json()
    cache_status2 = result2['metadata']['cache_status']
    time2 = result2['metadata']['total_time_ms']
    
    print(f"Request 1: {cache_status1}, {time1:.2f}ms")
    print(f"Request 2: {cache_status2}, {time2:.2f}ms")
    
    assert cache_status1 == "MISS"
    assert cache_status2 == "HIT"
    assert time2 < time1  # Cache hit should be faster
    print("✓ PASSED")

def test_parse_error():
    """Test error handling for invalid Cypher syntax"""
    print("\n=== Test 6: Parse Error Handling ===")
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={"query": "MATCH RETURN"}  # Missing pattern and projection
    )
    
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        error = response.json()
        print(f"Error Type: {error.get('error_type')}")
        print(f"Error: {error.get('error')[:100]}")
        
        assert response.status_code == 400
        assert error['error_type'] == "ParseError"
        print("✓ PASSED")
    else:
        print("✗ FAILED: Expected error response")

def test_include_plan():
    """Test including logical plan in response"""
    print("\n=== Test 7: Include Logical Plan ===")
    # Use a unique query to avoid cache hit
    import time
    unique_query = f"MATCH (u:User) WHERE u.user_id = {int(time.time())} RETURN u.name LIMIT 1"
    
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={
            "query": unique_query,
            "include_plan": True
        }
    )
    
    print(f"Status: {response.status_code}")
    result = response.json()
    has_plan = 'logical_plan' in result and result['logical_plan'] is not None
    print(f"Has Logical Plan: {has_plan}")
    if has_plan:
        print(f"Plan Length: {len(result['logical_plan'])} characters")
    
    assert response.status_code == 200
    assert has_plan, "logical_plan field should be present when include_plan=true"
    assert len(result['logical_plan']) > 100, "logical_plan should contain plan details"
    print("✓ PASSED")
    assert has_plan
    print("✓ PASSED")

def test_explicit_clickhouse():
    """Test explicitly specifying clickhouse as target"""
    print("\n=== Test 8: Explicit ClickHouse Target ===")
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={
            "query": "MATCH (u:User) RETURN u.name LIMIT 5",
            "target_database": "clickhouse"
        }
    )
    
    print(f"Status: {response.status_code}")
    result = response.json()
    print(f"Target Database: {result['target_database']}")
    
    assert response.status_code == 200
    assert result['target_database'] == 'clickhouse'
    print("✓ PASSED")

def test_unsupported_database():
    """Test error handling for unsupported database"""
    print("\n=== Test 9: Unsupported Database ===")
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={
            "query": "MATCH (u:User) RETURN u.name",
            "target_database": "postgresql"  # Not yet supported
        }
    )
    
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        error = response.json()
        print(f"Error Type: {error.get('error_type')}")
        print(f"Error: {error.get('error')}")
        print(f"Hint: {error.get('error_details', {}).get('hint')}")
        
        assert response.status_code == 400
        assert error['error_type'] == "UnsupportedDialectError"
        assert 'postgresql' in error['error'].lower()
        print("✓ PASSED")
    else:
        print("✗ FAILED: Expected error response")

def main():
    print("=" * 60)
    print("SQL Generation API Tests")
    print("=" * 60)
    print(f"Testing against: {BASE_URL}")
    
    tests = [
        test_simple_query,
        test_query_with_rbac,
        test_query_with_parameters,
        test_query_with_view_parameters,
        test_cache_behavior,
        test_parse_error,
        test_include_plan,
        test_explicit_clickhouse,
        test_unsupported_database,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"✗ FAILED: {e}")
            failed += 1
        except requests.exceptions.RequestException as e:
            print(f"✗ CONNECTION ERROR: {e}")
            print("Make sure the server is running: cargo run --bin clickgraph")
            break
        except Exception as e:
            print(f"✗ UNEXPECTED ERROR: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return failed == 0

if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)
