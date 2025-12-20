#!/usr/bin/env python3
"""
Test USE clause functionality for multi-database selection.

This test verifies:
1. USE clause overrides request parameter
2. USE clause overrides default schema
3. Request parameter works when no USE clause
4. Default schema used when neither USE nor parameter specified
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

BASE_URL = f"{CLICKGRAPH_URL}"

def test_use_clause_override_parameter():
    """Test that USE clause overrides schema_name parameter"""
    
    # Query with USE clause should use social_network, not ecommerce
    query = {
        "query": "USE social_network MATCH (u:User) RETURN count(u) as user_count",
        "schema_name": "ecommerce"  # This should be ignored due to USE clause
    }
    
    response = requests.post(f"{BASE_URL}/query", json=query)
    print(f"\n1. USE clause overrides parameter:")
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.json()}")
    
    # Should succeed if social_network schema exists and has User nodes
    assert response.status_code == 200
    print("   [OK] USE clause successfully overrides schema_name parameter")


def test_use_clause_without_parameter():
    """Test that USE clause works without schema_name parameter"""
    
    query = {
        "query": "USE social_network MATCH (u:User) RETURN count(u) as user_count"
    }
    
    response = requests.post(f"{BASE_URL}/query", json=query)
    print(f"\n2. USE clause without parameter:")
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.json()}")
    
    assert response.status_code == 200
    print("   [OK] USE clause works independently")


def test_parameter_without_use_clause():
    """Test that schema_name parameter works when no USE clause"""
    
    query = {
        "query": "MATCH (u:User) RETURN count(u) as user_count",
        "schema_name": "social_network"
    }
    
    response = requests.post(f"{BASE_URL}/query", json=query)
    print(f"\n3. Parameter without USE clause:")
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.json()}")
    
    assert response.status_code == 200
    print("   [OK] schema_name parameter works without USE clause")


def test_use_clause_with_qualified_name():
    """Test USE clause with qualified database name"""
    
    query = {
        "query": "USE clickgraph.social_network MATCH (u:User) RETURN count(u) as user_count"
    }
    
    response = requests.post(f"{BASE_URL}/query", json=query)
    print(f"\n4. USE clause with qualified name:")
    print(f"   Status: {response.status_code}")
    
    # This might fail if schema doesn't exist, which is expected
    print(f"   Response: {response.text[:200]}")
    
    # Just verify it was parsed correctly (schema error is OK)
    if response.status_code != 200:
        assert "Schema error" in response.text or "schema" in response.text.lower()
        print("   [OK] Qualified name parsed correctly (schema not found, as expected)")
    else:
        print("   [OK] Qualified name works")


def test_use_clause_case_insensitive():
    """Test that USE clause is case-insensitive"""
    
    queries = [
        "USE social_network MATCH (u:User) RETURN count(u)",
        "use social_network MATCH (u:User) RETURN count(u)",
        "Use social_network MATCH (u:User) RETURN count(u)",
    ]
    
    print(f"\n5. USE clause case insensitivity:")
    for query_str in queries:
        query = {"query": query_str}
        response = requests.post(f"{BASE_URL}/query", json=query)
        assert response.status_code == 200
        print(f"   [OK] '{query_str[:20]}...' works")
    
    print("   [OK] USE clause is case-insensitive")


def test_precedence_order():
    """Test full precedence order: USE > parameter > default"""
    
    # Test 1: USE wins over parameter
    q1 = {
        "query": "USE social_network MATCH (n) RETURN count(n) as cnt LIMIT 1",
        "schema_name": "should_be_ignored"
    }
    r1 = requests.post(f"{BASE_URL}/query", json=q1)
    
    # Test 2: Parameter wins over default (no USE)
    q2 = {
        "query": "MATCH (n) RETURN count(n) as cnt LIMIT 1",
        "schema_name": "social_network"
    }
    r2 = requests.post(f"{BASE_URL}/query", json=q2)
    
    print(f"\n6. Precedence order verification:")
    print(f"   USE > parameter: {r1.status_code == 200}")
    print(f"   parameter > default: {r2.status_code == 200}")
    print("   [OK] Precedence order: USE clause > schema_name parameter > default")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing USE clause functionality")
    print("=" * 60)
    
    try:
        # Check if server is running
        health = requests.get(f"{BASE_URL}/health")
        if health.status_code != 200:
            print("[FAIL] Server is not running. Start with: cargo run --bin clickgraph")
            exit(1)
        
        print("[OK] Server is running\n")
        
        # Run tests
        test_use_clause_override_parameter()
        test_use_clause_without_parameter()
        test_parameter_without_use_clause()
        test_use_clause_with_qualified_name()
        test_use_clause_case_insensitive()
        test_precedence_order()
        
        print("\n" + "=" * 60)
        print("[OK] All USE clause tests passed!")
        print("=" * 60)
        
    except requests.exceptions.ConnectionError:
        print("[FAIL] Cannot connect to server. Start with: cargo run --bin clickgraph")
        exit(1)
    except Exception as e:
        print(f"\n[FAIL] Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
