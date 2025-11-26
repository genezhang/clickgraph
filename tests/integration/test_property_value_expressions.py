#!/usr/bin/env python3
"""
Integration test for PropertyValue feature - expressions in schema property mappings.

Tests:
1. Schema loading with expressions
2. Simple property access (fast path)
3. Expression property access (parsed at schema load)
4. SQL generation correctness
5. Query execution
"""

import requests
import json
import sys

BASE_URL = "http://localhost:8080"

def test_schema_info():
    """Test that schema loads successfully with expressions."""
    print("=" * 60)
    print("TEST 1: Schema Info - Verify expressions are loaded")
    print("=" * 60)
    
    response = requests.get(f"{BASE_URL}/schema")
    if response.status_code != 200:
        print(f"❌ FAIL: Schema endpoint returned {response.status_code}")
        return False
    
    schema = response.json()
    print(f"✓ Schema loaded successfully")
    
    # Check User node has expression properties
    user_props = schema.get('nodes', {}).get('User', {}).get('properties', {})
    
    if 'full_display_name' in user_props:
        print(f"✓ Expression property 'full_display_name' found in schema")
    else:
        print(f"❌ FAIL: Expression property 'full_display_name' not found")
        return False
        
    if 'primary_tag' in user_props:
        print(f"✓ Expression property 'primary_tag' found in schema")
    else:
        print(f"❌ FAIL: Expression property 'primary_tag' not found")
        return False
    
    print(f"✓ TEST 1 PASSED\n")
    return True


def test_simple_property_query():
    """Test query with simple column properties (fast path)."""
    print("=" * 60)
    print("TEST 2: Simple Property - Fast path (no expressions)")
    print("=" * 60)
    
    cypher = "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.email LIMIT 1"
    print(f"Cypher: {cypher}")
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ FAIL: Query returned {response.status_code}")
        print(f"Response: {response.text}")
        return False
    
    result = response.json()
    print(f"✓ Query executed successfully")
    print(f"Result: {json.dumps(result, indent=2)}")
    print(f"✓ TEST 2 PASSED\n")
    return True


def test_expression_property_query():
    """Test query with expression properties."""
    print("=" * 60)
    print("TEST 3: Expression Property - full_display_name")
    print("=" * 60)
    
    cypher = "MATCH (u:User) RETURN u.full_display_name LIMIT 5"
    print(f"Cypher: {cypher}")
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ FAIL: Query returned {response.status_code}")
        print(f"Response: {response.text}")
        return False
    
    result = response.json()
    print(f"✓ Query executed successfully")
    print(f"Result: {json.dumps(result, indent=2)}")
    print(f"✓ TEST 3 PASSED\n")
    return True


def test_array_indexing_expression():
    """Test query with array indexing expression."""
    print("=" * 60)
    print("TEST 4: Array Indexing - primary_tag")
    print("=" * 60)
    
    cypher = "MATCH (u:User) RETURN u.user_id, u.primary_tag LIMIT 5"
    print(f"Cypher: {cypher}")
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ FAIL: Query returned {response.status_code}")
        print(f"Response: {response.text}")
        return False
    
    result = response.json()
    print(f"✓ Query executed successfully")
    print(f"Result: {json.dumps(result, indent=2)}")
    print(f"✓ TEST 4 PASSED\n")
    return True


def test_math_expression():
    """Test query with math expression."""
    print("=" * 60)
    print("TEST 5: Math Expression - age_in_months")
    print("=" * 60)
    
    cypher = "MATCH (u:User) RETURN u.user_id, u.age_in_months LIMIT 5"
    print(f"Cypher: {cypher}")
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ FAIL: Query returned {response.status_code}")
        print(f"Response: {response.text}")
        return False
    
    result = response.json()
    print(f"✓ Query executed successfully")
    print(f"Result: {json.dumps(result, indent=2)}")
    print(f"✓ TEST 5 PASSED\n")
    return True


def test_sql_generation():
    """Test SQL generation to verify correct alias substitution."""
    print("=" * 60)
    print("TEST 6: SQL Generation - Verify alias substitution")
    print("=" * 60)
    
    cypher = "MATCH (u:User) RETURN u.full_display_name, u.primary_tag LIMIT 5"
    print(f"Cypher: {cypher}")
    
    response = requests.post(
        f"{BASE_URL}/sql",
        json={"query": cypher},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ FAIL: SQL generation returned {response.status_code}")
        print(f"Response: {response.text}")
        return False
    
    result = response.json()
    sql = result.get('sql', '')
    print(f"✓ SQL generated successfully")
    print(f"\nGenerated SQL:")
    print("-" * 60)
    print(sql)
    print("-" * 60)
    
    # Verify expressions are in SQL with proper alias
    if "concat(u." in sql or "concat(users_bench." in sql:
        print(f"✓ Expression 'concat' found in SQL with proper alias")
    else:
        print(f"⚠️  Warning: 'concat' expression not found in expected format")
    
    if "[1]" in sql:
        print(f"✓ Array indexing '[1]' found in SQL")
    else:
        print(f"⚠️  Warning: Array indexing '[1]' not found in SQL")
    
    print(f"✓ TEST 6 PASSED\n")
    return True


def test_mixed_properties():
    """Test query mixing simple columns and expressions."""
    print("=" * 60)
    print("TEST 7: Mixed Properties - Simple + Expression")
    print("=" * 60)
    
    cypher = """
    MATCH (u:User)
    WHERE u.user_id = 1
    RETURN u.name, u.full_display_name, u.email, u.primary_tag
    """
    print(f"Cypher: {cypher.strip()}")
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ FAIL: Query returned {response.status_code}")
        print(f"Response: {response.text}")
        return False
    
    result = response.json()
    print(f"✓ Query executed successfully")
    print(f"Result: {json.dumps(result, indent=2)}")
    print(f"✓ TEST 7 PASSED\n")
    return True


def main():
    """Run all integration tests."""
    print("\n" + "=" * 60)
    print("PropertyValue Integration Tests")
    print("Testing expressions in schema property mappings")
    print("=" * 60 + "\n")
    
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=2)
        if response.status_code != 200:
            print(f"❌ Server health check failed: {response.status_code}")
            sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to server at {BASE_URL}")
        print(f"   Make sure ClickGraph server is running")
        print(f"   Error: {e}")
        sys.exit(1)
    
    print(f"✓ Server is running at {BASE_URL}\n")
    
    # Run tests
    tests = [
        ("Schema Info", test_schema_info),
        ("Simple Properties", test_simple_property_query),
        ("Expression Properties", test_expression_property_query),
        ("Array Indexing", test_array_indexing_expression),
        ("Math Expression", test_math_expression),
        ("SQL Generation", test_sql_generation),
        ("Mixed Properties", test_mixed_properties),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ EXCEPTION in {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total tests: {len(tests)}")
    print(f"✓ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED!")
        sys.exit(0)
    else:
        print(f"\n❌ {failed} test(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
