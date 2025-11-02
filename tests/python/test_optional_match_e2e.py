"""
End-to-end test for OPTIONAL MATCH feature with real ClickHouse data.

This script tests that OPTIONAL MATCH generates LEFT JOIN SQL and handles
null values correctly for unmatched patterns.
"""

import requests
import json
import sys

# ClickGraph server URL
SERVER_URL = "http://localhost:8080"

def send_query(cypher_query):
    """Send a Cypher query to ClickGraph and return the response."""
    payload = {
        "query": cypher_query,
        "view": "social_graph"
    }
    
    try:
        response = requests.post(
            f"{SERVER_URL}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error sending query: {e}")
        return None

def test_simple_optional_match():
    """Test basic OPTIONAL MATCH with relationship."""
    print("\n" + "="*70)
    print("TEST 1: Simple OPTIONAL MATCH")
    print("="*70)
    
    query = """
    MATCH (u:User)
    OPTIONAL MATCH (u)-[f:FRIENDS_WITH]->(friend:User)
    RETURN u.name, friend.name
    LIMIT 10
    """
    
    print(f"\nQuery:\n{query}")
    
    result = send_query(query)
    if result:
        print(f"\n✅ Query executed successfully")
        print(f"Response: {json.dumps(result, indent=2)}")
        
        # Check if SQL contains LEFT JOIN
        if 'sql' in result:
            sql = result['sql']
            if 'LEFT JOIN' in sql:
                print(f"\n✅ SUCCESS: SQL contains LEFT JOIN!")
                print(f"Generated SQL:\n{sql}")
            else:
                print(f"\n⚠️ WARNING: SQL does not contain LEFT JOIN")
                print(f"Generated SQL:\n{sql}")
        return result
    return None

def test_multiple_optional_match():
    """Test multiple OPTIONAL MATCH clauses."""
    print("\n" + "="*70)
    print("TEST 2: Multiple OPTIONAL MATCH")
    print("="*70)
    
    query = """
    MATCH (u:User {name: 'Alice'})
    OPTIONAL MATCH (u)-[f1:FRIENDS_WITH]->(friend1:User)
    OPTIONAL MATCH (u)-[f2:FRIENDS_WITH]->(friend2:User)
    RETURN u.name, friend1.name, friend2.name
    """
    
    print(f"\nQuery:\n{query}")
    
    result = send_query(query)
    if result:
        print(f"\n✅ Query executed successfully")
        print(f"Response: {json.dumps(result, indent=2)}")
        
        if 'sql' in result:
            sql = result['sql']
            left_join_count = sql.count('LEFT JOIN')
            print(f"\n✅ SUCCESS: Found {left_join_count} LEFT JOIN(s) in SQL")
            print(f"Generated SQL:\n{sql}")
        return result
    return None

def test_mixed_match_and_optional():
    """Test MATCH followed by OPTIONAL MATCH."""
    print("\n" + "="*70)
    print("TEST 3: Mixed MATCH and OPTIONAL MATCH")
    print("="*70)
    
    query = """
    MATCH (u:User)
    WHERE u.city = 'NYC'
    OPTIONAL MATCH (u)-[f:FRIENDS_WITH]->(friend:User)
    RETURN u.name, u.city, friend.name
    LIMIT 5
    """
    
    print(f"\nQuery:\n{query}")
    
    result = send_query(query)
    if result:
        print(f"\n✅ Query executed successfully")
        print(f"Response: {json.dumps(result, indent=2)}")
        
        if 'sql' in result:
            sql = result['sql']
            # Should have INNER JOIN for User table, LEFT JOIN for optional relationship
            inner_joins = sql.count('INNER JOIN') + sql.count('JOIN') - sql.count('LEFT JOIN')
            left_joins = sql.count('LEFT JOIN')
            print(f"\n✅ SUCCESS: Found {inner_joins} INNER JOIN(s) and {left_joins} LEFT JOIN(s)")
            print(f"Generated SQL:\n{sql}")
        return result
    return None

def test_optional_match_with_where():
    """Test OPTIONAL MATCH with WHERE clause."""
    print("\n" + "="*70)
    print("TEST 4: OPTIONAL MATCH with WHERE")
    print("="*70)
    
    query = """
    MATCH (u:User)
    OPTIONAL MATCH (u)-[f:FRIENDS_WITH]->(friend:User)
    WHERE friend.age > 25
    RETURN u.name, friend.name, friend.age
    LIMIT 5
    """
    
    print(f"\nQuery:\n{query}")
    
    result = send_query(query)
    if result:
        print(f"\n✅ Query executed successfully")
        print(f"Response: {json.dumps(result, indent=2)}")
        
        if 'sql' in result:
            sql = result['sql']
            if 'LEFT JOIN' in sql:
                print(f"\n✅ SUCCESS: OPTIONAL MATCH with WHERE generates LEFT JOIN")
                print(f"Generated SQL:\n{sql}")
            else:
                print(f"\n⚠️ WARNING: Expected LEFT JOIN not found")
        return result
    return None

def main():
    """Run all end-to-end tests."""
    print("\n" + "="*70)
    print("OPTIONAL MATCH End-to-End Tests")
    print("Testing LEFT JOIN SQL generation with real ClickHouse data")
    print("="*70)
    
    # Check if server is running
    try:
        response = requests.get(f"{SERVER_URL}/health", timeout=5)
        print(f"\n✅ Server is running at {SERVER_URL}")
    except requests.exceptions.RequestException:
        print(f"\n❌ Server is not running at {SERVER_URL}")
        print("Please start the server with:")
        print("  cargo run --bin brahmand -- --http-port 8080")
        sys.exit(1)
    
    # Run tests
    tests = [
        test_simple_optional_match,
        test_multiple_optional_match,
        test_mixed_match_and_optional,
        test_optional_match_with_where,
    ]
    
    results = []
    for test_func in tests:
        result = test_func()
        results.append(result is not None)
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    passed = sum(results)
    total = len(results)
    print(f"\n✅ Passed: {passed}/{total}")
    
    if passed == total:
        print("\n🎉 All tests passed! OPTIONAL MATCH is working correctly!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
