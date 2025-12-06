#!/usr/bin/env python3
"""
Test that queries with errors are NOT cached
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import time

BASE_URL = f"{CLICKGRAPH_URL}"

def test_parse_error_not_cached():
    """Test that parse errors don't get cached"""
    print("\n" + "="*60)
    print("TEST: Parse Error - Should NOT be cached")
    print("="*60)
    
    # Invalid syntax query - missing closing parenthesis
    query = "MATCH (u:User RETURN u.name"  # Invalid: missing )
    
    # First request - should be MISS and ERROR
    print("\n--- First Request (Expected: MISS + ERROR) ---")
    response1 = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "sql_only": True  # Use sql_only to see the error
        }
    )
    cache_status1 = response1.headers.get('X-Query-Cache-Status', 'NOT_SET')
    print(f"Cache Status: {cache_status1}")
    print(f"Status Code: {response1.status_code}")
    
    if response1.status_code == 200:
        data1 = response1.json()
        generated_sql = data1.get('generated_sql', '')
        print(f"Generated SQL: {generated_sql[:100]}...")
        is_error1 = 'ERROR' in generated_sql
        print(f"Is Error: {is_error1}")
    
    time.sleep(0.5)
    
    # Second request - should STILL be MISS (error not cached)
    print("\n--- Second Request (Expected: MISS + ERROR again) ---")
    response2 = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "sql_only": True
        }
    )
    cache_status2 = response2.headers.get('X-Query-Cache-Status', 'NOT_SET')
    print(f"Cache Status: {cache_status2}")
    print(f"Status Code: {response2.status_code}")
    
    if response2.status_code == 200:
        data2 = response2.json()
        generated_sql = data2.get('generated_sql', '')
        print(f"Generated SQL: {generated_sql[:100]}...")
        is_error2 = 'ERROR' in generated_sql
        print(f"Is Error: {is_error2}")
    
    # Verify: both should be MISS (error not cached)
    success = cache_status1 == "MISS" and cache_status2 == "MISS"
    
    print(f"\n{'✅ PASS' if success else '❌ FAIL'}: Parse errors NOT cached")
    return success

def test_valid_query_is_cached():
    """Test that valid queries ARE cached"""
    print("\n" + "="*60)
    print("TEST: Valid Query - SHOULD be cached")
    print("="*60)
    
    query = "MATCH (u:User) WHERE u.age > 25 RETURN u.name LIMIT 5"
    
    # First request - should be MISS
    print("\n--- First Request (Expected: MISS) ---")
    response1 = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "sql_only": True
        }
    )
    cache_status1 = response1.headers.get('X-Query-Cache-Status', 'NOT_SET')
    print(f"Cache Status: {cache_status1}")
    print(f"Status Code: {response1.status_code}")
    
    time.sleep(0.5)
    
    # Second request - should be HIT
    print("\n--- Second Request (Expected: HIT) ---")
    response2 = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "sql_only": True
        }
    )
    cache_status2 = response2.headers.get('X-Query-Cache-Status', 'NOT_SET')
    print(f"Cache Status: {cache_status2}")
    print(f"Status Code: {response2.status_code}")
    
    # Verify: MISS then HIT
    success = cache_status1 == "MISS" and cache_status2 == "HIT"
    
    print(f"\n{'✅ PASS' if success else '❌ FAIL'}: Valid queries ARE cached")
    return success

def main():
    print("="*60)
    print("CACHE ERROR HANDLING TEST")
    print("Verify that errors are NOT cached")
    print("="*60)
    
    results = []
    results.append(("Parse Error NOT Cached", test_parse_error_not_cached()))
    results.append(("Valid Query IS Cached", test_valid_query_is_cached()))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")
    
    print(f"\nResults: {passed}/{total} tests passed")
    print("="*60)
    
    return passed == total

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
