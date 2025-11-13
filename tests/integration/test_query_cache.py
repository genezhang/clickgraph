#!/usr/bin/env python3
"""Test script for query cache functionality."""

import requests
import json
import time
import yaml

BASE_URL = "http://localhost:8080"

def load_schema():
    """Load social network schema."""
    print("=== Loading Schema ===")
    with open("schemas/demo/social_network.yaml", "r") as f:
        schema_content = f.read()
    
    response = requests.post(
        f"{BASE_URL}/schemas/load",
        json={
            "schema_name": "social_network_demo",
            "config_content": schema_content,
            "validate_schema": False
        }
    )
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}\n")
    return response.status_code == 200

def test_cache_miss():
    """Test cache MISS (first query)."""
    print("=== Test 1: Cache MISS (First Query) ===")
    query = "MATCH (u:User) WHERE u.age > $minAge RETURN u.name, u.age LIMIT 5"
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "parameters": {"minAge": 25},
            "sql_only": True  # Just generate SQL, don't execute
        }
    )
    
    cache_status = response.headers.get("X-Query-Cache-Status", "NOT_SET")
    print(f"Cache Status: {cache_status}")
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Generated SQL: {data.get('generated_sql', 'N/A')[:100]}...")
    else:
        print(f"Error: {response.text}")
    
    print()
    return cache_status == "MISS"

def test_cache_hit():
    """Test cache HIT (repeat same query)."""
    print("=== Test 2: Cache HIT (Same Query) ===")
    query = "MATCH (u:User) WHERE u.age > $minAge RETURN u.name, u.age LIMIT 5"
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "parameters": {"minAge": 30},  # Different parameter value
            "sql_only": True
        }
    )
    
    cache_status = response.headers.get("X-Query-Cache-Status", "NOT_SET")
    print(f"Cache Status: {cache_status}")
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Generated SQL: {data.get('generated_sql', 'N/A')[:100]}...")
    else:
        print(f"Error: {response.text}")
    
    print()
    return cache_status == "HIT"

def test_whitespace_normalization():
    """Test that whitespace differences don't affect caching."""
    print("=== Test 3: Whitespace Normalization ===")
    
    # Query with extra whitespace
    query = """
    MATCH   (u:User)
    WHERE   u.age > $minAge
    RETURN  u.name, u.age
    LIMIT   5
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "parameters": {"minAge": 35},
            "sql_only": True
        }
    )
    
    cache_status = response.headers.get("X-Query-Cache-Status", "NOT_SET")
    print(f"Query with extra whitespace")
    print(f"Cache Status: {cache_status}")
    print(f"Expected: HIT (whitespace should be normalized)")
    print()
    return cache_status == "HIT"

def test_cypher_prefix_removal():
    """Test that CYPHER prefix is removed from cache key."""
    print("=== Test 4: CYPHER Prefix Removal ===")
    query = "CYPHER replan=default MATCH (u:User) WHERE u.age > $minAge RETURN u.name, u.age LIMIT 5"
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "parameters": {"minAge": 40},
            "sql_only": True
        }
    )
    
    cache_status = response.headers.get("X-Query-Cache-Status", "NOT_SET")
    print(f"Query with CYPHER prefix")
    print(f"Cache Status: {cache_status}")
    print(f"Expected: HIT (CYPHER prefix should be stripped)")
    print()
    return cache_status == "HIT"

def test_replan_force():
    """Test CYPHER replan=force bypasses cache."""
    print("=== Test 5: CYPHER replan=force ===")
    query = "CYPHER replan=force MATCH (u:User) WHERE u.age > $minAge RETURN u.name, u.age LIMIT 5"
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "parameters": {"minAge": 45},
            "sql_only": True
        }
    )
    
    cache_status = response.headers.get("X-Query-Cache-Status", "NOT_SET")
    print(f"Cache Status: {cache_status}")
    print(f"Expected: BYPASS (replan=force should bypass cache)")
    print()
    return cache_status == "BYPASS"

def test_different_query():
    """Test that different queries don't hit cache."""
    print("=== Test 6: Different Query (Cache MISS) ===")
    query = "MATCH (u:User)-[:FOLLOWS]->(friend) RETURN u.name, friend.name LIMIT 10"
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "schema_name": "social_network_demo",
            "sql_only": True
        }
    )
    
    cache_status = response.headers.get("X-Query-Cache-Status", "NOT_SET")
    print(f"Cache Status: {cache_status}")
    print(f"Expected: MISS (different query structure)")
    print()
    return cache_status == "MISS"

def main():
    """Run all tests."""
    print("=" * 60)
    print("QUERY CACHE TEST SUITE")
    print("=" * 60)
    print()
    
    # Load schema
    if not load_schema():
        print("❌ Failed to load schema. Aborting tests.")
        return
    
    time.sleep(0.5)
    
    # Run tests
    tests = [
        ("Cache MISS", test_cache_miss),
        ("Cache HIT", test_cache_hit),
        ("Whitespace Normalization", test_whitespace_normalization),
        ("CYPHER Prefix Removal", test_cypher_prefix_removal),
        ("Replan Force", test_replan_force),
        ("Different Query MISS", test_different_query),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
            time.sleep(0.2)  # Small delay between tests
        except Exception as e:
            print(f"❌ Test '{name}' failed with exception: {e}")
            results.append((name, False))
    
    # Summary
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {name}")
    
    print()
    print(f"Results: {passed}/{total} tests passed")
    print("=" * 60)

if __name__ == "__main__":
    main()
