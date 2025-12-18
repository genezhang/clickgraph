#!/usr/bin/env python3
"""
Test CTE column aliasing issue - Issue #4
"""

import requests
import json
import sys

BASE_URL = "http://localhost:8080"

def test_cte_column_aliasing():
    """Test CTE column aliasing with WITH node + aggregation"""
    print("\n🧪 Testing CTE Column Aliasing Issue #4")
    print("=" * 60)
    
    # Test 1: WITH a, COUNT(b) - RETURN a.name, follows
    print("\nTest 1: WITH a, COUNT(b) - RETURN a.name, follows")
    print("-" * 60)
    
    query1 = """
    MATCH (a:User)-[:FOLLOWS]->(b:User)
    WITH a, COUNT(b) as follows
    WHERE follows > 1
    RETURN a.name, follows
    ORDER BY a.name
    LIMIT 5
    """
    
    print(f"Query: {query1.strip()}")
    print()
    
    try:
        response = requests.post(
            f"{BASE_URL}/query/sql",
            json={"query": query1},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ SQL Generated:")
            for i, sql in enumerate(result['sql'], 1):
                print(f"\nStatement {i}:")
                print(sql)
            print()
        else:
            print(f"❌ Error {response.status_code}:")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False
    
    # Test 2: Workaround - WITH a.name, COUNT(b)
    print("\nTest 2: Workaround - WITH a.name as name, COUNT(b)")
    print("-" * 60)
    
    query2 = """
    MATCH (a:User)-[:FOLLOWS]->(b:User)
    WITH a.name as name, COUNT(b) as follows
    WHERE follows > 1
    RETURN name, follows
    ORDER BY name
    LIMIT 5
    """
    
    print(f"Query: {query2.strip()}")
    print()
    
    try:
        response = requests.post(
            f"{BASE_URL}/query/sql",
            json={"query": query2},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ SQL Generated:")
            for i, sql in enumerate(result['sql'], 1):
                print(f"\nStatement {i}:")
                print(sql)
            print()
        else:
            print(f"❌ Error {response.status_code}:")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False
    
    print("🎉 All tests executed")
    return True

if __name__ == "__main__":
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=2)
        if response.status_code != 200:
            print("❌ Server not responding correctly at", BASE_URL)
            print("Please start the server with: cargo run --bin clickgraph")
            sys.exit(1)
    except:
        print("❌ Server not running at", BASE_URL)
        print("Please start the server with: cargo run --bin clickgraph")
        sys.exit(1)
    
    success = test_cte_column_aliasing()
    sys.exit(0 if success else 1)
