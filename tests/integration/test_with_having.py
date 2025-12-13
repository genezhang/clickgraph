#!/usr/bin/env python3
"""
Simple test for WITH + aggregation + WHERE → HAVING clause generation
Uses sql_only=true to test SQL generation without running queries
"""

import requests
import json
import sys
import os

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

def test_with_where_having():
    """Test that WITH + aggregation + WHERE generates HAVING clause"""
    
    query = {
        "query": "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as cnt WHERE cnt > 2 RETURN a.name, cnt",
        "schema_name": "social_benchmark",
        "sql_only": True
    }
    
    print("Testing: WITH + aggregation + WHERE → HAVING")
    print(f"Query: {query['query']}")
    print()
    
    try:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json=query,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code != 200:
            print(f"❌ FAIL: HTTP {response.status_code}")
            print(response.text)
            return False
        
        data = response.json()
        sql = data.get("sql", "")
        
        print("Generated SQL:")
        print(sql)
        print()
        
        # Check for HAVING clause
        if "HAVING" not in sql.upper():
            print("❌ FAIL: No HAVING clause found in SQL")
            return False
        
        # Check for GROUP BY
        if "GROUP BY" not in sql.upper():
            print("❌ FAIL: No GROUP BY clause found in SQL")
            return False
        
        # Check for the condition
        if "cnt" not in sql.lower() or "> 2" not in sql:
            print("❌ FAIL: Condition 'cnt > 2' not found in SQL")
            return False
        
        # Check HAVING comes after GROUP BY
        group_by_pos = sql.upper().find("GROUP BY")
        having_pos = sql.upper().find("HAVING")
        if having_pos < group_by_pos:
            print("❌ FAIL: HAVING must come after GROUP BY")
            return False
        
        print("✅ PASS: SQL correctly contains HAVING clause after GROUP BY")
        return True
        
    except Exception as e:
        print(f"❌ FAIL: Exception: {e}")
        return False


def test_with_where_no_aggregation():
    """Test that WITH + WHERE without aggregation stays WHERE (not HAVING)"""
    
    query = {
        "query": "MATCH (a:User) WITH a WHERE a.user_id > 100 RETURN a.name",
        "schema_name": "social_benchmark",
        "sql_only": True
    }
    
    print("\nTesting: WITH + WHERE (no aggregation) → WHERE")
    print(f"Query: {query['query']}")
    print()
    
    try:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json=query,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code != 200:
            print(f"❌ FAIL: HTTP {response.status_code}")
            print(response.text)
            return False
        
        data = response.json()
        sql = data.get("sql", "")
        
        print("Generated SQL:")
        print(sql)
        print()
        
        # Check for WHERE clause
        if "WHERE" not in sql.upper():
            print("❌ FAIL: No WHERE clause found in SQL")
            return False
        
        # Should NOT have HAVING
        if "HAVING" in sql.upper():
            print("❌ FAIL: Should not have HAVING clause (no aggregation)")
            return False
        
        # Should NOT have GROUP BY
        if "GROUP BY" in sql.upper():
            print("❌ FAIL: Should not have GROUP BY (no aggregation)")
            return False
        
        print("✅ PASS: SQL correctly has WHERE without HAVING")
        return True
        
    except Exception as e:
        print(f"❌ FAIL: Exception: {e}")
        return False


if __name__ == "__main__":
    # Check if server is running
    try:
        response = requests.get(f"{CLICKGRAPH_URL}/health", timeout=2)
        if response.status_code != 200:
            print(f"Warning: Server health check returned {response.status_code}")
    except Exception as e:
        print(f"Error: Cannot connect to server at {CLICKGRAPH_URL}")
        print(f"Please start the server first with proper environment variables")
        sys.exit(1)
    
    print("=" * 70)
    print("WITH + WHERE → HAVING Clause Generation Tests")
    print("=" * 70)
    print()
    
    test1 = test_with_where_having()
    test2 = test_with_where_no_aggregation()
    
    print()
    print("=" * 70)
    if test1 and test2:
        print("✅ ALL TESTS PASSED")
        sys.exit(0)
    else:
        print("❌ SOME TESTS FAILED")
        sys.exit(1)
