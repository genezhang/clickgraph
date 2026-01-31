#!/usr/bin/env python3
"""
Test script for WITH + WHERE → GROUP BY + HAVING
Tests the new With logical plan node implementation
"""

import requests
import json

SERVER_URL = "http://localhost:8080"

def execute_test_query(description, query, expected_keywords=None):
    """Test a Cypher query and check for expected SQL keywords"""
    print(f"\n{'='*70}")
    print(f"Test: {description}")
    print(f"{'='*70}")
    print(f"Query: {query}")
    print()
    
    try:
        response = requests.post(
            f"{SERVER_URL}/query",
            json={"query": query, "sql_only": True},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            sql = result.get("generated_sql", result.get("sql", ""))
            print(f"Generated SQL:")
            print(sql)
            
            if expected_keywords:
                print(f"\nChecking for expected keywords...")
                for keyword in expected_keywords:
                    if keyword.upper() in sql.upper():
                        print(f"  ✓ Found: {keyword}")
                    else:
                        print(f"  ✗ Missing: {keyword}")
            
            return True
        else:
            print(f"Error: HTTP {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"Exception: {e}")
        return False

def main():
    print("Testing WITH clause with aggregations and HAVING support")
    print(f"Server: {SERVER_URL}")
    
    # Test 1: WITH aggregation + WHERE → should generate GROUP BY + HAVING
    execute_test_query(
        "WITH aggregation + WHERE on aggregated alias",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 1",
        expected_keywords=["GROUP BY", "HAVING"]
    )
    
    # Test 2: WITH aggregation + WHERE + RETURN
    execute_test_query(
        "WITH aggregation + WHERE + RETURN",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 1 RETURN a.name, follows",
        expected_keywords=["GROUP BY", "HAVING", "SELECT"]
    )
    
    # Test 3: WITH without aggregation (should not create GROUP BY)
    execute_test_query(
        "WITH without aggregation (should be simple projection)",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, b.name as name RETURN a.user_id, name",
        expected_keywords=["SELECT"]
    )
    
    # Test 4: Multiple aggregations
    execute_test_query(
        "WITH multiple aggregations",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows, AVG(b.age) as avg_age WHERE follows > 2 RETURN a.name, follows, avg_age",
        expected_keywords=["GROUP BY", "HAVING", "COUNT", "AVG"]
    )

if __name__ == "__main__":
    main()
