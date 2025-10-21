#!/usr/bin/env python3
"""
Quick sql_only test to check if WHERE filters are being applied.
This script will:
1. Send a query with sql_only=True
2. Show the generated SQL
3. Check for filter presence
"""

import requests
import sys

def test_filter_query():
    query = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice Johnson' RETURN b"
    
    print("="*80)
    print("TESTING WHERE FILTER IN VARIABLE-LENGTH PATH")
    print("="*80)
    print(f"\nQuery: {query}\n")
    
    try:
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": query, "sql_only": True},
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        
        if response.status_code != 200:
            print(f"ERROR: Server returned {response.status_code}")
            print(f"Response: {response.text}")
            return False
        
        data = response.json()
        sql = data.get("generated_sql", "")
        
        print("Generated SQL:")
        print("-"*80)
        print(sql)
        print("-"*80)
        
        # Check for filter
        has_where = "WHERE" in sql or "where" in sql
        has_alice = "Alice Johnson" in sql or "Alice" in sql
        
        print(f"\nFilter Check:")
        print(f"   Has WHERE clause: {has_where}")
        print(f"   Has 'Alice Johnson': {has_alice}")
        
        if has_where and has_alice:
            print("\n[SUCCESS] Filter is present in SQL!")
            return True
        else:
            print("\n[ISSUE] Filter is NOT properly applied")
            print("   Expected to find 'WHERE' and 'Alice Johnson' in the SQL")
            return False
            
    except requests.exceptions.ConnectionError:
        print("[ERROR] Cannot connect to server at localhost:8080")
        print("   Make sure the server is running")
        return False
    except Exception as e:
        print(f"[ERROR] {e}")
        return False

if __name__ == "__main__":
    success = test_filter_query()
    sys.exit(0 if success else 1)
