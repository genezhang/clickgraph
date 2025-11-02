#!/usr/bin/env python3
"""
Comprehensive test for WHERE clause filters in variable-length paths
Tests multiple scenarios: start filter, end filter, both filters
"""

import requests
import json

BASE_URL = "http://localhost:8080"

def test_query(description, query, check_strings):
    print(f"\n{'='*80}")
    print(f"TEST: {description}")
    print(f"{'='*80}")
    print(f"Query: {query}\n")
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "sql_only": True},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"[FAILED] HTTP {response.status_code}")
        print(response.text)
        return False
    
    try:
        result = response.json()
        sql = result.get("generated_sql") or result.get("sql", "")
        
        if not sql:
            print(f"[ERROR] No SQL in response: {result}")
            return False
        print("Generated SQL (excerpt):")
        print("-" * 80)
        # Show relevant WHERE clauses
        lines = sql.split('\n')
        shown = False
        for i, line in enumerate(lines):
            if 'WHERE' in line.upper() or any(check in line for check in check_strings):
                # Show context
                start = max(0, i-2)
                end = min(len(lines), i+4)
                for j in range(start, end):
                    print(lines[j])
                print("...")
                shown = True
                break
        if not shown:
            # Show first 20 lines if no WHERE found
            for i, line in enumerate(lines[:20]):
                print(line)
            print("...")
        print("-" * 80)
        
        # Check if all expected strings are present
        all_found = True
        for check_str in check_strings:
            found = check_str in sql
            status = "[OK]" if found else "[MISSING]"
            print(f"  {status} Checking for: {check_str}")
            if not found:
                all_found = False
        
        if all_found:
            print(f"\n[SUCCESS] All filters present!")
            return True
        else:
            print(f"\n[FAILED] Some filters missing")
            return False
            
    except Exception as e:
        print(f"[ERROR] {e}")
        return False

def main():
    print("="*80)
    print("COMPREHENSIVE WHERE CLAUSE FILTER TESTS")
    print("="*80)
    
    tests = [
        {
            "description": "Start node filter only",
            "query": "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice Johnson' RETURN b",
            "check": ["Alice Johnson"]
        },
        {
            "description": "End node filter only",
            "query": "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE b.name = 'David Lee' RETURN a",
            "check": ["David Lee"]
        },
        {
            "description": "Both start and end filters",
            "query": "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice Johnson' AND b.name = 'David Lee' RETURN a, b",
            "check": ["Alice Johnson", "David Lee"]
        },
        {
            "description": "Property filter on start node",
            "query": "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.user_id = 1 RETURN b",
            "check": ["user_id", "1"]
        }
    ]
    
    results = []
    for test in tests:
        result = test_query(test["description"], test["query"], test["check"])
        results.append((test["description"], result))
    
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    for desc, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  [{status}] {desc}")
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    print(f"\nTotal: {passed}/{total} passed")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
