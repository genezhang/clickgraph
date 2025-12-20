#!/usr/bin/env python3
"""
Test WHERE clause filters specifically for shortestPath() queries
This is what originally triggered the debugging session
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

BASE_URL = f"{CLICKGRAPH_URL}"

def test_shortest_path_query(description, query, check_strings):
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
            print(f"[ERROR] No SQL in response")
            return False
        
        print("Generated SQL (key sections):")
        print("-" * 80)
        
        # Show WHERE clauses and key filtering logic
        lines = sql.split('\n')
        in_relevant_section = False
        context_lines = []
        
        for i, line in enumerate(lines):
            # Check if this line or nearby lines contain our check strings
            line_lower = line.lower()
            is_relevant = ('where' in line_lower or 
                          any(check.lower() in line.lower() for check in check_strings) or
                          'order by' in line_lower or
                          'limit 1' in line_lower)
            
            if is_relevant:
                # Show context
                start = max(0, i-2)
                end = min(len(lines), i+3)
                if not in_relevant_section:
                    for j in range(start, end):
                        print(lines[j])
                    in_relevant_section = True
                    context_lines = list(range(start, end))
                elif i not in context_lines:
                    for j in range(start, end):
                        if j not in context_lines:
                            print(lines[j])
                            context_lines.append(j)
        
        if not in_relevant_section:
            # Show first 25 lines if nothing found
            print('\n'.join(lines[:25]))
            print('...')
        
        print("-" * 80)
        
        # Check if all expected strings are present
        all_found = True
        print("\nFilter Checks:")
        for check_str in check_strings:
            found = check_str in sql
            status = "[OK]" if found else "[MISSING]"
            print(f"  {status} Checking for: {check_str}")
            if not found:
                all_found = False
        
        # Additional checks for shortestPath
        has_order_by = 'ORDER BY' in sql or 'order by' in sql.lower()
        has_limit = 'LIMIT 1' in sql or 'limit 1' in sql.lower()
        
        print("\nShortestPath-specific checks:")
        print(f"  {'[OK]' if has_order_by else '[MISSING]'} Has ORDER BY hop_count")
        print(f"  {'[OK]' if has_limit else '[MISSING]'} Has LIMIT 1")
        
        if all_found and has_order_by and has_limit:
            print(f"\n[SUCCESS] All filters and shortestPath logic present!")
            return True
        else:
            print(f"\n[FAILED] Some filters or shortestPath logic missing")
            return False
            
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("="*80)
    print("SHORTESTPATH WITH WHERE CLAUSE FILTER TESTS")
    print("="*80)
    print("\nThis tests the original issue that triggered the debugging session:")
    print("WHERE clause filters not being applied in shortestPath queries")
    
    tests = [
        {
            "description": "shortestPath with start node filter",
            "query": "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice Johnson' AND b.name = 'David Lee' RETURN p",
            "check": ["Alice Johnson", "David Lee"]
        },
        {
            "description": "shortestPath with user_id filters",
            "query": "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.user_id = 1 AND b.user_id = 4 RETURN p",
            "check": ["user_id", "1", "4"]
        },
        {
            "description": "shortestPath with only start filter",
            "query": "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice Johnson' RETURN p",
            "check": ["Alice Johnson"]
        },
        {
            "description": "shortestPath with only end filter",
            "query": "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE b.user_id = 4 RETURN p",
            "check": ["user_id", "4"]
        }
    ]
    
    results = []
    for test in tests:
        result = test_shortest_path_query(test["description"], test["query"], test["check"])
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
    
    if passed == total:
        print("\n" + "="*80)
        print("SUCCESS! WHERE clause filters work correctly with shortestPath!")
        print("="*80)
    
    return passed == total

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
