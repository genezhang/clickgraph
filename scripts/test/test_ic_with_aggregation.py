#!/usr/bin/env python3
"""
Test the 5 LDBC IC queries that use WITH + aggregation
These were previously failing due to column name resolution issues
"""

import requests
import json
from pathlib import Path
from datetime import datetime

BASE_URL = "http://localhost:8080/query"
QUERY_DIR = Path("/home/gz/clickgraph/benchmarks/ldbc_snb/queries/official/interactive")

def load_query(filename: str) -> str:
    """Load and clean query from file"""
    with open(QUERY_DIR / filename, 'r') as f:
        content = f.read()
    
    # Remove single-line comments
    lines = []
    for line in content.split('\n'):
        if '//' in line:
            line = line[:line.index('//')]
        lines.append(line)
    
    return ' '.join(lines)

def test_query(name: str, filename: str, params: dict) -> bool:
    """Test a single query"""
    print(f"Testing {name}...", end=' ', flush=True)
    
    try:
        query = load_query(filename)
        
        # Send query with parameters
        response = requests.post(
            BASE_URL,
            json={"query": query, "parameters": params},
            timeout=30
        )
        
        result = response.json()
        
        if 'error' in result:
            print(f"❌ FAILED: {result['error'][:100]}")
            return False
        elif 'results' in result:
            count = len(result['results'])
            print(f"✅ PASSED: {count} rows")
            return True
        else:
            print(f"⚠️  UNKNOWN: {str(result)[:100]}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {str(e)[:100]}")
        return False

def main():
    print("Testing IC queries with WITH + aggregation patterns")
    print("=" * 60)
    print()
    
    # Test the 5 queries that were failing
    queries = [
        ("IC1", "complex-1.cypher", {"personId": 933, "firstName": "Yang"}),
        ("IC3", "complex-3.cypher", {"personId": 933, "countryXName": "India", "countryYName": "China", "startDate": 1275393600000, "endDate": 1277812800000}),
        ("IC4", "complex-4.cypher", {"personId": 933, "startDate": 1275350400000, "endDate": 1277856000000}),
        ("IC7", "complex-7.cypher", {"personId": 933}),
        ("IC8", "complex-8.cypher", {"personId": 933}),
    ]
    
    passed = 0
    failed = 0
    
    for name, filename, params in queries:
        if test_query(name, filename, params):
            passed += 1
        else:
            failed += 1
    
    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed (total: {len(queries)})")
    print("=" * 60)
    
    if failed == 0:
        print("🎉 All WITH + aggregation queries now passing!")
        return 0
    else:
        print(f"⚠️  {failed} queries still failing")
        return 1

if __name__ == "__main__":
    exit(main())
