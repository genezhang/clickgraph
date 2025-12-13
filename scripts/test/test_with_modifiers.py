#!/usr/bin/env python3
"""
Test WITH clause modifiers to identify what's broken in rendering.
"""

import subprocess
import json

def test_query(desc, query):
    print(f"\n{'='*80}")
    print(f"TEST: {desc}")
    print(f"Query: {query}")
    print('='*80)
    
    cmd = [
        'curl', '-s', '-X', 'POST', 'http://localhost:8080/query',
        '-H', 'Content-Type: application/json',
        '-d', json.dumps({'query': query, 'sql_only': True})
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"✗ FAILED: {result.stderr}")
        return False
    
    try:
        data = json.loads(result.stdout)
        sql = data.get('generated_sql', '')
        error = data.get('error', '')
        
        if error:
            print(f"✗ ERROR: {error}")
            return False
            
        print(f"Generated SQL (first 500 chars):\n{sql[:500]}...")
        return sql
    except Exception as e:
        print(f"✗ PARSE ERROR: {e}")
        return False

print("="*80)
print("WITH CLAUSE MODIFIER TESTS")
print("="*80)

tests = [
    ("Simple WITH", 
     "MATCH (a:User) WITH a RETURN a.name LIMIT 3"),
    
    ("WITH + ORDER BY", 
     "MATCH (a:User) WITH a ORDER BY a.name LIMIT 3 RETURN a.name, a.user_id"),
    
    ("WITH + LIMIT", 
     "MATCH (a:User) WITH a LIMIT 10 RETURN a.name"),
    
    ("WITH + WHERE (no aggregation)", 
     "MATCH (a:User) WITH a WHERE a.user_id > 100 RETURN a.name"),
    
    ("WITH + Aggregation", 
     "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as cnt RETURN a.name, cnt LIMIT 3"),
    
    ("WITH + Aggregation + WHERE (HAVING)", 
     "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as cnt WHERE cnt > 2 RETURN a.name, cnt"),
    
    ("WITH + Aggregation + ORDER BY + LIMIT", 
     "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as cnt ORDER BY cnt DESC LIMIT 5 RETURN a.name, cnt"),
    
    ("WITH + DISTINCT", 
     "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH DISTINCT a RETURN a.name"),
]

results = {}
for desc, query in tests:
    sql = test_query(desc, query)
    
    # Check for expected SQL elements
    if sql:
        checks = {
            'ORDER BY': 'ORDER BY' in sql,
            'LIMIT': 'LIMIT' in sql,
            'WHERE': 'WHERE' in sql,
            'HAVING': 'HAVING' in sql,
            'DISTINCT': 'DISTINCT' in sql,
            'GROUP BY': 'GROUP BY' in sql,
        }
        
        print("\nSQL Elements:")
        for elem, present in checks.items():
            if present:
                print(f"  ✓ {elem} present")
        results[desc] = (True, checks)
    else:
        results[desc] = (False, {})

print("\n" + "="*80)
print("SUMMARY")
print("="*80)

print("\n✓ WORKING:")
for desc, (success, checks) in results.items():
    if success:
        print(f"  - {desc}")

print("\n✗ BROKEN:")
broken = []
for desc, (success, checks) in results.items():
    if not success:
        broken.append(desc)
        print(f"  - {desc}")

# Check for specific issues
print("\n⚠️ ISSUES DETECTED:")
for desc, (success, checks) in results.items():
    if success and desc.startswith("WITH + Aggregation + WHERE"):
        if not checks.get('WHERE') and not checks.get('HAVING'):
            print(f"  - {desc}: WHERE clause missing (should be HAVING in GROUP BY)")
            broken.append(desc)

if not broken:
    print("  None! All tests passed.")

print(f"\nTotal: {len(results)} tests, {len(broken)} broken")
