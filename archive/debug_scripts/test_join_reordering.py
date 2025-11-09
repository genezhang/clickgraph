#!/usr/bin/env python3
"""Test JOIN reordering with incoming optional relationship."""

import requests
import json

# Query with incoming optional relationship
query = """
MATCH (a:User)
WHERE a.name = 'Alice'
OPTIONAL MATCH (b:User)-[:FOLLOWS]->(a)
RETURN a.name, b.name
"""

print("=" * 80)
print("Testing JOIN Reordering")
print("=" * 80)
print(f"Query:\n{query}\n")

# Test SQL generation only (no ClickHouse needed)
response = requests.post(
    'http://localhost:8080/query',
    json={"query": query, "sql_only": True},
    headers={"Content-Type": "application/json"}
)

print(f"Status: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(f"\nResponse data: {json.dumps(data, indent=2)}")
    sql = data.get("sql", "")
    
    print("\nGenerated SQL:")
    print("-" * 80)
    print(sql if sql else "(empty)")
    print("-" * 80)
    
    # Check for anchor in FROM
    if "FROM test_integration.users AS a" in sql:
        print("✅ PASS: Anchor node 'a' is in FROM clause")
    else:
        print("❌ FAIL: Anchor node 'a' is NOT in FROM clause")
    
    # Check JOIN order
    lines = [line.strip() for line in sql.split('\n') if 'JOIN' in line or 'FROM' in line]
    print(f"\nJOIN sequence:")
    for i, line in enumerate(lines, 1):
        print(f"  {i}. {line}")
    
    # Find the positions
    from_pos = next((i for i, line in enumerate(lines) if 'FROM' in line), -1)
    b_join_pos = next((i for i, line in enumerate(lines) if 'AS b' in line and 'JOIN' in line), -1)
    rel_join_pos = next((i for i, line in enumerate(lines) if 'follows' in line.lower() and 'JOIN' in line), -1)
    
    print(f"\nPositions: FROM={from_pos}, b_join={b_join_pos}, rel_join={rel_join_pos}")
    
    if from_pos >= 0 and b_join_pos > from_pos and (rel_join_pos < 0 or rel_join_pos > b_join_pos):
        print("✅ PASS: JOINs are in correct order (node b before relationship)")
    elif from_pos >= 0 and rel_join_pos > from_pos and b_join_pos > rel_join_pos:
        print("❌ FAIL: JOINs are in WRONG order (relationship before node b)")
    else:
        print("⚠️  UNKNOWN: Cannot determine JOIN order from SQL")
    
else:
    print(f"\nError: {response.text}")
