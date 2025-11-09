#!/usr/bin/env python3
"""Quick test of the bug fixes for variable-length paths, shortest path, and aggregation."""

import requests
import json

SERVER_URL = "http://localhost:8080/query"

test_queries = [
    {
        "name": "Variable-length *2 (Bug #1 - ChainedJoin CTE wrapper)",
        "query": "MATCH (u1:User)-[:FOLLOWS*2]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name LIMIT 5",
        "expected_bug": "CTE wrapper or duplicate alias"
    },
    {
        "name": "Variable-length *1..3",
        "query": "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name LIMIT 5",
        "expected_bug": "May work or path variable issue"
    },
    {
        "name": "Shortest path with filter (Bug #2 - end_node filter rewrite)",
        "query": "MATCH (u1:User)-[:FOLLOWS*]-(u2:User) WHERE u1.user_id = 1 AND u2.user_id = 10 RETURN u1.name, u2.name",
        "expected_bug": "end_node.user_id filter (FIXED if works)"
    },
    {
        "name": "Aggregation with incoming relationship (Bug #3)",
        "query": "MATCH (u:User)<-[:FOLLOWS]-(follower) RETURN u.name, COUNT(follower) as follower_count LIMIT 5",
        "expected_bug": "Uses label 'User' as table name"
    },
]

print("Testing Bug Fixes")
print("=" * 80)

for i, test in enumerate(test_queries, 1):
    print(f"\n{i}. {test['name']}")
    print(f"   Query: {test['query']}")
    print(f"   Expected issue: {test['expected_bug']}")
    
    try:
        response = requests.post(SERVER_URL, json={"query": test["query"]}, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if isinstance(result, list):
                # Server returned array of results directly
                print(f"   [OK] SUCCESS! Got {len(result)} results")
                if len(result) > 0:
                    print(f"   Sample result: {result[0]}")
            elif isinstance(result, dict) and result.get("data"):
                print(f"   [OK] SUCCESS! Got {len(result['data'])} results")
                if len(result['data']) > 0:
                    print(f"   Sample result: {result['data'][0]}")
            else:
                error = result.get("error", str(result))
                if len(error) > 200:
                    error = error[:200] + "..."
                print(f"   [FAIL] FAILED: {error}")
        else:
            error_text = response.text[:200] if len(response.text) > 200 else response.text
            print(f"   [FAIL] FAILED (HTTP {response.status_code}): {error_text}")
    except Exception as e:
        print(f"   [FAIL] EXCEPTION: {str(e)}")

print("\n" + "=" * 80)
print("Test Summary Complete")
