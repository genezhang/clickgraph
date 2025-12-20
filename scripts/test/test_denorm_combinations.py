#!/usr/bin/env python3
"""Test denormalized pattern combinations for variable-length paths.

Tests variable-length paths with denormalized edges including:
- *1..2, *1..3 (bounded ranges)
- *0..2, *0..3 (zero-hop ranges) 
- *0.. (unbounded starting at zero)
- * (unbounded)

Uses the denormalized_flights schema for denorm patterns.
"""

import requests
import sys

BASE_URL = "http://localhost:8080"

def load_schema(schema_path: str, schema_name: str):
    """Load a schema file."""
    try:
        with open(schema_path, 'r') as f:
            schema_content = f.read()
    except FileNotFoundError:
        print(f"Schema file not found: {schema_path}")
        return False
    
    response = requests.post(
        f'{BASE_URL}/schemas/load',
        json={
            'schema_name': schema_name,
            'config_content': schema_content
        }
    )
    if response.status_code != 200:
        print(f"Failed to load schema {schema_name}: {response.text}")
        return False
    print(f"✅ Loaded schema: {schema_name}")
    return True

def test_sql_only(query: str, schema_name: str, description: str, expected_patterns: list = None, forbidden_patterns: list = None):
    """Test query and show generated SQL only."""
    print(f"\n{'='*70}")
    print(f"TEST: {description}")
    print(f"{'='*70}")
    print(f"Schema: {schema_name}")
    print(f"Cypher: {query}")
    
    response = requests.post(
        f'{BASE_URL}/query',
        json={
            'query': query,
            'schema_name': schema_name,
            'sql_only': True
        }
    )
    
    if response.status_code != 200:
        print(f"❌ ERROR: {response.status_code}")
        print(response.text)
        return False
    
    result = response.json()
    if 'error' in result:
        print(f"❌ ERROR: {result['error']}")
        return False
    
    sql = result.get('generated_sql') or result.get('sql', 'NO SQL RETURNED')
    print(f"\nGenerated SQL:\n{sql}")
    
    # Check patterns
    passed = True
    
    if expected_patterns:
        for pattern in expected_patterns:
            if pattern.lower() in sql.lower():
                print(f"  ✅ Contains expected: '{pattern}'")
            else:
                print(f"  ❌ Missing expected: '{pattern}'")
                passed = False
    
    if forbidden_patterns:
        for pattern in forbidden_patterns:
            if pattern.lower() in sql.lower():
                print(f"  ❌ Contains forbidden: '{pattern}'")
                passed = False
            else:
                print(f"  ✅ Does not contain: '{pattern}'")
    
    if passed:
        print("\n✅ PASSED")
    else:
        print("\n❌ FAILED")
    
    return passed

def main():
    # Check server is up
    try:
        response = requests.get(f'{BASE_URL}/health')
        if response.status_code != 200:
            print(f"Server not healthy: {response.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"Server not reachable: {e}")
        sys.exit(1)
    
    # Load schema
    if not load_schema('schemas/test/denormalized_flights.yaml', 'denormalized_flights_test'):
        sys.exit(1)
    
    results = []
    
    # ===================================================================
    # TEST 1: Single-hop pattern (baseline)
    # ===================================================================
    results.append(test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "denormalized_flights_test",
        "Single-hop: Denorm pattern, no CTE",
        expected_patterns=[
            "test_integration.flights",
        ],
        forbidden_patterns=[
            "WITH RECURSIVE",  # No CTE for single hop
        ]
    ))
    
    # ===================================================================
    # TEST 2: Variable-length *1..2 (standard bounded range)
    # ===================================================================
    results.append(test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT*1..2]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "denormalized_flights_test",
        "*1..2: Standard bounded range with denormalized edges",
        expected_patterns=[
            "WITH RECURSIVE",     # Uses CTE
            "test_integration.flights",  # Edge table
        ],
        forbidden_patterns=[
            "JOIN test_integration.flights AS a",  # No node table JOINs (denorm)
            "JOIN test_integration.flights AS b",
        ]
    ))
    
    # ===================================================================
    # TEST 3: Variable-length *0..2 (zero-hop bounded range)
    # ===================================================================
    results.append(test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT*0..2]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "denormalized_flights_test",
        "*0..2: Zero-hop bounded range (includes self-loops)",
        expected_patterns=[
            "test_integration.flights",  # Edge table
        ],
    ))
    
    # ===================================================================
    # TEST 4: Variable-length *0..3 (zero-hop with larger range)
    # ===================================================================
    results.append(test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT*0..3]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "denormalized_flights_test",
        "*0..3: Zero-hop with larger range",
        expected_patterns=[
            "test_integration.flights",  # Edge table
        ],
    ))
    
    # ===================================================================
    # TEST 5: Variable-length *0.. (unbounded starting at zero)
    # ===================================================================
    results.append(test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT*0..]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "denormalized_flights_test",
        "*0..: Unbounded starting at zero",
        expected_patterns=[
            "WITH RECURSIVE",     # Uses CTE for unbounded
            "test_integration.flights",  # Edge table
        ],
    ))
    
    # ===================================================================
    # TEST 6: Variable-length * (unbounded, same as *1..)
    # ===================================================================
    results.append(test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT*]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "denormalized_flights_test",
        "*: Unbounded (same as *1..)",
        expected_patterns=[
            "WITH RECURSIVE",     # Uses CTE for unbounded
            "test_integration.flights",  # Edge table
        ],
    ))
    
    # ===================================================================
    # TEST 7: Exact 2-hop (*2) - should use inline JOINs
    # ===================================================================
    results.append(test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT*2]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "denormalized_flights_test",
        "*2: Exact 2-hop (inline JOINs, no CTE)",
        expected_patterns=[
            "test_integration.flights",
        ],
        forbidden_patterns=[
            "WITH RECURSIVE",  # Exact hop uses inline JOINs
        ]
    ))
    
    # ===================================================================
    # TEST 8: Two-hop inline pattern (multi-hop without variable-length)
    # ===================================================================
    results.append(test_sql_only(
        "MATCH (a:Airport)-[f1:FLIGHT]->(m:Airport)-[f2:FLIGHT]->(b:Airport) RETURN a.code, m.code, b.code LIMIT 5",
        "denormalized_flights_test",
        "Multi-hop inline: Two explicit hops without *",
        expected_patterns=[
            "test_integration.flights",  # Edge table
        ],
        forbidden_patterns=[
            "WITH RECURSIVE",  # No CTE for explicit hops
        ]
    ))
    
    # ===================================================================
    # Summary
    # ===================================================================
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    passed = sum(1 for r in results if r)
    total = len(results)
    
    print(f"\nPassed: {passed}/{total}")
    
    if passed == total:
        print("\n✅ ALL TESTS PASSED")
        return 0
    else:
        print("\n❌ SOME TESTS FAILED")
        return 1

if __name__ == '__main__':
    sys.exit(main())
