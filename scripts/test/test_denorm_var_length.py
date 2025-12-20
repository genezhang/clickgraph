#!/usr/bin/env python3
"""Quick test for variable-length paths with denormalized schema."""

import requests
import sys

BASE_URL = "http://localhost:8080"

def load_schema():
    """Load the denormalized flights schema."""
    with open('schemas/test/denormalized_flights.yaml', 'r') as f:
        schema_content = f.read()
    
    response = requests.post(
        f'{BASE_URL}/schemas/load',
        json={
            'schema_name': 'denormalized_flights_test',
            'config_content': schema_content
        }
    )
    if response.status_code != 200:
        print(f"Failed to load schema: {response.text}")
        return False
    print("Schema loaded successfully")
    return True

def test_sql_only(query: str, description: str):
    """Test query and show generated SQL only."""
    print(f"\n{'='*60}")
    print(f"TEST: {description}")
    print(f"{'='*60}")
    print(f"Cypher: {query}")
    
    response = requests.post(
        f'{BASE_URL}/query',
        json={
            'query': query,
            'schema_name': 'denormalized_flights_test',
            'sql_only': True
        }
    )
    
    if response.status_code != 200:
        print(f"ERROR: {response.status_code}")
        print(response.text)
        return False
    
    result = response.json()
    if 'error' in result:
        print(f"ERROR: {result['error']}")
        return False
    
    sql = result.get('generated_sql') or result.get('sql', 'NO SQL RETURNED')
    print(f"\nGenerated SQL:\n{sql}")
    
    # Check for issues
    issues = []
    if 'users' in sql.lower():
        issues.append("Contains 'users' table (hardcoded fallback)")
    if 'JOIN users' in sql or 'JOIN User' in sql:
        issues.append("Contains JOIN to users table")
    
    if issues:
        print(f"\n⚠️  ISSUES FOUND:")
        for issue in issues:
            print(f"   - {issue}")
        return False
    else:
        print("\n✅ No hardcoded references found")
        return True

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
    if not load_schema():
        sys.exit(1)
    
    # Test 1: Single hop (should work)
    test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "Single hop - should use flights table only"
    )
    
    # Test 2: Two hops (should work with our fix)
    test_sql_only(
        "MATCH (a:Airport)-[f1:FLIGHT]->(m:Airport)-[f2:FLIGHT]->(b:Airport) RETURN a.code, m.code, b.code LIMIT 5",
        "Two hops - should use flights table only, no node table joins"
    )
    
    # Test 3: Variable-length - this is the key test
    test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT*1..2]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "Variable-length *1..2 - should use recursive CTE, no node table joins"
    )
    
    # Test 4: Variable-length with unbounded max
    test_sql_only(
        "MATCH (a:Airport)-[f:FLIGHT*1..]->(b:Airport) RETURN a.code, b.code LIMIT 5",
        "Variable-length *1.. (unbounded) - should use recursive CTE"
    )

if __name__ == '__main__':
    main()
