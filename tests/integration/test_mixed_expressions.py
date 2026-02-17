#!/usr/bin/env python3
"""
Test mixed property expressions across FROM/TO/RELATION nodes
Tests both standard and denormalized schema patterns

Purpose: Identify which expression patterns work and which fail
before refactoring the property resolution logic.
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json
import sys

BASE_URL = f"{CLICKGRAPH_URL}"

def run_query(query, description, sql_only=False):
    """Run a query and return result with status"""
    print(f"\n{'='*70}")
    print(f"TEST: {description}")
    print(f"{'='*70}")
    print(f"Query: {query}")
    print("-" * 70)
    
    payload = {"query": query}
    if sql_only:
        payload["sql_only"] = True
    
    try:
        response = requests.post(
            f"{BASE_URL}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        status = "✅ SUCCESS" if response.status_code == 200 else f"❌ FAILED ({response.status_code})"
        print(f"Status: {status}")
        
        if response.status_code == 200:
            data = response.json()
            if sql_only and 'sql' in data:
                print(f"Generated SQL:\n{data['sql']}")
            elif 'results' in data:
                print(f"Row count: {len(data['results'])}")
                if data['results']:
                    print(f"Sample row: {json.dumps(data['results'][0], indent=2)}")
        else:
            print(f"Error: {response.text}")
        
        return {
            "description": description,
            "query": query,
            "status_code": response.status_code,
            "success": response.status_code == 200,
            "response": response.json() if response.status_code == 200 else response.text
        }
    except Exception as e:
        print(f"❌ EXCEPTION: {str(e)}")
        return {
            "description": description,
            "query": query,
            "status_code": None,
            "success": False,
            "error": str(e)
        }

def test_standard_schema():
    """Test with standard schema (users, follows tables)"""
    print("\n" + "="*70)
    print("STANDARD SCHEMA TESTS (users/follows)")
    print("="*70)
    
    results = []
    
    # Test 1: Simple property access (baseline)
    results.append(run_query(
        "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name LIMIT 1",
        "Baseline: Simple property access"
    ))
    
    # Test 2: Two nodes, simple properties
    results.append(run_query(
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN u1.name, u2.name LIMIT 5",
        "Two nodes: Simple properties in RETURN"
    ))
    
    # Test 3: Expression in WHERE with two nodes
    results.append(run_query(
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id + u2.user_id < 10 RETURN u1.name, u2.name LIMIT 5",
        "WHERE: Mixed expression (u1.user_id + u2.user_id)"
    ))
    
    # Test 4: Expression in RETURN with two nodes
    results.append(run_query(
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN u1.name, u2.name, u1.user_id + u2.user_id AS sum_ids LIMIT 5",
        "RETURN: Mixed expression as computed column"
    ))
    
    # Test 5: String concatenation
    results.append(run_query(
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN concat(u1.name, ' -> ', u2.name) AS connection LIMIT 5",
        "RETURN: String function with mixed properties"
    ))
    
    # Test 6: Three nodes (multi-hop)
    results.append(run_query(
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u3:User) WHERE u1.user_id = 1 RETURN u1.name, u2.name, u3.name LIMIT 5",
        "Three nodes: Simple properties"
    ))
    
    # Test 7: Three nodes with expression
    results.append(run_query(
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u3:User) WHERE u1.user_id + u2.user_id + u3.user_id < 20 RETURN u1.name, u2.name, u3.name LIMIT 5",
        "Three nodes: Expression in WHERE"
    ))
    
    # Test 8: ORDER BY with expression
    results.append(run_query(
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) RETURN u1.name, u2.name, u1.user_id + u2.user_id AS sum_ids ORDER BY sum_ids DESC LIMIT 5",
        "ORDER BY: Using computed column"
    ))
    
    # Test 9: Relationship property mixed with node properties
    results.append(run_query(
        "MATCH (u1:User)-[f:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN u1.name, u2.name, f.follow_date LIMIT 5",
        "Relationship property with node properties"
    ))
    
    # Test 10: SQL-only mode to see generated SQL
    results.append(run_query(
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id + u2.user_id < 10 RETURN u1.name, u2.name",
        "SQL Generation: Mixed expression WHERE clause",
        sql_only=True
    ))
    
    return results

def test_denormalized_schema():
    """Test with denormalized schema (Airport nodes on flights table)"""
    print("\n" + "="*70)
    print("DENORMALIZED SCHEMA TESTS (flights with embedded airports)")
    print("="*70)
    
    results = []
    
    # Test 1: Simple FROM node property
    results.append(run_query(
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) WHERE a.code = 'JFK' RETURN a.code LIMIT 5",
        "Baseline: FROM node simple property"
    ))
    
    # Test 2: FROM and TO node properties
    results.append(run_query(
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) WHERE a.code = 'JFK' RETURN a.code, b.code LIMIT 5",
        "FROM and TO: Simple properties"
    ))
    
    # Test 3: Expression mixing FROM and TO
    results.append(run_query(
        "MATCH (s:Airport)-[:FLIGHT]->(t:Airport) WHERE s.code = 'JFK' AND t.code = 'LAX' RETURN s.code, t.code LIMIT 5",
        "WHERE: FROM AND TO properties"
    ))
    
    # Test 4: Numeric expression (if schema supports)
    # Note: This might fail if x/y properties don't exist - that's OK, we're testing resolution
    results.append(run_query(
        "MATCH (s:Airport)-[:FLIGHT]->(t:Airport) WHERE s.x + t.y < 100 RETURN s.code, t.code LIMIT 5",
        "WHERE: Expression with FROM.x + TO.y (may fail if columns don't exist)"
    ))
    
    # Test 5: RETURN with mixed FROM/TO expression
    results.append(run_query(
        "MATCH (s:Airport)-[f:FLIGHT]->(t:Airport) WHERE s.code = 'JFK' RETURN s.code, t.code, concat(s.code, '-', t.code) AS route LIMIT 5",
        "RETURN: String concat with FROM/TO properties"
    ))
    
    # Test 6: Edge property with node properties
    results.append(run_query(
        "MATCH (s:Airport)-[f:FLIGHT]->(t:Airport) WHERE f.distance > 1000 RETURN s.code, t.code, f.distance LIMIT 5",
        "Mixed: Edge property with node properties"
    ))
    
    # Test 7: All three in WHERE
    results.append(run_query(
        "MATCH (s:Airport)-[f:FLIGHT]->(t:Airport) WHERE s.code = 'JFK' AND t.code = 'LAX' AND f.distance > 2000 RETURN s.code, t.code, f.distance",
        "WHERE: FROM, TO, and EDGE properties"
    ))
    
    # Test 8: Multi-hop denormalized (complex case)
    results.append(run_query(
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport) WHERE a.code = 'JFK' RETURN a.code, b.code, c.code LIMIT 5",
        "Multi-hop: Three denormalized nodes (b appears twice - FROM and TO)"
    ))
    
    # Test 9: SQL-only mode
    results.append(run_query(
        "MATCH (s:Airport)-[f:FLIGHT]->(t:Airport) WHERE s.code = 'JFK' AND t.code = 'LAX' RETURN s.code, t.code, f.distance",
        "SQL Generation: Denormalized pattern",
        sql_only=True
    ))
    
    return results

def print_summary(standard_results, denorm_results):
    """Print test summary"""
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    def count_results(results):
        success = sum(1 for r in results if r.get('success'))
        total = len(results)
        return success, total
    
    std_success, std_total = count_results(standard_results)
    denorm_success, denorm_total = count_results(denorm_results)
    
    print(f"\nStandard Schema: {std_success}/{std_total} passed")
    print(f"Denormalized Schema: {denorm_success}/{denorm_total} passed")
    print(f"Overall: {std_success + denorm_success}/{std_total + denorm_total} passed")
    
    # Show failures
    all_results = standard_results + denorm_results
    failures = [r for r in all_results if not r.get('success')]
    
    if failures:
        print(f"\n{'='*70}")
        print("FAILURES:")
        print("="*70)
        for f in failures:
            print(f"\n❌ {f['description']}")
            print(f"   Query: {f['query']}")
            if 'error' in f:
                print(f"   Error: {f['error']}")
            elif f.get('status_code'):
                print(f"   Status: {f['status_code']}")
    
    return len(failures) == 0

if __name__ == "__main__":
    print("="*70)
    print("MIXED EXPRESSION TESTING SUITE")
    print("Testing property resolution with expressions across multiple nodes")
    print("="*70)
    
    # Check server health
    try:
        health_response = requests.post(f"{BASE_URL}/health", timeout=5)
        print(f"\n✅ Server is running (status: {health_response.status_code})")
    except Exception as e:
        print(f"\n❌ Server is not running: {e}")
        print("Please start the server with:")
        print("  export GRAPH_CONFIG_PATH=schemas/test/social_integration.yaml")
        print("  cargo run --bin clickgraph -- --http-port 8080 --disable-bolt")
        sys.exit(1)
    
    # Run tests
    print("\nNote: Switch schema between runs with GRAPH_CONFIG_PATH")
    print("  Standard: schemas/test/social_integration.yaml")
    print("  Denormalized: benchmarks/schemas/ontime_denormalized.yaml")
    
    # Detect which schema is loaded by trying a test query
    test_query = requests.post(
        f"{BASE_URL}/query",
        json={"query": "MATCH (u:User) RETURN u.name LIMIT 1"},
        headers={"Content-Type": "application/json"}
    )
    
    if test_query.status_code == 200:
        print("\n🔍 Detected: Standard schema (User nodes found)")
        standard_results = test_standard_schema()
        denorm_results = []
    else:
        print("\n🔍 Detected: Denormalized schema (or other)")
        standard_results = []
        denorm_results = test_denormalized_schema()
    
    # Print summary
    all_passed = print_summary(standard_results, denorm_results)
    
    sys.exit(0 if all_passed else 1)
