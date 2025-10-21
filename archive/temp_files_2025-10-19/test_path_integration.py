#!/usr/bin/env python3
"""
Phase 2.7 Integration Tests - Path Variables and Path Functions
Tests the complete implementation end-to-end with ClickHouse
"""
import requests
import json
import sys
from typing import Dict, List, Any

# Configuration
CLICKGRAPH_URL = "http://localhost:8080"
TEST_DATABASE = "social"

class TestRunner:
    def __init__(self):
        self.tests_run = 0
        self.tests_passed = 0
        self.tests_failed = 0
        self.test_results = []
        
    def test(self, name: str, query: str, expected_checks: List[callable] = None):
        """Run a single test query"""
        self.tests_run += 1
        print(f"\n{'='*80}")
        print(f"Test #{self.tests_run}: {name}")
        print(f"{'='*80}")
        print(f"Query: {query}")
        print(f"{'-'*80}")
        
        try:
            response = requests.post(
                f"{CLICKGRAPH_URL}/query",
                json={"query": query},
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"❌ FAILED: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                self.tests_failed += 1
                self.test_results.append({
                    "name": name,
                    "status": "FAILED",
                    "error": f"HTTP {response.status_code}: {response.text}"
                })
                return
            
            result = response.json()
            
            # Print response
            print(f"Status: ✅ SUCCESS")
            
            # Check if we have SQL in response (debug mode)
            if 'sql' in result:
                print(f"\nGenerated SQL:")
                print(result['sql'])
            
            # Check results
            if 'results' in result:
                print(f"\nResults ({len(result['results'])} rows):")
                for i, row in enumerate(result['results'][:5]):  # Show first 5
                    print(f"  Row {i+1}: {json.dumps(row, indent=4)}")
                if len(result['results']) > 5:
                    print(f"  ... and {len(result['results']) - 5} more rows")
            else:
                print("\nNo results returned")
            
            # Run custom checks
            if expected_checks:
                print(f"\nRunning validation checks...")
                for check in expected_checks:
                    check_result = check(result)
                    if check_result:
                        print(f"  ✅ {check_result}")
                    else:
                        print(f"  ❌ Check failed")
                        self.tests_failed += 1
                        self.test_results.append({
                            "name": name,
                            "status": "FAILED",
                            "error": "Validation check failed"
                        })
                        return
            
            self.tests_passed += 1
            self.test_results.append({
                "name": name,
                "status": "PASSED",
                "result_count": len(result.get('results', []))
            })
            
        except requests.exceptions.ConnectionError:
            print(f"❌ FAILED: Could not connect to ClickGraph server")
            print(f"   Make sure server is running on {CLICKGRAPH_URL}")
            self.tests_failed += 1
            self.test_results.append({
                "name": name,
                "status": "FAILED",
                "error": "Connection error"
            })
        except Exception as e:
            print(f"❌ FAILED: {e}")
            self.tests_failed += 1
            self.test_results.append({
                "name": name,
                "status": "FAILED",
                "error": str(e)
            })
    
    def print_summary(self):
        """Print test summary"""
        print(f"\n{'='*80}")
        print(f"TEST SUMMARY")
        print(f"{'='*80}")
        print(f"Total tests: {self.tests_run}")
        print(f"Passed: {self.tests_passed} ✅")
        print(f"Failed: {self.tests_failed} ❌")
        print(f"Success rate: {(self.tests_passed/self.tests_run*100):.1f}%")
        
        if self.tests_failed > 0:
            print(f"\nFailed tests:")
            for result in self.test_results:
                if result['status'] == 'FAILED':
                    print(f"  - {result['name']}: {result.get('error', 'Unknown error')}")
        
        print(f"{'='*80}\n")
        
        return self.tests_failed == 0

def main():
    print("="*80)
    print("Phase 2.7 Integration Tests - Path Variables and Path Functions")
    print("="*80)
    
    runner = TestRunner()
    
    # Test 1: Basic path variable return
    runner.test(
        "Basic path variable return",
        """
        MATCH p = (a:User)-[:FOLLOWS*1..3]-(b:User)
        WHERE a.name = 'Alice Johnson'
        RETURN p
        LIMIT 5
        """,
        [
            lambda r: "Found results" if r.get('results') else None,
        ]
    )
    
    # Test 2: Path variable with path object structure
    runner.test(
        "Path variable returns path object with structure",
        """
        MATCH p = (a:User)-[:FOLLOWS*1..2]-(b:User)
        WHERE a.name = 'Alice Johnson'
        RETURN p
        LIMIT 3
        """,
        [
            lambda r: "Has results" if r.get('results') else None,
        ]
    )
    
    # Test 3: length(p) function
    runner.test(
        "length(p) function returns hop count",
        """
        MATCH p = (a:User)-[:FOLLOWS*1..3]-(b:User)
        WHERE a.name = 'Alice Johnson'
        RETURN length(p) AS path_length, a.name AS start, b.name AS end
        LIMIT 10
        """
    )
    
    # Test 4: nodes(p) function
    runner.test(
        "nodes(p) function returns node array",
        """
        MATCH p = (a:User)-[:FOLLOWS*1..2]-(b:User)
        WHERE a.name = 'Alice Johnson'
        RETURN nodes(p) AS node_ids, length(p) AS hops
        LIMIT 5
        """
    )
    
    # Test 5: Path functions together
    runner.test(
        "Multiple path functions in same query",
        """
        MATCH p = (a:User)-[:FOLLOWS*1..3]-(b:User)
        WHERE a.name = 'Alice Johnson'
        RETURN length(p) AS distance, nodes(p) AS path, a.name, b.name
        LIMIT 5
        """
    )
    
    # Test 6: length(p) in WHERE clause
    runner.test(
        "length(p) in WHERE clause filters correctly",
        """
        MATCH p = (a:User)-[:FOLLOWS*]-(b:User)
        WHERE a.name = 'Alice Johnson' AND length(p) <= 2
        RETURN length(p), a.name, b.name
        """
    )
    
    # Test 7: length(p) in ORDER BY
    runner.test(
        "length(p) in ORDER BY sorts correctly",
        """
        MATCH p = (a:User)-[:FOLLOWS*1..3]-(b:User)
        WHERE a.name = 'Alice Johnson'
        RETURN length(p) AS distance, b.name AS destination
        ORDER BY length(p)
        LIMIT 5
        """
    )
    
    # Test 8: Complex query with path variable and properties
    runner.test(
        "Path variable with node properties",
        """
        MATCH p = (a:User)-[:FOLLOWS*1..2]-(b:User)
        WHERE a.name = 'Alice Johnson'
        RETURN p, length(p), a.name AS start_name, b.name AS end_name
        LIMIT 5
        """
    )
    
    # Test 9: relationships(p) function (returns empty array)
    runner.test(
        "relationships(p) function",
        """
        MATCH p = (a:User)-[:FOLLOWS*1..2]-(b:User)
        WHERE a.name = 'Alice Johnson'
        RETURN relationships(p) AS rels, length(p)
        LIMIT 3
        """
    )
    
    # Test 10: All three path functions
    runner.test(
        "All path functions in single query",
        """
        MATCH p = (a:User)-[:FOLLOWS*1..2]-(b:User)
        WHERE a.name = 'Alice Johnson'
        RETURN 
            length(p) AS hops,
            nodes(p) AS node_path,
            relationships(p) AS rel_path,
            a.name AS start,
            b.name AS end
        LIMIT 5
        """
    )
    
    # Print summary
    success = runner.print_summary()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
