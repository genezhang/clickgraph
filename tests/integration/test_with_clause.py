#!/usr/bin/env python3
"""
Integration tests for WITH clause functionality
Tests various WITH clause patterns including chaining with multiple MATCHes
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json
import sys

SERVER_URL = f"{CLICKGRAPH_URL}"

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def add_pass(self, test_name):
        self.passed += 1
        print(f"[PASS] {test_name}")

    def add_fail(self, test_name, reason):
        self.failed += 1
        self.errors.append((test_name, reason))
        print(f"[FAIL] {test_name}")
        print(f"  Reason: {reason}")

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*70}")
        print(f"Test Results: {self.passed}/{total} passed")
        if self.failed > 0:
            print(f"\nFailed tests:")
            for name, reason in self.errors:
                print(f"  - {name}: {reason}")
        print(f"{'='*70}")
        return self.failed == 0


def test_query(result, test_name, query, check_fn=None, should_fail=False):
    """
    Execute a test query and validate results
    
    Args:
        result: TestResult object to track pass/fail
        test_name: Name of the test
        query: Cypher query to execute
        check_fn: Optional function to validate response (takes response dict)
        should_fail: If True, expect the query to fail
    """
    print(f"\nTest: {test_name}")
    print(f"Query: {query}")
    
    try:
        response = requests.post(
            f"{SERVER_URL}/query",
            json={"query": query},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if should_fail:
            if response.status_code != 200:
                result.add_pass(test_name)
                return
            else:
                result.add_fail(test_name, "Expected query to fail but it succeeded")
                return
        
        if response.status_code != 200:
            result.add_fail(test_name, f"HTTP {response.status_code}: {response.text}")
            return
        
        data = response.json()
        
        if check_fn:
            check_result = check_fn(data)
            if check_result is True:
                result.add_pass(test_name)
            else:
                result.add_fail(test_name, check_result or "Check function returned False")
        else:
            result.add_pass(test_name)
            
    except Exception as e:
        result.add_fail(test_name, f"Exception: {str(e)}")


def check_sql_keywords(keywords):
    """Return a check function that verifies SQL contains specific keywords"""
    def check(data):
        sql = data.get("sql", "")
        missing = [kw for kw in keywords if kw.upper() not in sql.upper()]
        if missing:
            return f"Missing keywords in SQL: {missing}"
        return True
    return check


def check_result_count(expected_count):
    """Return a check function that verifies result count"""
    def check(data):
        results = data.get("results", [])
        if len(results) != expected_count:
            return f"Expected {expected_count} results, got {len(results)}"
        return True
    return check


def check_has_results():
    """Return a check function that verifies there are some results"""
    def check(data):
        results = data.get("results", [])
        if len(results) == 0:
            return "Expected results but got none"
        return True
    return check


def run_tests():
    """Run all WITH clause integration tests"""
    result = TestResult()
    
    print("="*70)
    print("WITH CLAUSE INTEGRATION TESTS")
    print("="*70)
    
    # Test 1: Basic WITH with aggregation and HAVING
    test_query(
        result,
        "WITH + aggregation + HAVING",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 1 RETURN a.name, follows",
        check_has_results()
    )
    
    # Test 2: WITH chaining another MATCH
    test_query(
        result,
        "WITH → MATCH pattern",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 1 MATCH (a)-[:LIKED]->(p:Post) RETURN a.name, follows, p.name as post_name",
        check_has_results()
    )
    
    # Test 3: WITH without aggregation (simple projection)
    test_query(
        result,
        "WITH simple projection (no aggregation)",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, b.name as friend_name RETURN a.name, friend_name",
        check_has_results()
    )
    
    # Test 4: Multiple aggregations in WITH
    test_query(
        result,
        "WITH multiple aggregations",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follow_count, MAX(b.age) as max_age WHERE follow_count > 0 RETURN a.name, follow_count, max_age",
        check_has_results()
    )
    
    # Test 5: WITH + ORDER BY + LIMIT
    test_query(
        result,
        "WITH + ORDER BY + LIMIT",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 0 RETURN a.name, follows ORDER BY follows DESC LIMIT 5",
        check_has_results()
    )
    
    # Test 6: WITH referencing relationship properties
    test_query(
        result,
        "WITH with relationship data",
        "MATCH (a:User)-[f:FOLLOWS]->(b:User) WITH a, COUNT(f) as rel_count WHERE rel_count > 1 RETURN a.name, rel_count",
        check_has_results()
    )
    
    # Test 7: WITH filtering and then another MATCH with filtering
    test_query(
        result,
        "WITH filter → MATCH with WHERE",
        "MATCH (a:User) WITH a WHERE a.age > 25 MATCH (a)-[:FOLLOWS]->(b:User) WHERE b.age > 20 RETURN a.name, b.name",
        check_has_results()
    )
    
    # Test 8: WITH collecting nodes
    test_query(
        result,
        "WITH collecting node IDs",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a.user_id as user_id, COUNT(b) as follows WHERE follows > 0 RETURN user_id, follows",
        check_has_results()
    )
    
    # Test 9: Multiple WITH clauses (chaining)
    test_query(
        result,
        "Multiple WITH clauses chained",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 1 WITH a.name as name, follows WHERE follows > 1 RETURN name, follows",
        check_has_results()
    )
    
    # Test 10: WITH after complex pattern
    test_query(
        result,
        "WITH after multi-hop pattern",
        "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) WITH a, COUNT(DISTINCT c) as second_degree WHERE second_degree > 0 RETURN a.name, second_degree",
        check_has_results()
    )
    
    # Test 11: WITH using expressions
    test_query(
        result,
        "WITH computed expressions",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows, COUNT(b) * 10 as score WHERE follows > 0 RETURN a.name, follows, score",
        check_has_results()
    )
    
    # Test 12: WITH + MATCH + aggregation
    test_query(
        result,
        "WITH → MATCH → aggregation in RETURN",
        "MATCH (a:User) WITH a WHERE a.age > 20 MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.name, COUNT(b) as follows",
        check_has_results()
    )
    
    # Test WITH + aggregation + WHERE (should generate HAVING clause)
    test_query(
        result,
        "WITH + aggregation + WHERE → HAVING clause",
        """MATCH (a:User)-[:FOLLOWS]->(b:User) 
           WITH a, COUNT(b) as cnt 
           WHERE cnt > 2 
           RETURN a.name, cnt""",
        check_sql_keywords(["HAVING", "GROUP BY"])
    )
    
    # Test WITH + WHERE without aggregation (should stay WHERE)
    test_query(
        result,
        "WITH + WHERE (no aggregation) → WHERE clause",
        """MATCH (a:User) 
           WITH a 
           WHERE a.user_id > 100 
           RETURN a.name""",
        lambda data: (
            True if "WHERE" in data.get("sql", "").upper() and 
                    "HAVING" not in data.get("sql", "").upper() 
            else "Expected WHERE but not HAVING in SQL"
        )
    )
    
    # Test WITH + aggregation + multiple WHERE conditions
    test_query(
        result,
        "WITH + aggregation + complex WHERE → complex HAVING",
        """MATCH (a:User)-[:FOLLOWS]->(b:User) 
           WITH a, COUNT(b) as cnt 
           WHERE cnt > 2 AND cnt < 100 
           RETURN a.name, cnt""",
        check_sql_keywords(["HAVING", "AND", "GROUP BY"])
    )
    
    return result.summary()


if __name__ == "__main__":
    # Check if server is running
    try:
        response = requests.get(f"{SERVER_URL}/health", timeout=2)
        if response.status_code != 200:
            print(f"Warning: Server health check returned {response.status_code}")
    except Exception as e:
        print(f"Error: Cannot connect to server at {SERVER_URL}")
        print(f"Please start the server first: cargo run --bin clickgraph")
        sys.exit(1)
    
    success = run_tests()
    sys.exit(0 if success else 1)
