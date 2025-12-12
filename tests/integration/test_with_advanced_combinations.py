#!/usr/bin/env python3
"""
Integration tests for WITH clause combined with advanced features.
Tests combinations that were identified as potential fragility points.

Based on architectural-fragility-analysis.md recommendations.
"""

import requests
import os
import json
import sys

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
SERVER_URL = f"{CLICKGRAPH_URL}"

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def add_pass(self, test_name):
        self.passed += 1
        print(f"✅ [PASS] {test_name}")

    def add_fail(self, test_name, reason):
        self.failed += 1
        self.errors.append((test_name, reason))
        print(f"❌ [FAIL] {test_name}")
        print(f"   Reason: {reason}")

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*70}")
        print(f"Test Results: {self.passed}/{total} passed ({100*self.passed//total if total > 0 else 0}%)")
        if self.failed > 0:
            print(f"\n❌ Failed tests:")
            for name, reason in self.errors:
                print(f"  - {name}")
                print(f"    {reason}")
        print(f"{'='*70}")
        return self.failed == 0


def test_query(result, test_name, query, check_fn=None, should_fail=False, parameters=None):
    """Execute a test query and validate results"""
    print(f"\n🧪 Test: {test_name}")
    print(f"   Query: {query[:100]}..." if len(query) > 100 else f"   Query: {query}")
    if parameters:
        print(f"   Parameters: {parameters}")
    
    try:
        payload = {"query": query}
        if parameters:
            payload["parameters"] = parameters
        
        response = requests.post(
            f"{SERVER_URL}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30  # Longer timeout for complex queries
        )
        
        if should_fail:
            if response.status_code != 200:
                result.add_pass(test_name)
                return
            else:
                result.add_fail(test_name, "Expected query to fail but it succeeded")
                return
        
        if response.status_code != 200:
            result.add_fail(test_name, f"HTTP {response.status_code}: {response.text[:200]}")
            return
        
        data = response.json()
        
        # Check for ClickHouse errors in response
        if isinstance(data, str) and "Exception" in data:
            result.add_fail(test_name, f"ClickHouse error: {data[:200]}")
            return
        
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


def check_has_cte(cte_name):
    """Check that SQL contains a specific CTE"""
    def check(data):
        sql = data.get("sql", "")
        if f"WITH {cte_name}" not in sql.upper() and f"{cte_name} AS" not in sql.upper():
            return f"Missing CTE '{cte_name}' in SQL"
        return True
    return check


def check_has_recursive_cte():
    """Check that SQL contains RECURSIVE keyword"""
    def check(data):
        sql = data.get("sql", "")
        if "WITH RECURSIVE" not in sql.upper():
            return "Missing 'WITH RECURSIVE' in SQL"
        return True
    return check


def check_has_results():
    """Check that query returned some results"""
    def check(data):
        if "results" in data and isinstance(data["results"], list) and len(data["results"]) > 0:
            return True
        if "data" in data and len(data["data"]) > 0:
            return True
        return "No results returned"
    return check


def check_multiple_ctes(min_count=2):
    """Check that SQL contains at least min_count CTEs"""
    def check(data):
        sql = data.get("sql", "")
        # Count CTE definitions (look for pattern "name AS (")
        import re
        cte_pattern = r'\b\w+\s+AS\s*\('
        ctes = re.findall(cte_pattern, sql, re.IGNORECASE)
        if len(ctes) < min_count:
            return f"Expected at least {min_count} CTEs, found {len(ctes)}"
        return True
    return check


def run_tests():
    """Run all WITH + advanced feature combination tests"""
    result = TestResult()
    
    print("="*70)
    print("WITH CLAUSE + ADVANCED FEATURES COMBINATION TESTS")
    print("Testing fragility points identified in architectural analysis")
    print("="*70)
    
    # ========================================================================
    # Category 1: VLP (Variable-Length Paths) + WITH
    # ========================================================================
    print("\n" + "="*70)
    print("CATEGORY 1: Variable-Length Paths + WITH")
    print("="*70)
    
    # Test 1.1: Basic VLP + WITH
    test_query(
        result,
        "VLP + WITH aggregation",
        """
        MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)
        WITH a, COUNT(DISTINCT b) as reachable
        WHERE reachable > 0
        RETURN a.user_id, reachable
        LIMIT 5
        """,
        check_has_recursive_cte()
    )
    
    # Test 1.2: VLP + WITH + second MATCH
    test_query(
        result,
        "VLP + WITH + MATCH",
        """
        MATCH (a:User)-[:FOLLOWS*1..2]->(friend:User)
        WITH a, friend
        WHERE friend.user_id <> a.user_id
        MATCH (friend)-[:FOLLOWS]->(other:User)
        RETURN a.user_id, friend.user_id, COUNT(other) as others
        LIMIT 5
        """,
        check_multiple_ctes(2)
    )
    
    # Test 1.3: Chained WITH after VLP
    test_query(
        result,
        "VLP + chained WITH clauses",
        """
        MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)
        WITH a, COUNT(DISTINCT b) as reach
        WHERE reach > 1
        WITH a.user_id as uid, reach
        WHERE reach > 1
        RETURN uid, reach
        LIMIT 5
        """,
        check_has_recursive_cte()
    )
    
    # ========================================================================
    # Category 2: OPTIONAL MATCH + WITH
    # ========================================================================
    print("\n" + "="*70)
    print("CATEGORY 2: OPTIONAL MATCH + WITH")
    print("="*70)
    
    # Test 2.1: WITH + OPTIONAL MATCH
    test_query(
        result,
        "WITH + OPTIONAL MATCH",
        """
        MATCH (a:User)
        WITH a
        WHERE a.user_id < 10
        OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
        RETURN a.user_id, b.user_id
        LIMIT 10
        """,
        check_has_results()
    )
    
    # Test 2.2: OPTIONAL MATCH + WITH + aggregation
    test_query(
        result,
        "OPTIONAL MATCH + WITH aggregation",
        """
        MATCH (a:User)
        OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
        WITH a, COUNT(b) as follows_count
        WHERE follows_count >= 0
        RETURN a.user_id, follows_count
        LIMIT 10
        """,
        check_has_results()
    )
    
    # Test 2.3: VLP + WITH + OPTIONAL MATCH (complex combination)
    test_query(
        result,
        "VLP + WITH + OPTIONAL MATCH",
        """
        MATCH (a:User)-[:FOLLOWS*1..2]->(friend:User)
        WITH a, friend
        WHERE friend.user_id <> a.user_id
        OPTIONAL MATCH (friend)-[:FOLLOWS]->(other:User)
        WITH a, friend, COUNT(other) as others
        RETURN a.user_id, friend.user_id, others
        LIMIT 5
        """,
        check_multiple_ctes(2)
    )
    
    # ========================================================================
    # Category 3: Multiple Relationship Types + WITH
    # ========================================================================
    print("\n" + "="*70)
    print("CATEGORY 3: Multiple Relationship Types + WITH")
    print("="*70)
    
    # Test 3.1: Alternate rel types + WITH
    test_query(
        result,
        "Alternate relationship types + WITH",
        """
        MATCH (a:User)-[:FOLLOWS]->(b:User)
        WITH a, COUNT(DISTINCT b) as connections
        WHERE connections > 0
        RETURN a.user_id, connections
        LIMIT 5
        """,
        check_has_results()
    )
    
    # Test 3.2: Multiple patterns + WITH + aggregation
    test_query(
        result,
        "Multiple patterns + WITH aggregation",
        """
        MATCH (a:User)-[:FOLLOWS]->(b:User)
        WITH a, b
        MATCH (b)-[:FOLLOWS]->(c:User)
        WITH a, COUNT(DISTINCT c) as second_degree
        WHERE second_degree > 0
        RETURN a.user_id, second_degree
        LIMIT 5
        """,
        check_multiple_ctes(2)
    )
    
    # ========================================================================
    # Category 4: WITH + Complex Aggregations
    # ========================================================================
    print("\n" + "="*70)
    print("CATEGORY 4: WITH + Complex Aggregations")
    print("="*70)
    
    # Test 4.1: WITH TableAlias + aggregation (GROUP BY expansion)
    test_query(
        result,
        "WITH TableAlias + aggregation",
        """
        MATCH (a:User)-[:FOLLOWS]->(b:User)
        WITH a, COUNT(b) as cnt
        WHERE cnt > 0
        RETURN a.user_id, cnt
        LIMIT 5
        """,
        check_has_results()
    )
    
    # Test 4.2: Two-level aggregation
    test_query(
        result,
        "Two-level aggregation (WITH + RETURN)",
        """
        MATCH (a:User)-[:FOLLOWS]->(b:User)
        WITH a, COUNT(b) as follows
        WHERE follows > 0
        RETURN COUNT(a) as active_users, AVG(follows) as avg_follows
        """,
        check_has_results()
    )
    
    # Test 4.3: WITH + HAVING-like pattern
    test_query(
        result,
        "WITH filtering on aggregates",
        """
        MATCH (a:User)-[:FOLLOWS]->(b:User)
        WITH a.user_id as uid, COUNT(b) as follows
        WHERE follows > 1
        RETURN uid, follows
        ORDER BY follows DESC
        LIMIT 5
        """,
        check_has_results()
    )
    
    # ========================================================================
    # Category 5: WITH + Modifiers (ORDER BY, SKIP, LIMIT)
    # ========================================================================
    print("\n" + "="*70)
    print("CATEGORY 5: WITH + Query Modifiers")
    print("="*70)
    
    # Test 5.1: WITH + ORDER BY + LIMIT
    test_query(
        result,
        "WITH + ORDER BY + LIMIT",
        """
        MATCH (a:User)-[:FOLLOWS]->(b:User)
        WITH a, COUNT(b) as follows
        ORDER BY follows DESC
        LIMIT 3
        MATCH (a)-[:FOLLOWS]->(friend:User)
        RETURN a.user_id, follows, COUNT(friend) as verify
        """,
        check_has_results()
    )
    
    # Test 5.2: WITH + SKIP + LIMIT
    test_query(
        result,
        "WITH + SKIP + LIMIT",
        """
        MATCH (a:User)
        WITH a
        ORDER BY a.user_id
        SKIP 2
        LIMIT 5
        MATCH (a)-[:FOLLOWS]->(b:User)
        RETURN a.user_id, COUNT(b) as follows
        """,
        check_has_results()
    )
    
    # ========================================================================
    # Category 6: CTE Hoisting Edge Cases
    # ========================================================================
    print("\n" + "="*70)
    print("CATEGORY 6: CTE Hoisting Validation")
    print("="*70)
    
    # Test 6.1: Deeply nested WITH clauses
    test_query(
        result,
        "Three-level WITH nesting",
        """
        MATCH (a:User)
        WITH a WHERE a.user_id < 100
        WITH a WHERE a.user_id < 50
        WITH a WHERE a.user_id < 25
        MATCH (a)-[:FOLLOWS]->(b:User)
        RETURN a.user_id, COUNT(b) as follows
        LIMIT 5
        """,
        check_multiple_ctes(3)
    )
    
    # Test 6.2: VLP nested in WITH chain (tests CTE hoisting)
    test_query(
        result,
        "VLP within WITH chain",
        """
        MATCH (a:User)
        WITH a WHERE a.user_id < 50
        MATCH (a)-[:FOLLOWS*1..2]->(friend:User)
        WITH a, COUNT(DISTINCT friend) as reach
        WHERE reach > 0
        RETURN a.user_id, reach
        LIMIT 5
        """,
        check_has_recursive_cte()
    )
    
    # ========================================================================
    # Category 7: Parameters + WITH
    # ========================================================================
    print("\n" + "="*70)
    print("CATEGORY 7: Parameters + WITH")
    print("="*70)
    
    # Test 7.1: Parameter in WHERE before WITH
    test_query(
        result,
        "Parameter in WHERE before WITH",
        """
        MATCH (u:User)
        WHERE u.user_id = $userId
        WITH u, u.name as username
        RETURN username
        """,
        check_has_results(),
        parameters={"userId": 1}
    )
    
    # Test 7.2: Parameter in WITH WHERE clause
    test_query(
        result,
        "Parameter in WITH WHERE clause",
        """
        MATCH (u:User)-[:FOLLOWS]->(f:User)
        WITH u, COUNT(f) as following_count
        WHERE following_count > $minFollows
        RETURN u.user_id, following_count
        ORDER BY following_count DESC
        LIMIT 5
        """,
        check_has_results(),
        parameters={"minFollows": 0}
    )
    
    # Test 7.3: Multiple parameters across WITH boundary
    test_query(
        result,
        "Multiple parameters with WITH",
        """
        MATCH (u:User)
        WHERE u.user_id >= $minId AND u.user_id <= $maxId
        WITH u, u.name as name
        WHERE length(name) > $minNameLen
        RETURN u.user_id, name
        ORDER BY u.user_id
        LIMIT 5
        """,
        check_has_results(),
        parameters={"minId": 1, "maxId": 10, "minNameLen": 3}
    )
    
    # Test 7.4: Parameter in aggregation expression
    test_query(
        result,
        "Parameter in aggregation with WITH",
        """
        MATCH (u:User)-[:FOLLOWS]->(f:User)
        WITH u, COUNT(f) * $multiplier as weighted_follows
        RETURN u.user_id, weighted_follows
        ORDER BY weighted_follows DESC
        LIMIT 5
        """,
        check_has_results(),
        parameters={"multiplier": 2}
    )
    
    # Test 7.5: VLP + WITH + parameters
    test_query(
        result,
        "VLP + WITH + parameters",
        """
        MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)
        WHERE a.user_id = $startId
        WITH a, COUNT(DISTINCT b) as reachable
        WHERE reachable > $minReachable
        RETURN a.user_id, reachable
        """,
        check_has_recursive_cte(),
        parameters={"startId": 1, "minReachable": 0}
    )
    
    # ========================================================================
    # Category 8: Regression Tests
    # ========================================================================
    print("\n" + "="*70)
    print("CATEGORY 8: Regression Tests")
    print("="*70)
    
    # Test 8.1: LDBC IC-1 pattern (VLP + WITH + aggregation)
    test_query(
        result,
        "LDBC IC-1 pattern",
        """
        MATCH (p:User {user_id: 1})-[:FOLLOWS*1..3]-(friend:User)
        WITH friend, COUNT(*) as cnt
        WHERE cnt > 0
        RETURN friend.user_id, cnt
        LIMIT 5
        """,
        check_has_recursive_cte()
    )
    
    # Test 8.2: GROUP BY with TableAlias expansion
    test_query(
        result,
        "TableAlias GROUP BY expansion",
        """
        MATCH (a:User)-[:FOLLOWS]->(b:User)
        WITH a, b
        WITH a, COUNT(b) as connections
        RETURN a.user_id, connections
        LIMIT 5
        """,
        check_has_results()
    )
    
    return result.summary()


if __name__ == "__main__":
    # Check if server is running
    try:
        response = requests.get(f"{SERVER_URL}/health", timeout=2)
        if response.status_code != 200:
            print(f"⚠️  Warning: Server health check returned {response.status_code}")
    except Exception as e:
        print(f"❌ Error: Cannot connect to server at {SERVER_URL}")
        print(f"   Please start the server first:")
        print(f"   export CLICKHOUSE_URL=http://localhost:8123")
        print(f"   export GRAPH_CONFIG_PATH=./benchmarks/social_network/schemas/social_benchmark.yaml")
        print(f"   cargo run --bin clickgraph")
        sys.exit(1)
    
    print(f"\n🚀 Running tests against: {SERVER_URL}\n")
    success = run_tests()
    sys.exit(0 if success else 1)
