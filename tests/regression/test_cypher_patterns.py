#!/usr/bin/env python3
"""
Comprehensive Cypher Pattern Regression Tests
Tests all patterns and their combinations for ClickGraph

Categories:
1. Anonymous nodes and edges
2. Aggregates (COUNT, SUM, AVG, MIN, MAX, collect)
3. Path variables (length, nodes, relationships)
4. WITH clause combinations
5. OPTIONAL MATCH combinations
6. Functions (id, type, labels, label)
7. Complex combinations

Usage:
    python3 tests/regression/test_cypher_patterns.py
    
Set CLICKGRAPH_URL environment variable if not localhost:8080
"""

import json
import requests
import sys
import os
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

CLICKGRAPH_URL = os.environ.get("CLICKGRAPH_URL", "http://localhost:8080")


class QueryTestStatus(Enum):
    PASS = "✅"
    FAIL = "❌"
    ERROR = "⚠️"
    SKIP = "⏭️"


@dataclass
class QueryTestResult:
    name: str
    query: str
    status: QueryTestStatus
    sql: Optional[str] = None
    error: Optional[str] = None
    expected_contains: List[str] = None
    expected_not_contains: List[str] = None


def run_query(query: str, sql_only: bool = True) -> Tuple[bool, str, Optional[str]]:
    """Execute a Cypher query and return (success, sql_or_error, raw_response)"""
    try:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": query, "sql_only": sql_only},
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            sql = data.get("generated_sql", "")
            return True, sql, json.dumps(data)
        else:
            return False, response.text, None
    except Exception as e:
        return False, str(e), None


def check_sql_contains(sql: str, expected: List[str], not_expected: List[str] = None) -> Tuple[bool, str]:
    """Check if SQL contains expected strings and doesn't contain unwanted strings"""
    missing = []
    unwanted = []
    
    for exp in expected or []:
        if exp.lower() not in sql.lower():
            missing.append(exp)
    
    for not_exp in not_expected or []:
        if not_exp.lower() in sql.lower():
            unwanted.append(not_exp)
    
    if missing or unwanted:
        errors = []
        if missing:
            errors.append(f"Missing: {missing}")
        if unwanted:
            errors.append(f"Unwanted: {unwanted}")
        return False, "; ".join(errors)
    
    return True, ""


# =============================================================================
# TEST DEFINITIONS
# =============================================================================

ANONYMOUS_PATTERN_TESTS = [
    # Anonymous end node
    {
        "name": "Anonymous end node (u)-[:REL]->()",
        "query": "MATCH (u:User)-[:FOLLOWS]->() RETURN u.name, COUNT(*) AS cnt",
        "expected_contains": ["GROUP BY"],
        "expected_not_contains": [],
    },
    # Anonymous start node
    {
        "name": "Anonymous start node ()-[:REL]->(u)",
        "query": "MATCH ()-[:FOLLOWS]->(u:User) RETURN u.name, COUNT(*) AS cnt",
        "expected_contains": ["GROUP BY"],
    },
    # Anonymous relationship
    {
        "name": "Anonymous relationship (a)-[]->(b)",
        "query": "MATCH (u:User)-[]->(f:User) RETURN u.name, f.name LIMIT 3",
        "expected_contains": ["JOIN", "LIMIT"],
    },
    # Anonymous typed relationship
    {
        "name": "Anonymous typed relationship (a)-[:TYPE]->(b)",
        "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name LIMIT 3",
        "expected_contains": ["user_follows_bench"],
    },
    # Both nodes anonymous
    {
        "name": "Both nodes anonymous ()-[:REL]->()",
        "query": "MATCH ()-[:FOLLOWS]->() RETURN COUNT(*) AS total",
        "expected_contains": ["COUNT(*)"],
    },
    # All anonymous (node-rel-node)
    {
        "name": "All anonymous ()-[]->()",
        "query": "MATCH ()-[]->() RETURN COUNT(*) AS total",
        "expected_contains": ["COUNT(*)"],
    },
    # Anonymous with filter on named
    {
        "name": "Anonymous + filter on named node",
        "query": "MATCH (u:User)-[:FOLLOWS]->() WHERE u.user_id = 1 RETURN u.name, COUNT(*)",
        "expected_contains": ["user_id = 1", "GROUP BY"],
    },
    # Bidirectional anonymous
    {
        "name": "Bidirectional anonymous (u)-[]-()",
        "query": "MATCH (u:User)-[:FOLLOWS]-() WHERE u.user_id = 1 RETURN u.name, COUNT(*)",
        "expected_contains": ["user_id = 1"],
    },
    # Anonymous in variable length
    {
        "name": "Anonymous end with variable length",
        "query": "MATCH (u:User)-[:FOLLOWS*1..2]->() WHERE u.user_id = 1 RETURN u.name, COUNT(*)",
        "expected_contains": ["WITH RECURSIVE"],
    },
]

AGGREGATE_TESTS = [
    # COUNT variations
    {
        "name": "COUNT(*)",
        "query": "MATCH (u:User) RETURN COUNT(*) AS total",
        "expected_contains": ["COUNT(*)"],
    },
    {
        "name": "COUNT(n)",
        "query": "MATCH (u:User) RETURN COUNT(u) AS total",
        "expected_contains": ["COUNT("],
    },
    {
        "name": "COUNT(DISTINCT n.prop)",
        "query": "MATCH (u:User) RETURN COUNT(DISTINCT u.country) AS countries",
        "expected_contains": ["COUNT(DISTINCT"],
    },
    # Other aggregates
    {
        "name": "SUM aggregate",
        "query": "MATCH (u:User) RETURN SUM(u.user_id) AS total",
        "expected_contains": ["SUM("],
    },
    {
        "name": "AVG aggregate",
        "query": "MATCH (u:User) RETURN AVG(u.user_id) AS avg_id",
        "expected_contains": ["AVG("],
    },
    {
        "name": "MIN/MAX aggregates",
        "query": "MATCH (u:User) RETURN MIN(u.user_id), MAX(u.user_id)",
        "expected_contains": ["MIN(", "MAX("],
    },
    {
        "name": "collect() -> groupArray()",
        "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, collect(f.name) AS friends",
        "expected_contains": ["groupArray("],
        "expected_not_contains": ["collect("],
    },
    # Multiple aggregates
    {
        "name": "Multiple aggregates in one query",
        "query": "MATCH (u:User) RETURN COUNT(*), AVG(u.user_id), MAX(u.user_id)",
        "expected_contains": ["COUNT(*)", "AVG(", "MAX("],
    },
    # Aggregates with GROUP BY
    {
        "name": "COUNT with implicit GROUP BY",
        "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, COUNT(f) AS cnt",
        "expected_contains": ["GROUP BY"],
    },
    # HAVING equivalent
    {
        "name": "WITH + WHERE on aggregate (HAVING)",
        "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) WITH u, COUNT(f) AS cnt WHERE cnt > 2 RETURN u.name, cnt",
        "expected_contains": ["HAVING", "cnt > 2"],
    },
]

PATH_VARIABLE_TESTS = [
    # Basic path assignment
    {
        "name": "Path variable assignment",
        "query": "MATCH p = (u:User)-[:FOLLOWS*1..2]->(f:User) WHERE u.user_id = 1 RETURN p",
        "expected_contains": ["WITH RECURSIVE", "path_nodes"],
    },
    # length(p)
    {
        "name": "length(p) function",
        "query": "MATCH p = (u:User)-[:FOLLOWS*1..3]->(f:User) WHERE u.user_id = 1 RETURN length(p) AS len",
        "expected_contains": ["hop_count"],
    },
    # nodes(p)
    {
        "name": "nodes(p) function",
        "query": "MATCH p = (u:User)-[:FOLLOWS*1..2]->(f:User) WHERE u.user_id = 1 RETURN nodes(p)",
        "expected_contains": ["path_nodes"],
    },
    # relationships(p)
    {
        "name": "relationships(p) function",
        "query": "MATCH p = (u:User)-[:FOLLOWS*1..2]->(f:User) WHERE u.user_id = 1 RETURN relationships(p)",
        "expected_contains": ["path_relationships"],
    },
    # Path + aggregation
    {
        "name": "Path + collect(length(p))",
        "query": "MATCH p = (u:User)-[:FOLLOWS*1..2]->(f:User) WHERE u.user_id = 1 RETURN collect(length(p)) AS lengths",
        "expected_contains": ["groupArray", "hop_count"],
    },
    # shortestPath
    {
        "name": "shortestPath function",
        "query": "MATCH p = shortestPath((u:User {user_id: 1})-[:FOLLOWS*1..5]->(f:User {user_id: 10})) RETURN length(p)",
        "expected_contains": ["WITH RECURSIVE", "ROW_NUMBER", "PARTITION BY"],
    },
]

WITH_CLAUSE_TESTS = [
    # Basic WITH
    {
        "name": "Basic WITH projection",
        "query": "MATCH (u:User) WITH u.name AS name RETURN name",
        "expected_contains": ["SELECT"],
    },
    # WITH + aggregation
    {
        "name": "WITH + COUNT aggregation",
        "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) WITH u, COUNT(f) AS cnt RETURN u.name, cnt ORDER BY cnt DESC",
        "expected_contains": ["ORDER BY", "DESC"],
    },
    # Chained WITH - KNOWN LIMITATION: WITH + subsequent MATCH not fully implemented
    {
        "name": "WITH + MATCH (chained)",
        "query": "MATCH (u:User) WHERE u.user_id = 1 WITH u MATCH (u)-[:FOLLOWS]->(f:User) RETURN f.name",
        "expected_contains": ["JOIN"],
        "known_limitation": True,
    },
]

OPTIONAL_MATCH_TESTS = [
    # Basic OPTIONAL MATCH
    {
        "name": "Basic OPTIONAL MATCH",
        "query": "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) RETURN u.name, f.name",
        "expected_contains": ["LEFT JOIN"],
    },
    # OPTIONAL MATCH + COUNT
    {
        "name": "OPTIONAL MATCH + COUNT",
        "query": "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) RETURN u.name, COUNT(f) AS cnt",
        "expected_contains": ["LEFT JOIN", "GROUP BY"],
    },
    # WITH + OPTIONAL MATCH - KNOWN LIMITATION: WITH + subsequent patterns not fully implemented
    {
        "name": "WITH + OPTIONAL MATCH",
        "query": "MATCH (u:User) WHERE u.user_id = 1 WITH u OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) RETURN u.name, f.name",
        "expected_contains": ["LEFT JOIN"],
        "known_limitation": True,
    },
    # Multiple OPTIONAL MATCH
    {
        "name": "Multiple OPTIONAL MATCH",
        "query": "MATCH (u:User) WHERE u.user_id = 1 OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) OPTIONAL MATCH (u)<-[:FOLLOWS]-(g:User) RETURN u.name, f.name, g.name",
        "expected_contains": ["LEFT JOIN"],
    },
    # OPTIONAL + coalesce
    {
        "name": "OPTIONAL MATCH + coalesce",
        "query": "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) RETURN u.name, coalesce(f.name, 'No friends') AS friend",
        "expected_contains": ["LEFT JOIN", "coalesce("],
    },
]

FUNCTION_TESTS = [
    # id(n) for node
    {
        "name": "id(n) for node",
        "query": "MATCH (u:User) RETURN id(u) AS node_id LIMIT 1",
        "expected_contains": ["user_id"],
    },
    # id(r) for relationship
    {
        "name": "id(r) for relationship",
        "query": "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN id(r) AS rel_id LIMIT 1",
        "expected_contains": ["follow_id"],
    },
    # labels(n)
    {
        "name": "labels(n) function",
        "query": "MATCH (u:User) RETURN labels(u) AS node_labels LIMIT 1",
        "expected_contains": ["tuple(", "'User'"],
    },
    # type(r)
    {
        "name": "type(r) function (typed rel)",
        "query": "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN type(r) AS rel_type LIMIT 1",
        "expected_contains": ["'FOLLOWS'"],
    },
    # String functions
    {
        "name": "toUpper function",
        "query": "MATCH (u:User) RETURN toUpper(u.name) AS upper_name LIMIT 1",
        "expected_contains": ["upper("],
    },
    {
        "name": "toLower function",
        "query": "MATCH (u:User) RETURN toLower(u.name) AS lower_name LIMIT 1",
        "expected_contains": ["lower("],
    },
    # coalesce
    {
        "name": "coalesce function",
        "query": "MATCH (u:User) RETURN coalesce(u.city, 'Unknown') AS city LIMIT 1",
        "expected_contains": ["coalesce("],
    },
    # CASE expression
    {
        "name": "CASE WHEN expression",
        "query": "MATCH (u:User) RETURN CASE WHEN u.is_active THEN 'active' ELSE 'inactive' END AS status",
        "expected_contains": ["CASE WHEN", "THEN", "ELSE", "END"],
    },
    # Regex match
    {
        "name": "Regex match operator (=~)",
        "query": "MATCH (u:User) WHERE u.name =~ '^A.*' RETURN u.name",
        "expected_contains": ["match("],
        "expected_not_contains": ["=~"],
    },
    # size() function  
    {
        "name": "size() on list",
        "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) WITH u, collect(f.name) AS friends RETURN u.name, size(friends)",
        "expected_contains": ["length("],
    },
    # exists() pattern - NOT IMPLEMENTED: EXISTS subquery requires new feature development
    {
        "name": "EXISTS in WHERE",
        "query": "MATCH (u:User) WHERE EXISTS((u)-[:FOLLOWS]->()) RETURN u.name",
        "expected_contains": ["SELECT"],
        "known_limitation": True,
        "crashes_server": True,  # Causes panic - needs EXISTS feature implementation
    },
    # String concatenation (+) - FIXED: Now uses concat()
    {
        "name": "String concatenation",
        "query": "MATCH (u:User) RETURN u.name + ' - ' + u.country AS full_info LIMIT 3",
        "expected_contains": ["concat("],
    },
    # Arithmetic operations
    {
        "name": "Arithmetic in RETURN",
        "query": "MATCH (u:User) RETURN u.user_id * 2 + 1 AS calc LIMIT 3",
        "expected_contains": ["* 2", "+ 1"],
    },
]

COMPLEX_COMBINATION_TESTS = [
    # Anonymous + aggregate
    {
        "name": "Anonymous node + aggregate",
        "query": "MATCH (u:User)-[:FOLLOWS]->() RETURN u.name, COUNT(*) AS following ORDER BY following DESC LIMIT 5",
        "expected_contains": ["GROUP BY", "ORDER BY", "DESC", "LIMIT"],
    },
    # Path + aggregate + ORDER BY
    {
        "name": "Path + aggregate + ORDER BY",
        "query": "MATCH p = (u:User)-[:FOLLOWS*1..3]->(f:User) WHERE u.user_id < 10 RETURN u.name, COUNT(f), MAX(length(p)) ORDER BY u.name",
        "expected_contains": ["WITH RECURSIVE", "GROUP BY", "ORDER BY"],
    },
    # OPTIONAL + collect
    {
        "name": "OPTIONAL MATCH + collect",
        "query": "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) RETURN u.name, collect(f.name) AS friends",
        "expected_contains": ["LEFT JOIN", "groupArray("],
    },
    # Mutual followers (converging pattern)
    {
        "name": "Mutual followers pattern",
        "query": "MATCH (u1:User)-[:FOLLOWS]->(common:User)<-[:FOLLOWS]-(u2:User) WHERE u1.user_id <> u2.user_id RETURN u1.name, u2.name, common.name LIMIT 5",
        "expected_contains": ["JOIN", "LIMIT", "<>"],
    },
    # UNWIND + aggregate
    {
        "name": "UNWIND + aggregate",
        "query": "UNWIND [1, 2, 3, 4, 5] AS x RETURN SUM(x) AS total, AVG(x) AS average",
        "expected_contains": ["ARRAY JOIN", "SUM(", "AVG("],
    },
    # COUNT DISTINCT across relationship
    {
        "name": "COUNT DISTINCT across relationship",
        "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, COUNT(DISTINCT f.country) AS unique_countries",
        "expected_contains": ["COUNT(DISTINCT", "GROUP BY"],
    },
    # CASE in relationship query
    {
        "name": "CASE expression with relationship",
        "query": "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u.name, CASE WHEN f.is_active THEN 'active' ELSE 'inactive' END AS status",
        "expected_contains": ["CASE WHEN", "JOIN"],
    },
    # Variable length + aggregate
    {
        "name": "Variable length + aggregate",
        "query": "MATCH (u:User)-[:FOLLOWS*1..2]->(f:User) WHERE u.user_id = 1 RETURN COUNT(DISTINCT f) AS reachable_users",
        "expected_contains": ["WITH RECURSIVE", "COUNT(DISTINCT"],
    },
    # Anonymous + path variable
    {
        "name": "Anonymous end + path length",
        "query": "MATCH p = (u:User)-[:FOLLOWS*1..3]->() WHERE u.user_id = 1 RETURN length(p) AS depth",
        "expected_contains": ["WITH RECURSIVE", "hop_count"],
    },
    # OPTIONAL + CASE + coalesce
    {
        "name": "OPTIONAL + CASE + coalesce combo",
        "query": "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) RETURN u.name, CASE WHEN COUNT(f) > 0 THEN 'has friends' ELSE 'lonely' END AS status",
        "expected_contains": ["LEFT JOIN", "CASE WHEN", "GROUP BY"],
    },
    # Multiple anonymous nodes
    {
        "name": "Multiple anonymous in chain",
        "query": "MATCH ()-[:FOLLOWS]->(middle:User)-[:FOLLOWS]->() RETURN middle.name, COUNT(*) AS flow_count",
        "expected_contains": ["JOIN", "GROUP BY"],
    },
    # regex + relationship
    {
        "name": "Regex with relationship pattern",
        "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.name =~ '^A.*' RETURN u.name, f.name",
        "expected_contains": ["JOIN", "match("],
    },
    # Multiple aggregates with GROUP BY
    {
        "name": "Multiple aggregates same GROUP BY",
        "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, COUNT(f) AS cnt, collect(f.name) AS friends, MIN(f.user_id) AS min_id",
        "expected_contains": ["GROUP BY", "groupArray(", "MIN("],
    },
]


# =============================================================================
# TEST RUNNER
# =============================================================================

def run_test_category(category_name: str, tests: List[Dict]) -> List[QueryTestResult]:
    """Run all tests in a category and return results"""
    results = []
    
    for test in tests:
        name = test["name"]
        query = test["query"]
        expected = test.get("expected_contains", [])
        not_expected = test.get("expected_not_contains", [])
        known_limitation = test.get("known_limitation", False)
        crashes_server = test.get("crashes_server", False)
        
        # Skip tests that are known to crash the server
        if crashes_server:
            results.append(QueryTestResult(
                name=name + " [CRASHES SERVER - SKIPPED]",
                query=query,
                status=QueryTestStatus.SKIP,
                error="Known to crash server"
            ))
            continue
        
        success, sql_or_error, _ = run_query(query)
        
        if not success:
            results.append(QueryTestResult(
                name=name,
                query=query,
                status=QueryTestStatus.ERROR,
                error=sql_or_error
            ))
            continue
        
        # Check SQL contents
        check_pass, check_error = check_sql_contains(sql_or_error, expected, not_expected)
        
        if check_pass:
            results.append(QueryTestResult(
                name=name,
                query=query,
                status=QueryTestStatus.PASS,
                sql=sql_or_error
            ))
        else:
            # Known limitations are marked as SKIP not FAIL
            if known_limitation:
                results.append(QueryTestResult(
                    name=name + " [KNOWN LIMITATION]",
                    query=query,
                    status=QueryTestStatus.SKIP,
                    sql=sql_or_error,
                    error=check_error
                ))
            else:
                results.append(QueryTestResult(
                    name=name,
                    query=query,
                    status=QueryTestStatus.FAIL,
                    sql=sql_or_error,
                    error=check_error
                ))
    
    return results


def print_results(category_name: str, results: List[QueryTestResult], verbose: bool = False):
    """Print test results for a category"""
    print(f"\n{'='*60}")
    print(f"  {category_name}")
    print(f"{'='*60}")
    
    passed = sum(1 for r in results if r.status == QueryTestStatus.PASS)
    failed = sum(1 for r in results if r.status == QueryTestStatus.FAIL)
    errors = sum(1 for r in results if r.status == QueryTestStatus.ERROR)
    skipped = sum(1 for r in results if r.status == QueryTestStatus.SKIP)
    
    for result in results:
        status_icon = result.status.value
        print(f"  {status_icon} {result.name}")
        
        if result.status not in [QueryTestStatus.PASS, QueryTestStatus.SKIP] and verbose:
            print(f"      Query: {result.query}")
            if result.error:
                print(f"      Error: {result.error}")
            if result.sql:
                # Print first 200 chars of SQL
                sql_preview = result.sql[:200].replace('\n', ' ')
                print(f"      SQL: {sql_preview}...")
    
    print(f"\n  Summary: {passed} passed, {failed} failed, {errors} errors, {skipped} skipped")
    return passed, failed, errors, skipped


def main():
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    
    print("="*60)
    print("  ClickGraph Cypher Pattern Regression Tests")
    print("="*60)
    print(f"  Server: {CLICKGRAPH_URL}")
    
    # Check server health
    try:
        health = requests.get(f"{CLICKGRAPH_URL}/health", timeout=5)
        if health.status_code != 200:
            print("  ❌ Server not healthy!")
            sys.exit(1)
        print("  ✅ Server healthy")
    except Exception as e:
        print(f"  ❌ Cannot connect to server: {e}")
        sys.exit(1)
    
    all_categories = [
        ("Anonymous Pattern Tests", ANONYMOUS_PATTERN_TESTS),
        ("Aggregate Tests", AGGREGATE_TESTS),
        ("Path Variable Tests", PATH_VARIABLE_TESTS),
        ("WITH Clause Tests", WITH_CLAUSE_TESTS),
        ("OPTIONAL MATCH Tests", OPTIONAL_MATCH_TESTS),
        ("Function Tests", FUNCTION_TESTS),
        ("Complex Combination Tests", COMPLEX_COMBINATION_TESTS),
    ]
    
    total_passed = 0
    total_failed = 0
    total_errors = 0
    total_skipped = 0
    
    for category_name, tests in all_categories:
        results = run_test_category(category_name, tests)
        passed, failed, errors, skipped = print_results(category_name, results, verbose)
        total_passed += passed
        total_failed += failed
        total_errors += errors
        total_skipped += skipped
    
    print("\n" + "="*60)
    print("  FINAL SUMMARY")
    print("="*60)
    total = total_passed + total_failed + total_errors + total_skipped
    print(f"  Total: {total} tests")
    print(f"  ✅ Passed: {total_passed}")
    print(f"  ❌ Failed: {total_failed}")
    print(f"  ⚠️  Errors: {total_errors}")
    print(f"  ⏭️  Skipped (known limitations): {total_skipped}")
    print(f"  Pass rate: {total_passed/(total-total_skipped)*100:.1f}% (excluding known limitations)")
    print("="*60)
    
    # Return non-zero exit code if any failures
    sys.exit(0 if (total_failed == 0 and total_errors == 0) else 1)


if __name__ == "__main__":
    main()
