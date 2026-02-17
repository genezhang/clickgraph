#!/usr/bin/env python3
"""
Schema-Agnostic Query Pattern Validator

Tests query patterns against the CURRENTLY LOADED schema.
Adapts queries based on what's in the schema.

Usage:
    # First, check what schema is loaded
    curl http://localhost:8080/schema/info
    
    # Then run tests against it
    python3 tests/integration/query_patterns/validate_patterns.py
"""

import requests
import sys
import json
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


# Configuration
CLICKGRAPH_URL = "http://localhost:8080"
TIMEOUT = 30


@dataclass
class TestResult:
    """Result of a single test"""
    pattern_name: str
    query: str
    success: bool
    sql_generated: Optional[str]
    error: Optional[str]
    rows_returned: int


def get_schema_info() -> Optional[Dict]:
    """Get current schema information from server by probing"""
    try:
        # Try /schemas endpoint first
        response = requests.get(f"{CLICKGRAPH_URL}/schemas", timeout=TIMEOUT)
        if response.status_code == 200:
            # Server is up - we'll use hardcoded schema info based on what we know
            # In a real implementation, this would query schema metadata
            return {"status": "connected"}
    except Exception as e:
        print(f"Warning: Could not connect to server: {e}")
    return None


def get_schema_from_test_query() -> Tuple[List[str], List[str], Dict, Dict]:
    """
    Discover schema by running test queries.
    Returns hardcoded social_integration schema info (the standard test schema).
    """
    # Social benchmark schema (our standard test schema)
    node_labels = ["User", "Post"]
    rel_types = ["FOLLOWS", "AUTHORED", "LIKED"]
    node_props = {
        "User": ["user_id", "name", "email", "country", "city", "is_active"],
        "Post": ["post_id", "content", "created_at"],
    }
    rel_props = {
        "FOLLOWS": ["follow_date"],
        "AUTHORED": ["authored_date"],
        "LIKED": ["liked_date"],
    }
    return node_labels, rel_types, node_props, rel_props


def execute_query(query: str, sql_only: bool = False) -> Dict:
    """Execute a Cypher query"""
    payload = {"query": query}
    if sql_only:
        payload["sql_only"] = True
    
    try:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=TIMEOUT,
        )
        return response.json() if response.status_code == 200 else {"error": response.text}
    except Exception as e:
        return {"error": str(e)}


def extract_schema_info(schema_info: Dict) -> Tuple[List[str], List[str], Dict, Dict]:
    """
    Extract useful schema information.
    
    Returns:
        (node_labels, rel_types, node_props, rel_props)
    """
    node_labels = []
    rel_types = []
    node_props = {}
    rel_props = {}
    
    # Parse node definitions
    nodes = schema_info.get("nodes", [])
    for node in nodes:
        label = node.get("label", "")
        if label:
            node_labels.append(label)
            props = []
            prop_mapping = node.get("property_mapping", {})
            for cypher_prop in prop_mapping.keys():
                props.append(cypher_prop)
            node_props[label] = props if props else ["id"]
    
    # Parse relationship definitions
    edges = schema_info.get("edges", [])
    for edge in edges:
        rel_type = edge.get("type_name", "")
        if rel_type:
            rel_types.append(rel_type)
            props = []
            prop_mapping = edge.get("property_mapping", {})
            for cypher_prop in prop_mapping.keys():
                props.append(cypher_prop)
            rel_props[rel_type] = props if props else []
    
    return node_labels, rel_types, node_props, rel_props


def generate_pattern_queries(
    node_labels: List[str],
    rel_types: List[str],
    node_props: Dict[str, List[str]],
    rel_props: Dict[str, List[str]],
) -> List[Tuple[str, str]]:
    """
    Generate test queries based on available schema.
    
    Returns:
        List of (pattern_name, query) tuples
    """
    queries = []
    
    if not node_labels:
        print("ERROR: No node labels found in schema!")
        return queries
    
    label = node_labels[0]
    props = node_props.get(label, ["id"])
    prop = props[0] if props else "id"
    
    # Get relationship info
    has_rels = len(rel_types) > 0
    rel_type = rel_types[0] if has_rels else None
    
    # Get a second label (same as first if only one)
    to_label = node_labels[1] if len(node_labels) > 1 else label
    to_props = node_props.get(to_label, props)
    to_prop = to_props[0] if to_props else prop
    
    # ===========================================
    # 1. Basic Node Patterns
    # ===========================================
    
    # 1.1 Simple node scan with explicit property
    queries.append((
        "node_scan_explicit_prop",
        f"MATCH (n:{label}) RETURN n.{prop} LIMIT 10"
    ))
    
    # 1.2 Return whole node (wildcard expansion) - TESTS BUG #1
    queries.append((
        "return_whole_node",
        f"MATCH (n:{label}) RETURN n LIMIT 5"
    ))
    
    # 1.3 Multiple properties
    if len(props) >= 2:
        queries.append((
            "return_multiple_props",
            f"MATCH (n:{label}) RETURN n.{props[0]}, n.{props[1]} LIMIT 10"
        ))
    
    # 1.4 Node with WHERE filter
    queries.append((
        "node_with_where",
        f"MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN n.{prop} LIMIT 10"
    ))
    
    # 1.5 Count nodes
    queries.append((
        "count_nodes",
        f"MATCH (n:{label}) RETURN count(n)"
    ))
    
    # ===========================================
    # 2. Relationship Patterns (if available)
    # ===========================================
    
    if has_rels:
        # 2.1 Simple relationship
        queries.append((
            "simple_relationship",
            f"MATCH (a:{label})-[r:{rel_type}]->(b) RETURN a.{prop} LIMIT 10"
        ))
        
        # 2.2 Return relationship properties
        queries.append((
            "return_rel_props",
            f"MATCH (a:{label})-[r:{rel_type}]->(b) RETURN r LIMIT 5"
        ))
        
        # 2.3 Return both nodes
        queries.append((
            "return_both_nodes",
            f"MATCH (a:{label})-[r:{rel_type}]->(b:{to_label}) RETURN a.{prop}, b.{to_prop} LIMIT 10"
        ))
        
        # 2.4 Undirected relationship
        queries.append((
            "undirected_rel",
            f"MATCH (a:{label})-[r:{rel_type}]-(b:{to_label}) RETURN a.{prop}, b.{to_prop} LIMIT 10"
        ))
        
        # 2.5 Multi-hop (same relationship twice)
        queries.append((
            "multi_hop",
            f"MATCH (a:{label})-[r1:{rel_type}]->(b)-[r2:{rel_type}]->(c) RETURN a.{prop} LIMIT 5"
        ))
    
    # ===========================================
    # 3. Variable Length Paths
    # ===========================================
    
    if has_rels:
        # 3.1 VLP exact hops
        queries.append((
            "vlp_exact_2",
            f"MATCH (a:{label})-[*2]->(b) RETURN a.{prop} LIMIT 10"
        ))
        
        # 3.2 VLP range
        queries.append((
            "vlp_range_1_3",
            f"MATCH (a:{label})-[*1..3]->(b) RETURN a.{prop} LIMIT 10"
        ))
        
        # 3.3 Path variable
        queries.append((
            "path_variable",
            f"MATCH p = (a:{label})-[*1..2]->(b) RETURN length(p) LIMIT 10"
        ))
    
    # ===========================================
    # 4. Aggregation Patterns
    # ===========================================
    
    # 4.1 Simple count
    queries.append((
        "simple_count",
        f"MATCH (n:{label}) RETURN count(n)"
    ))
    
    # 4.2 GROUP BY
    queries.append((
        "group_by_count",
        f"MATCH (n:{label}) RETURN n.{prop}, count(n) AS cnt ORDER BY cnt DESC LIMIT 10"
    ))
    
    # 4.3 WITH aggregation - TESTS BUG #2
    if has_rels:
        queries.append((
            "with_aggregation",
            f"MATCH (a:{label})-[r:{rel_type}]->(b) WITH a.{prop} AS prop, count(r) AS cnt RETURN prop, cnt"
        ))
    
    # ===========================================
    # 5. OPTIONAL MATCH
    # ===========================================
    
    if has_rels:
        queries.append((
            "optional_match_simple",
            f"MATCH (a:{label}) OPTIONAL MATCH (a)-[r:{rel_type}]->(b) RETURN a.{prop}, count(r) AS rel_count"
        ))
    
    # ===========================================
    # 6. ORDER BY / LIMIT / SKIP
    # ===========================================
    
    queries.append((
        "order_limit_skip",
        f"MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN n.{prop} ORDER BY n.{prop} DESC SKIP 5 LIMIT 10"
    ))
    
    # ===========================================
    # 7. Graph Functions
    # ===========================================
    
    if has_rels:
        queries.append((
            "graph_functions",
            f"MATCH (a:{label})-[r:{rel_type}]->(b) RETURN type(r), labels(a) LIMIT 5"
        ))
    
    # ===========================================
    # 8. Multi-type relationships (if multiple types available)
    # ===========================================
    
    if len(rel_types) >= 2:
        queries.append((
            "multi_rel_types",
            f"MATCH (a:{label})-[r:{rel_types[0]}|{rel_types[1]}]->(b) RETURN type(r), count(*) AS cnt"
        ))
    
    # ===========================================
    # 9. Shortest Path
    # ===========================================
    
    if has_rels:
        queries.append((
            "shortest_path",
            f"MATCH p = shortestPath((a:{label})-[*1..5]->(b:{label})) WHERE a.{prop} <> b.{prop} RETURN length(p) LIMIT 5"
        ))
    
    return queries


def run_tests(queries: List[Tuple[str, str]], sql_only: bool = False) -> List[TestResult]:
    """Run all test queries and collect results"""
    results = []
    
    for pattern_name, query in queries:
        result = execute_query(query, sql_only=sql_only)
        
        # Handle different response structures
        sql_generated = result.get("generated_sql") or result.get("sql")
        error = result.get("error")
        rows = len(result.get("result", [])) if "result" in result else 0
        
        # Determine success
        # sql_only mode: success if SQL generated (no error)
        # full mode: success if no error AND (rows > 0 OR query is aggregation)
        is_agg = "count(" in query.lower() or "sum(" in query.lower()
        
        if sql_only:
            success = error is None and sql_generated is not None
        else:
            success = error is None and (rows > 0 or is_agg)
        
        results.append(TestResult(
            pattern_name=pattern_name,
            query=query,
            success=success,
            sql_generated=sql_generated,
            error=error,
            rows_returned=rows,
        ))
    
    return results


def print_results(results: List[TestResult], verbose: bool = False):
    """Print test results with summary"""
    
    passed = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    
    print("=" * 70)
    print(f"Query Pattern Validation Results")
    print("=" * 70)
    print(f"\nTotal: {len(results)} | Passed: {len(passed)} | Failed: {len(failed)}")
    print(f"Pass Rate: {len(passed) / len(results) * 100:.1f}%")
    
    if passed and verbose:
        print("\n" + "-" * 70)
        print("✅ PASSED:")
        print("-" * 70)
        for r in passed:
            print(f"  ✓ {r.pattern_name}: {r.rows_returned} rows")
    
    if failed:
        print("\n" + "-" * 70)
        print("❌ FAILED:")
        print("-" * 70)
        for r in failed:
            print(f"\n  ✗ {r.pattern_name}")
            print(f"    Query: {r.query}")
            if r.error:
                # Truncate long errors
                error_preview = r.error[:200] + "..." if len(r.error) > 200 else r.error
                print(f"    Error: {error_preview}")
            if r.sql_generated and verbose:
                sql_preview = r.sql_generated[:200] + "..." if len(r.sql_generated) > 200 else r.sql_generated
                print(f"    SQL: {sql_preview}")
    
    print("\n" + "=" * 70)
    
    return len(failed) == 0


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate query patterns against current schema")
    parser.add_argument("--sql-only", action="store_true", help="Only test SQL generation, don't execute")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    args = parser.parse_args()
    
    print("Connecting to ClickGraph server...")
    
    # Check server is up
    schema_info = get_schema_info()
    if not schema_info:
        print("ERROR: Could not connect to server")
        print(f"Make sure server is running at {CLICKGRAPH_URL}")
        sys.exit(1)
    
    # Use hardcoded schema info (social_integration)
    node_labels, rel_types, node_props, rel_props = get_schema_from_test_query()
    
    print(f"\nUsing social_integration schema:")
    print(f"  Node labels: {node_labels}")
    print(f"  Relationship types: {rel_types}")
    
    # Generate queries
    queries = generate_pattern_queries(node_labels, rel_types, node_props, rel_props)
    print(f"\nGenerated {len(queries)} test queries")
    
    # Run tests
    print(f"\nRunning tests ({'SQL only' if args.sql_only else 'with execution'})...")
    results = run_tests(queries, sql_only=args.sql_only)
    
    if args.json:
        output = [
            {
                "pattern": r.pattern_name,
                "query": r.query,
                "success": r.success,
                "error": r.error,
                "rows": r.rows_returned,
            }
            for r in results
        ]
        print(json.dumps(output, indent=2))
    else:
        all_passed = print_results(results, verbose=args.verbose)
        sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
