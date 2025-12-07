#!/usr/bin/env python3
"""
Compare v1 vs v2 graph_join_inference paths.

This script runs queries against the server and compares SQL generation
between v1 (default) and v2 (USE_PATTERN_SCHEMA_V2=1) paths.

Usage:
    # Start server with v1 (default)
    ./target/release/clickgraph &
    python scripts/test/compare_v1_v2.py --mode v1 --save

    # Restart server with v2
    USE_PATTERN_SCHEMA_V2=1 ./target/release/clickgraph &
    python scripts/test/compare_v1_v2.py --mode v2 --compare
"""

import argparse
import json
import os
import requests
import sys
from pathlib import Path
from typing import Dict, List, Tuple

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
RESULTS_DIR = Path("/tmp/clickgraph_v1_v2_compare")


# Test queries covering various patterns
TEST_QUERIES = {
    # Basic patterns
    "simple_node": "MATCH (u:User) RETURN u.name LIMIT 5",
    "node_with_filter": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name",
    
    # Single-hop relationships
    "single_hop_directed": "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u.name, f.name LIMIT 5",
    "single_hop_with_filter": "MATCH (u:User)-[r:FOLLOWS]->(f:User) WHERE u.user_id = 1 RETURN f.name",
    
    # Multi-hop patterns (this is what v2 should fix)
    "two_hop_directed": """
        MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User)
        WHERE a.user_id = 1
        RETURN a.name, b.name, c.name
        LIMIT 10
    """,
    "two_hop_anonymous": """
        MATCH (u1:User)-[:FOLLOWS]->()-[:FOLLOWS]->(u2:User)
        WHERE u1.user_id = 1
        RETURN DISTINCT u2.name
        LIMIT 10
    """,
    "three_hop_directed": """
        MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)-[:FOLLOWS]->(d:User)
        WHERE a.user_id = 1
        RETURN a.name, b.name, c.name, d.name
        LIMIT 5
    """,
    
    # Bidirectional (cycle detection)
    "bidirectional_cycle": """
        MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u1)
        RETURN u1.name, u2.name
        LIMIT 10
    """,
    
    # Aggregation with multi-hop
    "two_hop_with_count": """
        MATCH (u:User)-[:FOLLOWS]->(f:User)-[:FOLLOWS]->(fof:User)
        WHERE u.user_id = 1
        RETURN COUNT(DISTINCT fof) as fof_count
    """,
    
    # VLP patterns
    "vlp_unlimited": """
        MATCH (u:User)-[:FOLLOWS*]->(f:User)
        WHERE u.user_id = 1
        RETURN DISTINCT f.name
        LIMIT 10
    """,
    "vlp_bounded": """
        MATCH (u:User)-[:FOLLOWS*1..3]->(f:User)
        WHERE u.user_id = 1
        RETURN DISTINCT f.name
        LIMIT 10
    """,
    "vlp_exact": """
        MATCH (u:User)-[:FOLLOWS*2]->(f:User)
        WHERE u.user_id = 1
        RETURN DISTINCT f.name
        LIMIT 10
    """,
    
    # VLP + chained
    "vlp_chained": """
        MATCH (u:User)-[:FOLLOWS*]->(f:User)-[:LIKES]->(p:Post)
        WHERE u.user_id = 1
        RETURN u.name, f.name, p.title
        LIMIT 10
    """,
}


def get_sql(query: str, schema_name: str = None) -> Tuple[str, bool, str]:
    """Get generated SQL for a query. Returns (sql, success, error)."""
    payload = {"query": query, "sql_only": True}
    if schema_name:
        payload["schema_name"] = schema_name
    
    try:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        result = response.json()
        
        if "error" in result:
            return "", False, result["error"]
        
        sql = result.get("generated_sql", result.get("sql", ""))
        return sql, True, ""
    except Exception as e:
        return "", False, str(e)


def execute_query(query: str, schema_name: str = None) -> Tuple[List[Dict], bool, str]:
    """Execute a query and return results. Returns (data, success, error)."""
    payload = {"query": query}
    if schema_name:
        payload["schema_name"] = schema_name
    
    try:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        result = response.json()
        
        if "error" in result:
            return [], False, result["error"]
        
        data = result.get("data", result.get("results", []))
        return data, True, ""
    except Exception as e:
        return [], False, str(e)


def save_results(mode: str, results: Dict):
    """Save results to file."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    filepath = RESULTS_DIR / f"{mode}_results.json"
    with open(filepath, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {filepath}")


def load_results(mode: str) -> Dict:
    """Load results from file."""
    filepath = RESULTS_DIR / f"{mode}_results.json"
    if not filepath.exists():
        return {}
    with open(filepath) as f:
        return json.load(f)


def run_tests(mode: str, sql_only: bool = True) -> Dict:
    """Run all test queries and collect results."""
    print(f"\n{'='*60}")
    print(f" Running tests with {mode.upper()} path")
    print(f"{'='*60}\n")
    
    results = {}
    
    for name, query in TEST_QUERIES.items():
        print(f"Testing: {name}...", end=" ")
        
        # Get SQL
        sql, sql_ok, sql_err = get_sql(query.strip())
        
        # Execute if SQL generation succeeded and sql_only is False
        if sql_ok and not sql_only:
            data, exec_ok, exec_err = execute_query(query.strip())
        else:
            data, exec_ok, exec_err = [], sql_ok, sql_err if not sql_ok else ""
        
        results[name] = {
            "query": query.strip(),
            "sql": sql,
            "sql_ok": sql_ok,
            "sql_error": sql_err,
            "data": data,
            "exec_ok": exec_ok if not sql_only else sql_ok,
            "exec_error": exec_err,
            "row_count": len(data) if exec_ok and not sql_only else 0
        }
        
        if sql_ok:
            print(f"✅ SQL generated")
        else:
            print(f"❌ {sql_err[:50]}")
    
    return results


def compare_results(v1: Dict, v2: Dict):
    """Compare v1 and v2 results."""
    print(f"\n{'='*60}")
    print(" COMPARISON: v1 vs v2")
    print(f"{'='*60}\n")
    
    all_keys = set(v1.keys()) | set(v2.keys())
    
    improvements = []
    regressions = []
    unchanged = []
    
    for name in sorted(all_keys):
        r1 = v1.get(name, {})
        r2 = v2.get(name, {})
        
        v1_ok = r1.get("sql_ok", False) and r1.get("exec_ok", False)
        v2_ok = r2.get("sql_ok", False) and r2.get("exec_ok", False)
        
        v1_rows = r1.get("row_count", -1)
        v2_rows = r2.get("row_count", -1)
        
        if v2_ok and not v1_ok:
            improvements.append((name, "Now works", r1.get("sql_error") or r1.get("exec_error")))
        elif v1_ok and not v2_ok:
            regressions.append((name, "Now fails", r2.get("sql_error") or r2.get("exec_error")))
        elif v1_ok and v2_ok:
            if v1_rows != v2_rows:
                diff = f"Rows changed: {v1_rows} → {v2_rows}"
                if v2_rows > v1_rows:
                    improvements.append((name, diff, ""))
                else:
                    regressions.append((name, diff, ""))
            else:
                # Check if SQL differs
                v1_sql = r1.get("sql", "").strip()
                v2_sql = r2.get("sql", "").strip()
                if v1_sql != v2_sql:
                    unchanged.append((name, f"SQL differs, same results ({v1_rows} rows)"))
                else:
                    unchanged.append((name, f"Identical ({v1_rows} rows)"))
        else:
            unchanged.append((name, f"Both failed"))
    
    # Print results
    if improvements:
        print("🎉 IMPROVEMENTS (v2 better than v1):")
        for name, status, detail in improvements:
            print(f"  ✅ {name}: {status}")
            if detail:
                print(f"     (was: {detail[:80]})")
        print()
    
    if regressions:
        print("⚠️  REGRESSIONS (v2 worse than v1):")
        for name, status, detail in regressions:
            print(f"  ❌ {name}: {status}")
            if detail:
                print(f"     (error: {detail[:80]})")
        print()
    
    print(f"📊 SUMMARY:")
    print(f"  Improvements: {len(improvements)}")
    print(f"  Regressions:  {len(regressions)}")
    print(f"  Unchanged:    {len(unchanged)}")
    
    # Detailed SQL comparison for key multi-hop tests
    print(f"\n{'='*60}")
    print(" DETAILED SQL COMPARISON (Multi-hop patterns)")
    print(f"{'='*60}")
    
    multi_hop_tests = ["two_hop_directed", "two_hop_anonymous", "three_hop_directed"]
    for name in multi_hop_tests:
        if name in v1 and name in v2:
            print(f"\n--- {name} ---")
            print(f"Query: {v1[name]['query'][:100]}...")
            print(f"\nv1 SQL:")
            print(v1[name].get("sql", "N/A")[:500])
            print(f"\nv2 SQL:")
            print(v2[name].get("sql", "N/A")[:500])
            print()
    
    return len(regressions) == 0


def main():
    parser = argparse.ArgumentParser(description="Compare v1 vs v2 graph_join_inference")
    parser.add_argument("--mode", choices=["v1", "v2"], required=True, help="Which path is currently running")
    parser.add_argument("--save", action="store_true", help="Save results to file")
    parser.add_argument("--compare", action="store_true", help="Compare with previously saved results")
    args = parser.parse_args()
    
    # Run tests
    results = run_tests(args.mode)
    
    # Save if requested
    if args.save:
        save_results(args.mode, results)
    
    # Compare if requested
    if args.compare:
        other_mode = "v1" if args.mode == "v2" else "v2"
        other_results = load_results(other_mode)
        if not other_results:
            print(f"\n⚠️  No {other_mode} results found. Run with --mode {other_mode} --save first.")
            return 1
        
        if args.mode == "v2":
            success = compare_results(other_results, results)
        else:
            success = compare_results(results, other_results)
        
        return 0 if success else 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
