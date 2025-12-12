#!/usr/bin/env python3
"""
Run LDBC SNB benchmark queries against ClickGraph.

Usage:
    python run_benchmark.py --query is1 --param personId=123
    python run_benchmark.py --queries all --timing
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("Please install requests: pip install requests")
    sys.exit(1)

# Default ClickGraph endpoint
CLICKGRAPH_URL = os.environ.get("CLICKGRAPH_URL", "http://localhost:8080")

# Sample parameters for testing
SAMPLE_PARAMS = {
    "is1": {"personId": 933},
    "is2": {"personId": 933},
    "is3": {"personId": 933},
    "is5": {"messageId": 1030792},
    "ic1": {"personId": 933, "firstName": "John"},
    "ic2": {"personId": 933, "maxDate": 1287230400000},
    "ic3": {"personId": 933, "countryXName": "India", "countryYName": "China", 
            "startDate": 1275350400000, "endDate": 1277856000000},
    "ic9": {"personId": 933, "maxDate": 1287230400000},
}


def load_query(query_name: str) -> str:
    """Load a query file."""
    script_dir = Path(__file__).parent
    queries_dir = script_dir.parent / "queries" / "adapted"
    
    # Map query names to files
    if query_name.startswith("is"):
        filename = f"interactive-short-{query_name[2:]}.cypher"
    elif query_name.startswith("ic"):
        filename = f"interactive-complex-{query_name[2:]}.cypher"
    else:
        filename = f"{query_name}.cypher"
    
    query_path = queries_dir / filename
    if not query_path.exists():
        raise FileNotFoundError(f"Query file not found: {query_path}")
    
    # Read query and filter out comments
    with open(query_path) as f:
        lines = []
        for line in f:
            line = line.strip()
            if line and not line.startswith("--"):
                lines.append(line)
        return "\n".join(lines)


def substitute_params(query: str, params: dict) -> str:
    """Substitute parameters in the query."""
    for key, value in params.items():
        if isinstance(value, str):
            query = query.replace(f"${key}", f"'{value}'")
        elif isinstance(value, list):
            formatted = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in value)
            query = query.replace(f"${key}", f"[{formatted}]")
        else:
            query = query.replace(f"${key}", str(value))
    return query


def run_query(query: str, url: str = CLICKGRAPH_URL) -> dict:
    """Execute a query against ClickGraph."""
    endpoint = f"{url}/query"
    
    start_time = time.time()
    try:
        response = requests.post(
            endpoint,
            json={"query": query},
            headers={"Content-Type": "application/json"},
            timeout=300  # 5 minute timeout
        )
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            result = response.json()
            return {
                "success": True,
                "rows": len(result.get("results", [])),
                "data": result,
                "elapsed_ms": elapsed * 1000
            }
        else:
            return {
                "success": False,
                "error": response.text,
                "elapsed_ms": elapsed * 1000
            }
    except requests.RequestException as e:
        elapsed = time.time() - start_time
        return {
            "success": False,
            "error": str(e),
            "elapsed_ms": elapsed * 1000
        }


def main():
    parser = argparse.ArgumentParser(description="Run LDBC SNB benchmark queries")
    parser.add_argument("--query", "-q", help="Single query to run (e.g., is1, ic2)")
    parser.add_argument("--queries", help="Query set to run: 'is', 'ic', or 'all'")
    parser.add_argument("--param", "-p", action="append", default=[],
                       help="Query parameter in key=value format")
    parser.add_argument("--url", default=CLICKGRAPH_URL,
                       help=f"ClickGraph URL (default: {CLICKGRAPH_URL})")
    parser.add_argument("--timing", action="store_true",
                       help="Show timing information")
    parser.add_argument("--show-query", action="store_true",
                       help="Show the query being executed")
    parser.add_argument("--show-results", action="store_true",
                       help="Show query results")
    
    args = parser.parse_args()
    
    if not args.query and not args.queries:
        parser.print_help()
        return 1
    
    # Parse custom parameters
    custom_params = {}
    for p in args.param:
        if "=" in p:
            key, value = p.split("=", 1)
            # Try to parse as int/float
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass
            custom_params[key] = value
    
    # Determine queries to run
    queries_to_run = []
    if args.query:
        queries_to_run = [args.query]
    elif args.queries == "is":
        queries_to_run = ["is1", "is2", "is3", "is5"]
    elif args.queries == "ic":
        queries_to_run = ["ic1", "ic2", "ic3", "ic9"]
    elif args.queries == "all":
        queries_to_run = ["is1", "is2", "is3", "is5", "ic1", "ic2", "ic3", "ic9"]
    
    print("=" * 70)
    print("LDBC SNB Benchmark Runner")
    print("=" * 70)
    print(f"ClickGraph URL: {args.url}")
    print(f"Queries to run: {', '.join(queries_to_run)}")
    print("=" * 70)
    
    results = []
    for query_name in queries_to_run:
        print(f"\n[{query_name}] ", end="", flush=True)
        
        try:
            query_template = load_query(query_name)
        except FileNotFoundError as e:
            print(f"SKIPPED - {e}")
            continue
        
        # Get parameters
        params = SAMPLE_PARAMS.get(query_name, {})
        params.update(custom_params)
        
        # Substitute parameters
        query = substitute_params(query_template, params)
        
        if args.show_query:
            print(f"\nQuery:\n{query}\n")
        
        # Run query
        result = run_query(query, args.url)
        results.append((query_name, result))
        
        if result["success"]:
            print(f"✓ {result['rows']} rows", end="")
            if args.timing:
                print(f" ({result['elapsed_ms']:.1f}ms)", end="")
            print()
            
            if args.show_results and result.get("data"):
                data = result["data"]
                if "results" in data and data["results"]:
                    print(f"  First row: {json.dumps(data['results'][0], indent=2)}")
        else:
            print(f"✗ Error: {result['error']}")
    
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    
    success_count = sum(1 for _, r in results if r["success"])
    total_time = sum(r["elapsed_ms"] for _, r in results)
    
    print(f"Queries: {success_count}/{len(results)} successful")
    print(f"Total time: {total_time:.1f}ms")
    
    if args.timing:
        print("\nTiming breakdown:")
        for query_name, result in results:
            status = "✓" if result["success"] else "✗"
            print(f"  {status} {query_name}: {result['elapsed_ms']:.1f}ms")
    
    return 0 if success_count == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
