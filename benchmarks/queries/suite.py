#!/usr/bin/env python3
"""
Unified Benchmark Test Suite - All Scales
==========================================
Runs the same 14 queries across all scale factors for consistent comparison.

Includes:
- 10 core query patterns (node lookup, traversal, variable-length, shortest path, aggregation)
- 4 parameter + function queries (new patterns)

Usage:
    python test_benchmark_suite.py --scale 1          # Small (1K users)
    python test_benchmark_suite.py --scale 10         # Medium (10K users)  
    python test_benchmark_suite.py --scale 100        # Large (100K users)
    python test_benchmark_suite.py --scale 1000       # XLarge (1M users)
    python test_benchmark_suite.py --scale 5000       # XXLarge (5M users)
    
    # With performance metrics
    python test_benchmark_suite.py --scale 10 --iterations 5
    
    # Output to JSON
    python test_benchmark_suite.py --scale 100 --output results.json
"""
import argparse
import requests
import json
import time
from statistics import mean, median, stdev

SERVER_URL = "http://localhost:8080/query"

# Unified query set - same 14 queries for all scales
BENCHMARK_QUERIES = [
    # Core Query 1: Simple node lookup
    {
        "name": "simple_node_lookup",
        "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.user_id LIMIT 1",
        "category": "simple"
    },
    # Core Query 2: Node filter with range
    {
        "name": "node_filter_range",
        "query": "MATCH (u:User) WHERE u.user_id < 100 RETURN u.name, u.email LIMIT 10",
        "category": "simple"
    },
    # Core Query 3: Direct relationship traversal
    {
        "name": "direct_relationships",
        "query": "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, u2.user_id LIMIT 10",
        "category": "traversal"
    },
    # Core Query 4: Multi-hop traversal (2 hops)
    {
        "name": "multi_hop_2",
        "query": "MATCH (u1:User)-[:FOLLOWS]->()-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN DISTINCT u2.name, u2.user_id LIMIT 10",
        "category": "traversal"
    },
    # Core Query 5: Friends of friends pattern
    {
        "name": "friends_of_friends",
        "query": "MATCH (u:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof:User) WHERE u.user_id = 1 RETURN DISTINCT fof.name, fof.user_id LIMIT 10",
        "category": "traversal"
    },
    # Core Query 6: Variable-length path (exact 2 hops)
    {
        "name": "variable_length_exact_2",
        "query": "MATCH (u1:User)-[:FOLLOWS*2]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, u2.user_id LIMIT 10",
        "category": "variable_length"
    },
    # Core Query 7: Variable-length path (range 1-3 hops)
    {
        "name": "variable_length_range_1to3",
        "query": "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, u2.user_id LIMIT 20",
        "category": "variable_length"
    },
    # Core Query 8: Shortest path
    {
        "name": "shortest_path",
        "query": "MATCH (u1:User)-[:FOLLOWS*]-(u2:User) WHERE u1.user_id = 1 AND u2.user_id = 100 RETURN u1.name, u2.name LIMIT 10",
        "category": "shortest_path"
    },
    # Core Query 9: Aggregation - follower count
    {
        "name": "aggregation_follower_count",
        "query": "MATCH (u:User)<-[:FOLLOWS]-(follower) RETURN u.name, u.user_id, COUNT(follower) as count ORDER BY count DESC LIMIT 10",
        "category": "aggregation"
    },
    # Core Query 10: Bidirectional pattern - mutual follows
    {
        "name": "mutual_follows",
        "query": "MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u1) RETURN u1.name, u2.name, u1.user_id, u2.user_id LIMIT 10",
        "category": "aggregation"
    },
    
    # New Pattern 11: Parameter + function in filter
    {
        "name": "param_filter_function",
        "query": "MATCH (u:User) WHERE u.user_id < $maxId RETURN toUpper(u.name) AS name, u.user_id LIMIT 10",
        "parameters": {"maxId": 500},
        "category": "param_function"
    },
    # New Pattern 12: Function in aggregation with parameter
    {
        "name": "function_aggregation_param",
        "query": "MATCH (u:User)<-[:FOLLOWS]-(follower) WHERE u.user_id < $threshold RETURN toUpper(u.name) AS name, COUNT(follower) AS count ORDER BY count DESC LIMIT 10",
        "parameters": {"threshold": 1000},
        "category": "param_function"
    },
    # New Pattern 13: Math function with parameters
    {
        "name": "math_function_param",
        "query": "MATCH (u:User) WHERE abs(u.user_id - $targetId) < $tolerance RETURN u.name, u.user_id, abs(u.user_id - $targetId) AS distance LIMIT 10",
        "parameters": {"targetId": 500, "tolerance": 50},
        "category": "param_function"
    },
    # New Pattern 14: Parameter in variable-length path
    {
        "name": "param_variable_path",
        "query": "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) WHERE u1.user_id = $startId RETURN u2.name, u2.user_id LIMIT 10",
        "parameters": {"startId": 1},
        "category": "param_function"
    },
    
    # Post Queries (15-16): Test Post node and AUTHORED relationship
    {
        "name": "user_post_count",
        "query": "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, u.user_id, COUNT(p) as post_count ORDER BY post_count DESC LIMIT 10",
        "category": "posts"
    },
    {
        "name": "active_users_followers",
        "query": "MATCH (u:User)-[:AUTHORED]->(p:Post) WITH u, COUNT(p) as posts WITH AVG(posts) as avg_posts MATCH (u2:User)-[:AUTHORED]->(p2:Post) WITH u2, COUNT(p2) as user_posts, avg_posts WHERE user_posts > avg_posts * 3 MATCH (u2)<-[:FOLLOWS]-(f) RETURN u2.name, user_posts, COUNT(f) as followers ORDER BY followers DESC LIMIT 10",
        "category": "posts"
    }
]

def run_query(query_name, cypher_query, parameters=None, iterations=1):
    """Run a query one or more times and collect timing statistics"""
    times = []
    result_counts = []
    errors = []
    
    for i in range(iterations):
        try:
            payload = {"query": cypher_query}
            if parameters:
                payload["parameters"] = parameters
            
            start = time.time()
            response = requests.post(SERVER_URL, json=payload, timeout=60)
            elapsed = time.time() - start
            
            if response.status_code == 200:
                result = response.json()
                times.append(elapsed * 1000)  # Convert to ms
                result_counts.append(len(result) if isinstance(result, list) else 0)
            else:
                errors.append(f"HTTP {response.status_code}: {response.text[:200]}")
                
        except Exception as e:
            errors.append(str(e))
    
    # Calculate statistics
    if times:
        result = {
            "query_name": query_name,
            "status": "PASS" if not errors else "PARTIAL",
            "iterations": iterations,
            "successful_runs": len(times),
            "failed_runs": len(errors),
            "result_count": result_counts[0] if result_counts else 0,
            "timing": {
                "mean_ms": round(mean(times), 2) if len(times) > 0 else None,
                "median_ms": round(median(times), 2) if len(times) > 1 else round(times[0], 2) if times else None,
                "min_ms": round(min(times), 2) if times else None,
                "max_ms": round(max(times), 2) if times else None,
                "stdev_ms": round(stdev(times), 2) if len(times) > 1 else None
            },
            "errors": errors[:3] if errors else []  # Keep first 3 errors
        }
    else:
        result = {
            "query_name": query_name,
            "status": "FAIL",
            "iterations": iterations,
            "successful_runs": 0,
            "failed_runs": len(errors),
            "result_count": 0,
            "timing": None,
            "errors": errors[:3]
        }
    
    return result

def print_result(result, verbose=False):
    """Print query result in human-readable format"""
    status_icon = "✓" if result["status"] == "PASS" else "✗"
    name = result["query_name"]
    
    if result["timing"]:
        timing = result["timing"]
        if result["iterations"] == 1:
            print(f"{status_icon} {name}: {timing['mean_ms']}ms ({result['result_count']} rows)")
        else:
            print(f"{status_icon} {name}:")
            print(f"    Mean: {timing['mean_ms']}ms, Median: {timing['median_ms']}ms")
            print(f"    Min: {timing['min_ms']}ms, Max: {timing['max_ms']}ms")
            if timing['stdev_ms']:
                print(f"    StdDev: {timing['stdev_ms']}ms")
            print(f"    Results: {result['result_count']} rows, Runs: {result['successful_runs']}/{result['iterations']}")
    else:
        print(f"{status_icon} {name}: FAILED")
        if verbose and result["errors"]:
            for err in result["errors"]:
                print(f"    Error: {err}")

def main():
    parser = argparse.ArgumentParser(
        description='Run unified benchmark suite across all scales',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--scale', type=int, default=1,
                       help='Scale factor used in data generation (default: 1)')
    parser.add_argument('--iterations', type=int, default=1,
                       help='Number of iterations per query (default: 1, use 5+ for stats)')
    parser.add_argument('--output', type=str, default=None,
                       help='Output JSON file for results (default: None)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed error messages')
    parser.add_argument('--category', type=str, default='all',
                       choices=['all', 'simple', 'traversal', 'variable_length', 'shortest_path', 'aggregation', 'param_function'],
                       help='Run only queries in specific category (default: all)')
    
    args = parser.parse_args()
    
    # Filter queries by category
    queries_to_run = BENCHMARK_QUERIES
    if args.category != 'all':
        queries_to_run = [q for q in BENCHMARK_QUERIES if q.get('category') == args.category]
    
    num_users = args.scale * 1000
    
    print("=" * 70)
    print("ClickGraph Unified Benchmark Suite")
    print("=" * 70)
    print(f"Scale Factor: {args.scale} ({num_users:,} users)")
    print(f"Iterations: {args.iterations}")
    print(f"Category: {args.category}")
    print(f"Queries: {len(queries_to_run)}")
    print("=" * 70)
    print()
    
    results = []
    start_time = time.time()
    
    for i, test in enumerate(queries_to_run, 1):
        query_name = test["name"]
        query = test["query"]
        parameters = test.get("parameters")
        category = test.get("category", "unknown")
        
        print(f"[{i}/{len(queries_to_run)}] Running: {query_name} ({category})")
        if parameters:
            print(f"    Parameters: {parameters}")
        
        result = run_query(query_name, query, parameters, args.iterations)
        results.append(result)
        print_result(result, args.verbose)
        print()
    
    total_time = time.time() - start_time
    
    # Summary
    passed = sum(1 for r in results if r["status"] == "PASS")
    partial = sum(1 for r in results if r["status"] == "PARTIAL")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Total Queries: {len(results)}")
    print(f"Passed: {passed}")
    print(f"Partial: {partial}")
    print(f"Failed: {failed}")
    print(f"Success Rate: {passed}/{len(results)} ({passed/len(results)*100:.1f}%)")
    print(f"Total Time: {total_time:.1f}s")
    
    if args.iterations > 1 and passed > 0:
        # Overall performance stats
        all_means = [r["timing"]["mean_ms"] for r in results if r["timing"]]
        if all_means:
            print(f"\nOverall Performance:")
            print(f"  Mean query time: {mean(all_means):.1f}ms")
            print(f"  Median query time: {median(all_means):.1f}ms")
            print(f"  Fastest query: {min(all_means):.1f}ms")
            print(f"  Slowest query: {max(all_means):.1f}ms")
    
    print("=" * 70)
    
    # Save to JSON if requested
    if args.output:
        output_data = {
            "metadata": {
                "scale_factor": args.scale,
                "num_users": num_users,
                "iterations": args.iterations,
                "category": args.category,
                "total_time_seconds": round(total_time, 2),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            },
            "summary": {
                "total_queries": len(results),
                "passed": passed,
                "partial": partial,
                "failed": failed,
                "success_rate": round(passed/len(results)*100, 1)
            },
            "results": results
        }
        
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output}")
    
    # Exit code
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    import sys
    main()
