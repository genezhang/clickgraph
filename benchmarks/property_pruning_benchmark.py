#!/usr/bin/env python3
"""
Property Pruning Performance Benchmark

Measures the impact of property pruning optimization on query performance.
Compares execution time and memory usage for queries that benefit most from
the optimization (wide tables with many properties, using only a few).

Usage:
    python3 benchmarks/property_pruning_benchmark.py
"""

import requests
import time
import json
import sys
from typing import Dict, List, Tuple

# Server endpoint
SERVER_URL = "http://localhost:8080"

# Test queries that benefit from property pruning
BENCHMARK_QUERIES = [
    {
        "name": "Single Property Access",
        "cypher": "MATCH (u:User) WHERE u.user_id <= 100 RETURN u.name",
        "description": "Access 1 of 7 properties → 85.7% reduction expected"
    },
    {
        "name": "Two Property Access",
        "cypher": "MATCH (u:User) WHERE u.user_id <= 100 RETURN u.name, u.email",
        "description": "Access 2 of 7 properties → 71.4% reduction expected"
    },
    {
        "name": "Collect with Single Property",
        "cypher": """
            MATCH (u:User)-[:FOLLOWS]->(f:User)
            WHERE u.user_id <= 10
            RETURN u.name, collect(f.name) AS friend_names
        """,
        "description": "collect(f.name) vs collect(f) with 7 properties"
    },
    {
        "name": "Aggregate with Property",
        "cypher": """
            MATCH (u:User)-[:FOLLOWS]->(f:User)
            WHERE u.user_id <= 100
            WITH u, count(f) AS friend_count, collect(f.name) AS friends
            RETURN u.name, friend_count, friends
        """,
        "description": "WITH aggregation with specific properties"
    },
    {
        "name": "Wildcard Return (Control)",
        "cypher": "MATCH (u:User) WHERE u.user_id <= 100 RETURN u",
        "description": "Return all properties - should NOT be pruned"
    },
]

def query_clickgraph(cypher: str) -> Tuple[float, Dict]:
    """Execute a Cypher query and measure execution time."""
    payload = {"query": cypher}
    
    start_time = time.time()
    response = requests.post(
        f"{SERVER_URL}/query",
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=30
    )
    execution_time = time.time() - start_time
    
    if response.status_code != 200:
        print(f"❌ Query failed: {response.text}")
        return None, None
    
    result = response.json()
    return execution_time, result

def run_benchmark(query_info: Dict, iterations: int = 5) -> Dict:
    """Run a single benchmark query multiple times and collect statistics."""
    print(f"\n{'='*80}")
    print(f"📊 {query_info['name']}")
    print(f"   {query_info['description']}")
    print(f"{'='*80}")
    print(f"Cypher: {query_info['cypher'].strip()}")
    
    times = []
    row_counts = []
    
    # Warm-up run
    print("\n🔥 Warm-up run...")
    execution_time, result = query_clickgraph(query_info['cypher'])
    if execution_time is None:
        return None
    
    # Actual benchmark runs
    print(f"\n⏱️  Running {iterations} iterations...")
    for i in range(iterations):
        execution_time, result = query_clickgraph(query_info['cypher'])
        if execution_time is None:
            return None
        
        times.append(execution_time)
        row_counts.append(result.get('row_count', 0))
        print(f"   Run {i+1}: {execution_time*1000:.2f}ms ({row_counts[-1]} rows)")
    
    # Calculate statistics
    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)
    
    stats = {
        "query_name": query_info['name'],
        "avg_time_ms": avg_time * 1000,
        "min_time_ms": min_time * 1000,
        "max_time_ms": max_time * 1000,
        "rows": row_counts[0],
        "iterations": iterations
    }
    
    print(f"\n📈 Results:")
    print(f"   Average: {stats['avg_time_ms']:.2f}ms")
    print(f"   Min:     {stats['min_time_ms']:.2f}ms")
    print(f"   Max:     {stats['max_time_ms']:.2f}ms")
    print(f"   Rows:    {stats['rows']}")
    
    return stats

def print_summary(all_stats: List[Dict]):
    """Print a summary table of all benchmark results."""
    print(f"\n\n{'='*80}")
    print("📊 BENCHMARK SUMMARY")
    print(f"{'='*80}")
    print(f"{'Query':<35} {'Avg Time':<12} {'Min/Max':<20} {'Rows':<8}")
    print(f"{'-'*35} {'-'*12} {'-'*20} {'-'*8}")
    
    for stats in all_stats:
        if stats:
            print(f"{stats['query_name']:<35} "
                  f"{stats['avg_time_ms']:>10.2f}ms "
                  f"{stats['min_time_ms']:>7.2f}/{stats['max_time_ms']:<7.2f}ms "
                  f"{stats['rows']:>6}")
    
    print(f"{'='*80}")
    
    # Calculate relative performance
    if len(all_stats) >= 2:
        single_prop = all_stats[0]
        wildcard = all_stats[-1]
        if single_prop and wildcard:
            speedup = wildcard['avg_time_ms'] / single_prop['avg_time_ms']
            print(f"\n💡 Property Pruning Impact:")
            print(f"   Single property query: {single_prop['avg_time_ms']:.2f}ms")
            print(f"   Wildcard query: {wildcard['avg_time_ms']:.2f}ms")
            if speedup > 1.0:
                print(f"   ✅ Property pruning is {speedup:.2f}x FASTER")
            elif speedup < 0.95:
                print(f"   ⚠️ Property pruning is {1/speedup:.2f}x SLOWER (unexpected!)")
            else:
                print(f"   ⚠️ No significant performance difference (both ~{single_prop['avg_time_ms']:.2f}ms)")

def main():
    """Run all benchmarks and report results."""
    print("🚀 ClickGraph Property Pruning Benchmark")
    print(f"Server: {SERVER_URL}")
    print(f"Queries: {len(BENCHMARK_QUERIES)}")
    
    # Check if server is running
    try:
        response = requests.get(f"{SERVER_URL}/health", timeout=5)
        if response.status_code != 200:
            print(f"❌ Server health check failed: {response.status_code}")
            sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to ClickGraph server at {SERVER_URL}")
        print(f"   Error: {e}")
        print(f"\n💡 Start the server with: cargo run --bin clickgraph")
        sys.exit(1)
    
    print("✅ Server is healthy\n")
    
    # Run all benchmarks
    all_stats = []
    for query_info in BENCHMARK_QUERIES:
        stats = run_benchmark(query_info, iterations=5)
        all_stats.append(stats)
        time.sleep(0.5)  # Brief pause between benchmarks
    
    # Print summary
    print_summary(all_stats)
    
    print("\n✅ Benchmark complete!")

if __name__ == "__main__":
    main()
