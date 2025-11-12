#!/usr/bin/env python3
"""
Medium Benchmark - Performance Testing
Compares performance on 10,000 users vs 1,000 users
"""
import requests
import json
import time
from statistics import mean, median, stdev

SERVER_URL = "http://localhost:8080/query"

BENCHMARK_QUERIES = [
    {
        "name": "simple_node_lookup",
        "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.user_id LIMIT 1"
    },
    {
        "name": "node_filter_range",
        "query": "MATCH (u:User) WHERE u.user_id < 100 RETURN u.name, u.email LIMIT 10"
    },
    {
        "name": "direct_relationships",
        "query": "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, u2.user_id LIMIT 10"
    },
    {
        "name": "multi_hop_2",
        "query": "MATCH (u1:User)-[:FOLLOWS]->()-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN DISTINCT u2.name, u2.user_id LIMIT 10"
    },
    {
        "name": "variable_length_exact_2",
        "query": "MATCH (u1:User)-[:FOLLOWS*2]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, u2.user_id LIMIT 10"
    },
    {
        "name": "variable_length_range_1to3",
        "query": "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, u2.user_id LIMIT 20"
    },
    {
        "name": "shortest_path",
        "query": "MATCH (u1:User)-[:FOLLOWS*]-(u2:User) WHERE u1.user_id = 1 AND u2.user_id = 100 RETURN u1.name, u2.name LIMIT 10"
    },
    {
        "name": "aggregation_follower_count",
        "query": "MATCH (u:User)<-[:FOLLOWS]-(follower) RETURN u.name, u.user_id, COUNT(follower) as count ORDER BY count DESC LIMIT 10"
    },
    {
        "name": "aggregation_total_users",
        "query": "MATCH (u:User) RETURN COUNT(u) as total_users"
    },
    {
        "name": "mutual_follows",
        "query": "MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u1) RETURN u1.name, u2.name, u1.user_id, u2.user_id LIMIT 10"
    },
    # Parameter + Function queries (performance validation)
    {
        "name": "param_filter_function",
        "query": "MATCH (u:User) WHERE u.user_id < $maxId RETURN toUpper(u.name) AS name, u.user_id LIMIT 10",
        "parameters": {"maxId": 500}
    },
    {
        "name": "function_aggregation_param",
        "query": "MATCH (u:User)<-[:FOLLOWS]-(f) WHERE u.user_id < $threshold RETURN toUpper(u.name) AS name, COUNT(f) AS count ORDER BY count DESC LIMIT 10",
        "parameters": {"threshold": 5000}
    }
]

def run_query(query, parameters=None, iterations=5):
    """Run a query multiple times and collect timing statistics"""
    times = []
    result_counts = []
    
    for _ in range(iterations):
        start = time.time()
        try:
            payload = {"query": query}
            if parameters:
                payload["parameters"] = parameters
                
            response = requests.post(
                SERVER_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=60
            )
            elapsed = time.time() - start
            
            if response.status_code == 200:
                result = response.json()
                if isinstance(result, list):
                    times.append(elapsed)
                    result_counts.append(len(result))
                else:
                    return None, f"Unexpected response format"
            else:
                return None, f"HTTP {response.status_code}"
        except Exception as e:
            return None, str(e)
    
    if not times:
        return None, "No successful runs"
    
    return {
        "success": True,
        "iterations": len(times),
        "mean_time": mean(times),
        "median_time": median(times),
        "min_time": min(times),
        "max_time": max(times),
        "stdev": stdev(times) if len(times) > 1 else 0,
        "avg_result_count": mean(result_counts)
    }, None

def main():
    print("=" * 80)
    print("MEDIUM BENCHMARK - Performance Testing")
    print("Dataset: 10,000 users, 50,000 follows")
    print("Iterations per query: 5")
    print("=" * 80)
    print()
    
    results = []
    
    for i, test in enumerate(BENCHMARK_QUERIES, 1):
        name = test["name"]
        query = test["query"]
        parameters = test.get("parameters")
        
        print(f"{i}. {name}")
        print(f"   Query: {query[:70]}...")
        if parameters:
            print(f"   Parameters: {parameters}")
        
        stats, error = run_query(query, parameters, iterations=5)
        
        if stats:
            print(f"   [PASS] {stats['iterations']} runs")
            print(f"   Time: mean={stats['mean_time']*1000:.1f}ms, "
                  f"median={stats['median_time']*1000:.1f}ms, "
                  f"min={stats['min_time']*1000:.1f}ms, "
                  f"max={stats['max_time']*1000:.1f}ms")
            print(f"   Results: avg={stats['avg_result_count']:.0f} rows")
            results.append(("PASS", name, stats))
        else:
            print(f"   [FAIL] {error}")
            results.append(("FAIL", name, None))
        print()
    
    # Summary
    print("=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for status, _, _ in results if status == "PASS")
    
    print(f"\nSuccess Rate: {passed}/{len(results)} ({passed/len(results)*100:.0f}%)")
    print()
    
    # Performance table
    print("Query Performance (milliseconds):")
    print("-" * 80)
    print(f"{'Query':<35} {'Mean':<10} {'Median':<10} {'Min':<10} {'Max':<10}")
    print("-" * 80)
    
    for status, name, stats in results:
        if status == "PASS":
            print(f"{name:<35} {stats['mean_time']*1000:<10.1f} "
                  f"{stats['median_time']*1000:<10.1f} "
                  f"{stats['min_time']*1000:<10.1f} "
                  f"{stats['max_time']*1000:<10.1f}")
    
    print("=" * 80)
    
    # Overall statistics
    all_times = [stats['mean_time'] for _, _, stats in results if stats]
    if all_times:
        print(f"\nOverall Mean Query Time: {mean(all_times)*1000:.1f}ms")
        print(f"Overall Median Query Time: {median(all_times)*1000:.1f}ms")
        print(f"Fastest Query: {min(all_times)*1000:.1f}ms")
        print(f"Slowest Query: {max(all_times)*1000:.1f}ms")
    
    return passed == len(results)

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
