#!/usr/bin/env python3
"""
ClickGraph Benchmark Suite

Comprehensive performance benchmarking for ClickGraph - a Cypher-on-ClickHouse graph query engine.

This benchmark suite evaluates ClickGraph performance across:
- Different query types (simple lookups, traversals, aggregations, shortest paths)
- Multiple datasets (social network, e-commerce)
- Various complexity levels
- Performance metrics (latency, throughput, resource usage)

Usage:
    python benchmark.py --dataset social --queries all --iterations 5
    python benchmark.py --dataset ecommerce --queries traversal --iterations 10
    python benchmark.py --dataset all --queries all --iterations 3 --output results.json
"""

import argparse
import json
import statistics
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
import requests
import subprocess
import sys
import os

class ClickGraphBenchmark:
    """Main benchmarking class for ClickGraph performance evaluation."""

    def __init__(self, server_url: str = "http://localhost:8080"):
        self.server_url = server_url
        self.endpoint = f"{server_url}/query"
        self.results = []

    def run_query(self, query: str, description: str = "") -> Dict[str, Any]:
        """Execute a single query and collect performance metrics."""
        start_time = time.time()

        try:
            response = requests.post(
                self.endpoint,
                json={"query": query},
                headers={"Content-Type": "application/json"},
                timeout=300  # 5 minute timeout
            )

            end_time = time.time()
            total_time = end_time - start_time

            # Extract performance headers
            headers = response.headers
            perf_metrics = {
                "total_time_ms": float(headers.get("X-Query-Total-Time", "0").rstrip("ms")),
                "parse_time_ms": float(headers.get("X-Query-Parse-Time", "0").rstrip("ms")),
                "planning_time_ms": float(headers.get("X-Query-Planning-Time", "0").rstrip("ms")),
                "render_time_ms": float(headers.get("X-Query-Render-Time", "0").rstrip("ms")),
                "sql_gen_time_ms": float(headers.get("X-Query-SQL-Gen-Time", "0").rstrip("ms")),
                "execution_time_ms": float(headers.get("X-Query-Execution-Time", "0").rstrip("ms")),
            }

            result = {
                "query": query,
                "description": description,
                "timestamp": datetime.now().isoformat(),
                "success": response.status_code == 200,
                "status_code": response.status_code,
                "total_time_seconds": total_time,
                "performance_metrics": perf_metrics,
                "result_count": len(response.json()) if response.status_code == 200 and isinstance(response.json(), list) else 0,
                "error": response.text if response.status_code != 200 else None
            }

        except Exception as e:
            end_time = time.time()
            result = {
                "query": query,
                "description": description,
                "timestamp": datetime.now().isoformat(),
                "success": False,
                "status_code": None,
                "total_time_seconds": end_time - start_time,
                "performance_metrics": None,
                "result_count": 0,
                "error": str(e)
            }

        self.results.append(result)
        return result

    def run_benchmark_suite(self, dataset: str, query_types: List[str], iterations: int = 3) -> Dict[str, Any]:
        """Run a complete benchmark suite."""

        print(f"🚀 Starting ClickGraph Benchmark Suite")
        print(f"📊 Dataset: {dataset}")
        print(f"🔄 Iterations: {iterations}")
        print(f"📝 Query Types: {', '.join(query_types)}")
        print("=" * 60)

        # Get benchmark queries for the dataset
        queries = self.get_benchmark_queries(dataset, query_types)

        benchmark_results = {
            "benchmark_info": {
                "dataset": dataset,
                "query_types": query_types,
                "iterations": iterations,
                "timestamp": datetime.now().isoformat(),
                "server_url": self.server_url
            },
            "results": []
        }

        for query_info in queries:
            query_name = query_info["name"]
            query = query_info["query"]
            description = query_info["description"]
            category = query_info["category"]

            print(f"\n🔍 Running: {query_name}")
            print(f"📝 {description}")

            # Run multiple iterations
            iteration_results = []
            for i in range(iterations):
                print(f"  Iteration {i+1}/{iterations}...", end=" ", flush=True)
                result = self.run_query(query, description)

                if result["success"]:
                    print(f"✅ {result['total_time_seconds']:.3f}s")
                else:
                    print(f"❌ FAILED ({result['status_code']})")

                iteration_results.append(result)

            # Calculate statistics
            successful_runs = [r for r in iteration_results if r["success"]]
            if successful_runs:
                total_times = [r["total_time_seconds"] for r in successful_runs]
                perf_times = [r["performance_metrics"]["total_time_ms"] for r in successful_runs if r["performance_metrics"]]

                stats = {
                    "query_name": query_name,
                    "description": description,
                    "category": category,
                    "iterations_run": len(successful_runs),
                    "success_rate": len(successful_runs) / iterations,
                    "total_time_stats": {
                        "mean": statistics.mean(total_times),
                        "median": statistics.median(total_times),
                        "min": min(total_times),
                        "max": max(total_times),
                        "stdev": statistics.stdev(total_times) if len(total_times) > 1 else 0
                    },
                    "performance_time_stats": {
                        "mean": statistics.mean(perf_times) if perf_times else 0,
                        "median": statistics.median(perf_times) if perf_times else 0,
                        "min": min(perf_times) if perf_times else 0,
                        "max": max(perf_times) if perf_times else 0
                    } if perf_times else None,
                    "avg_result_count": statistics.mean([r["result_count"] for r in successful_runs]),
                    "individual_results": iteration_results
                }
            else:
                stats = {
                    "query_name": query_name,
                    "description": description,
                    "category": category,
                    "iterations_run": 0,
                    "success_rate": 0.0,
                    "error": "All iterations failed",
                    "individual_results": iteration_results
                }

            benchmark_results["results"].append(stats)

        return benchmark_results

    def get_benchmark_queries(self, dataset: str, query_types: List[str]) -> List[Dict[str, str]]:
        """Get benchmark queries for the specified dataset and query types."""

        queries = []

        if dataset in ["social", "all"]:
            # Social Network Queries
            if "simple" in query_types or "all" in query_types:
                queries.extend([
                    {
                        "name": "social_simple_node_lookup",
                        "query": 'MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.email',
                        "description": "Simple node lookup by ID",
                        "category": "simple"
                    },
                    {
                        "name": "social_node_filter",
                        "query": 'MATCH (u:User) WHERE u.is_active = 1 AND u.country = "USA" RETURN COUNT(u)',
                        "description": "Node filtering with aggregation",
                        "category": "simple"
                    }
                ])

            if "traversal" in query_types or "all" in query_types:
                queries.extend([
                    {
                        "name": "social_direct_relationships",
                        "query": 'MATCH (u1:User)-[r:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, r.follow_date',
                        "description": "Direct relationship traversal",
                        "category": "traversal"
                    },
                    {
                        "name": "social_multi_hop",
                        "query": 'MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u3:User) WHERE u1.user_id = 1 RETURN u3.name',
                        "description": "Multi-hop relationship traversal",
                        "category": "traversal"
                    }
                ])

            if "variable_length" in query_types or "all" in query_types:
                queries.extend([
                    {
                        "name": "social_variable_length_2",
                        "query": 'MATCH (u1:User)-[:FOLLOWS*2]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name',
                        "description": "Variable-length path (exactly 2 hops)",
                        "category": "variable_length"
                    },
                    {
                        "name": "social_variable_length_range",
                        "query": 'MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, length(path)',
                        "description": "Variable-length path (1-3 hops)",
                        "category": "variable_length"
                    }
                ])

            if "shortest_path" in query_types or "all" in query_types:
                queries.extend([
                    {
                        "name": "social_shortest_path",
                        "query": 'MATCH path = shortestPath((u1:User)-[:FOLLOWS*]-(u2:User)) WHERE u1.user_id = 1 AND u2.user_id = 10 RETURN length(path)',
                        "description": "Shortest path between users",
                        "category": "shortest_path"
                    }
                ])

            if "aggregation" in query_types or "all" in query_types:
                queries.extend([
                    {
                        "name": "social_follower_count",
                        "query": 'MATCH (u:User)<-[:FOLLOWS]-(follower) RETURN u.name, COUNT(follower) as follower_count ORDER BY follower_count DESC LIMIT 5',
                        "description": "User follower counts with ranking",
                        "category": "aggregation"
                    },
                    {
                        "name": "social_mutual_follows",
                        "query": 'MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u1) RETURN COUNT(DISTINCT u1) as mutual_follow_pairs',
                        "description": "Count of mutual follow relationships",
                        "category": "aggregation"
                    }
                ])

            if "complex" in query_types or "all" in query_types:
                queries.extend([
                    {
                        "name": "social_friends_of_friends",
                        "query": 'MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u3:User) WHERE u1.user_id = 1 AND u3.user_id <> 1 RETURN DISTINCT u3.name',
                        "description": "Friends of friends (excluding direct friends)",
                        "category": "complex"
                    }
                ])

        if dataset in ["ecommerce", "all"]:
            # E-commerce Queries
            if "simple" in query_types or "all" in query_types:
                queries.extend([
                    {
                        "name": "ecommerce_customer_lookup",
                        "query": 'MATCH (c:Customer) WHERE c.customer_id = 1 RETURN c.first_name, c.last_name, c.total_spent',
                        "description": "Customer lookup by ID",
                        "category": "simple"
                    }
                ])

            if "traversal" in query_types or "all" in query_types:
                queries.extend([
                    {
                        "name": "ecommerce_customer_orders",
                        "query": 'MATCH (c:Customer)-[r:PURCHASED]->(p:Product) WHERE c.customer_id = 1 RETURN p.name, r.quantity, r.total_amount',
                        "description": "Customer purchase history",
                        "category": "traversal"
                    }
                ])

            if "aggregation" in query_types or "all" in query_types:
                queries.extend([
                    {
                        "name": "ecommerce_top_products",
                        "query": 'MATCH (p:Product)<-[r:PURCHASED]-(c:Customer) RETURN p.name, SUM(r.quantity) as total_sold ORDER BY total_sold DESC LIMIT 5',
                        "description": "Top-selling products by quantity",
                        "category": "aggregation"
                    },
                    {
                        "name": "ecommerce_customer_spending",
                        "query": 'MATCH (c:Customer)-[r:PURCHASED]->(p:Product) RETURN c.first_name, c.last_name, SUM(r.total_amount) as total_spent ORDER BY total_spent DESC LIMIT 5',
                        "description": "Top-spending customers",
                        "category": "aggregation"
                    }
                ])

        return queries

    def save_results(self, results: Dict[str, Any], output_file: str):
        """Save benchmark results to a JSON file."""
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n💾 Results saved to: {output_file}")

    def print_summary(self, results: Dict[str, Any]):
        """Print a summary of benchmark results."""
        print("\n" + "=" * 80)
        print("📊 BENCHMARK SUMMARY")
        print("=" * 80)

        info = results["benchmark_info"]
        print(f"Dataset: {info['dataset']}")
        print(f"Query Types: {', '.join(info['query_types'])}")
        print(f"Iterations: {info['iterations']}")
        print(f"Timestamp: {info['timestamp']}")

        print(f"\n📈 RESULTS SUMMARY:")
        print("-" * 50)

        successful_queries = 0
        total_queries = len(results["results"])

        for query_result in results["results"]:
            name = query_result["query_name"]
            success_rate = query_result["success_rate"]
            if success_rate > 0:
                successful_queries += 1
                mean_time = query_result["total_time_stats"]["mean"]
                print(f"✅ {name}: {mean_time:.3f}s (avg)")
            else:
                print(f"❌ {name}: FAILED (0% success)")

        print(f"\n✅ Overall Success: {successful_queries}/{total_queries} queries successful")

        if successful_queries > 0:
            # Calculate overall statistics
            all_times = []
            for query_result in results["results"]:
                if query_result["success_rate"] > 0:
                    all_times.extend([r["total_time_seconds"] for r in query_result["individual_results"] if r["success"]])

            if all_times:
                print(f"📊 Min: {min(all_times):.3f}s")
                print(f"📊 Max: {max(all_times):.3f}s")
                print(f"📊 Avg: {statistics.mean(all_times):.3f}s")
def main():
    parser = argparse.ArgumentParser(description="ClickGraph Benchmark Suite")
    parser.add_argument("--dataset", choices=["social", "ecommerce", "all"],
                       default="social", help="Dataset to benchmark")
    parser.add_argument("--queries", nargs="+",
                       choices=["simple", "traversal", "variable_length", "shortest_path", "aggregation", "complex", "all"],
                       default=["all"], help="Query types to run")
    parser.add_argument("--iterations", type=int, default=3,
                       help="Number of iterations per query")
    parser.add_argument("--server-url", default="http://localhost:8080",
                       help="ClickGraph server URL")
    parser.add_argument("--output", help="Output file for results (JSON)")
    parser.add_argument("--warmup", action="store_true",
                       help="Run warmup queries before benchmarking")

    args = parser.parse_args()

    # Handle "all" query type
    if "all" in args.queries:
        args.queries = ["simple", "traversal", "variable_length", "shortest_path", "aggregation", "complex"]

    # Initialize benchmark
    benchmark = ClickGraphBenchmark(args.server_url)

    # Check server connectivity
    try:
        response = requests.get(f"{args.server_url}/health", timeout=5)
        if response.status_code != 200:
            print(f"❌ Server health check failed: {response.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Cannot connect to server at {args.server_url}: {e}")
        print("💡 Make sure ClickGraph server is running")
        sys.exit(1)

    print(f"✅ Connected to ClickGraph server at {args.server_url}")

    # Run warmup if requested
    if args.warmup:
        print("🔥 Running warmup queries...")
        warmup_queries = [
            'MATCH (u:User) RETURN COUNT(u) LIMIT 1',
            'MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN COUNT(f) LIMIT 1'
        ]
        for query in warmup_queries:
            benchmark.run_query(query, "warmup")

    # Run benchmark
    results = benchmark.run_benchmark_suite(args.dataset, args.queries, args.iterations)

    # Print summary
    benchmark.print_summary(results)

    # Save results if requested
    if args.output:
        benchmark.save_results(results, args.output)

if __name__ == "__main__":
    main()