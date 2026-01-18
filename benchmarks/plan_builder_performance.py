#!/usr/bin/env python3
"""
Cypher-to-SQL Translation Performance Benchmark

This script measures the performance of ClickGraph's Cypher-to-SQL translation process
(translation time only, excluding SQL execution).

Usage:
    python benchmarks/plan_builder_performance.py --baseline
    python benchmarks/plan_builder_performance.py --compare
"""

import subprocess
import json
import time
import statistics
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any

class CypherTranslationBenchmark:
    def __init__(self):
        self.results_file = Path("benchmarks/plan_builder_baseline.json")
        self.test_queries = self._get_test_queries()

    def _get_test_queries(self) -> List[Dict[str, Any]]:
        """Define test queries for benchmarking"""
        return [
            {
                "name": "simple_node",
                "query": "MATCH (n:User) RETURN n.name",
                "description": "Simple node query"
            },
            {
                "name": "complex_joins",
                "query": "MATCH (u:User)-[:FOLLOWS]->(f:User)-[:POSTED]->(p:Post) RETURN u.name, f.name, p.content",
                "description": "Multi-hop query with joins"
            },
            {
                "name": "variable_length_path",
                "query": "MATCH (u:User)-[*1..3]->(target) RETURN u.name, target.name",
                "description": "Variable-length path query"
            },
            {
                "name": "aggregation",
                "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN count(u) as followers, u.country",
                "description": "Aggregation query"
            },
            {
                "name": "complex_filters",
                "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.country = 'US' AND f.registration_date > '2020-01-01' RETURN u.name",
                "description": "Query with complex filters"
            }
        ]

    def _run_single_query(self, query: str) -> Dict[str, float]:
        """Run a single query and measure performance"""
        # Start timing
        start_time = time.time()

        # Run the query through ClickGraph in sql_only mode (translation only)
        try:
            result = subprocess.run([
                "curl", "-s", "-X", "POST",
                "http://localhost:8080/query",
                "-H", "Content-Type: application/json",
                "-d", json.dumps({"query": query, "sql_only": True})
            ], capture_output=True, text=True, timeout=30)

            end_time = time.time()
            duration = end_time - start_time

            if result.returncode == 0:
                try:
                    response_data = json.loads(result.stdout)
                    if "generated_sql" in response_data:
                        return {
                            "duration": duration,
                            "success": True,
                            "error": None,
                            "sql_length": len(response_data.get("generated_sql", ""))
                        }
                    else:
                        return {
                            "duration": duration,
                            "success": False,
                            "error": "No generated_sql in response"
                        }
                except json.JSONDecodeError:
                    return {
                        "duration": duration,
                        "success": False,
                        "error": "Invalid JSON response"
                    }
            else:
                return {
                    "duration": duration,
                    "success": False,
                    "error": result.stderr
                }

        except subprocess.TimeoutExpired:
            return {
                "duration": 30.0,
                "success": False,
                "error": "Timeout"
            }
        except Exception as e:
            return {
                "duration": time.time() - start_time,
                "success": False,
                "error": str(e)
            }

    def run_baseline(self, iterations: int = 5) -> Dict[str, Any]:
        """Run baseline Cypher-to-SQL translation performance measurements"""
        print("Running baseline Cypher-to-SQL translation performance measurements...")

        results = {}

        for query_info in self.test_queries:
            query_name = query_info["name"]
            query = query_info["query"]
            description = query_info["description"]

            print(f"  Testing {query_name}: {description}")

            durations = []
            successes = 0

            for i in range(iterations):
                print(f"    Iteration {i+1}/{iterations}...")
                result = self._run_single_query(query)

                if result["success"]:
                    durations.append(result["duration"])
                    successes += 1
                else:
                    print(f"      Failed: {result['error']}")

            if durations:
                results[query_name] = {
                    "query": query,
                    "description": description,
                    "iterations": iterations,
                    "success_rate": successes / iterations,
                    "avg_duration": statistics.mean(durations),
                    "min_duration": min(durations),
                    "max_duration": max(durations),
                    "std_duration": statistics.stdev(durations) if len(durations) > 1 else 0,
                    "timestamp": time.time()
                }
            else:
                results[query_name] = {
                    "query": query,
                    "description": description,
                    "error": "All iterations failed",
                    "timestamp": time.time()
                }

        # Save results
        with open(self.results_file, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"Baseline results saved to {self.results_file}")
        return results

    def compare_with_baseline(self) -> Dict[str, Any]:
        """Compare current performance with baseline"""
        if not self.results_file.exists():
            print("No baseline found. Run with --baseline first.")
            return {}

        print("Comparing with baseline...")

        with open(self.results_file, 'r') as f:
            baseline = json.load(f)

        current_results = self.run_baseline(iterations=3)  # Fewer iterations for comparison

        comparison = {}

        for query_name in baseline:
            if query_name in current_results:
                baseline_data = baseline[query_name]
                current_data = current_results[query_name]

                if "avg_duration" in baseline_data and "avg_duration" in current_data:
                    baseline_avg = baseline_data["avg_duration"]
                    current_avg = current_data["avg_duration"]

                    regression_pct = ((current_avg - baseline_avg) / baseline_avg) * 100

                    comparison[query_name] = {
                        "baseline_avg": baseline_avg,
                        "current_avg": current_avg,
                        "regression_pct": regression_pct,
                        "acceptable": abs(regression_pct) < 5.0  # 5% threshold
                    }

                    status = "✅ PASS" if abs(regression_pct) < 5.0 else "❌ FAIL"
                    print(f"  {query_name}: {status} ({regression_pct:.2f}%)")
        return comparison

def main():
    parser = argparse.ArgumentParser(description="Plan Builder Performance Benchmark")
    parser.add_argument("--baseline", action="store_true", help="Run baseline measurements")
    parser.add_argument("--compare", action="store_true", help="Compare with baseline")
    parser.add_argument("--iterations", type=int, default=5, help="Number of iterations per query")

    args = parser.parse_args()

    benchmark = CypherTranslationBenchmark()

    if args.baseline:
        results = benchmark.run_baseline(args.iterations)
        print("\nBaseline Results Summary:")
        for name, data in results.items():
            if "avg_duration" in data:
                print(".3f")
            else:
                print(f"  {name}: FAILED")

    elif args.compare:
        comparison = benchmark.compare_with_baseline()
        if comparison:
            failures = sum(1 for r in comparison.values() if not r.get("acceptable", False))
            if failures == 0:
                print("✅ All performance checks passed!")
                sys.exit(0)
            else:
                print(f"❌ {failures} performance regressions detected!")
                sys.exit(1)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()