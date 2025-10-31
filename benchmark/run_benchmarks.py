#!/usr/bin/env python3
"""
ClickGraph Benchmark Runner

Automated benchmark execution with multiple configurations and comparative analysis.

Usage:
    python run_benchmarks.py                    # Run default benchmark suite
    python run_benchmarks.py --comprehensive   # Run all configurations
    python run_benchmarks.py --quick           # Quick validation run
    python run_benchmarks.py --compare         # Compare different query types
"""

import subprocess
import json
import os
import argparse
from datetime import datetime
import statistics

class BenchmarkRunner:
    """Automated benchmark execution and analysis."""

    def __init__(self):
        self.results_dir = "benchmark_results"
        os.makedirs(self.results_dir, exist_ok=True)

    def run_command(self, cmd: str, description: str = "") -> bool:
        """Run a command and return success status."""
        print(f"🔧 {description}")
        print(f"   $ {cmd}")

        try:
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
            print("   ✅ Success")
            return True
        except subprocess.CalledProcessError as e:
            print(f"   ❌ Failed: {e}")
            print(f"   Error output: {e.stderr}")
            return False

    def setup_data(self, dataset: str = "social", size: str = "small") -> bool:
        """Set up benchmark data."""
        print(f"📊 Setting up {dataset} dataset ({size})...")

        cmd = f"python setup_benchmark_data.py --dataset {dataset} --size {size}"
        return self.run_command(cmd, f"Setting up {dataset} {size} dataset")

    def run_benchmark(self, dataset: str, queries: str, iterations: int, output_file: str) -> bool:
        """Run a single benchmark configuration."""
        print(f"🏃 Running {dataset} benchmark ({queries})...")

        cmd = f"python benchmark.py --dataset {dataset} --queries {queries} --iterations {iterations} --output {output_file}"
        return self.run_command(cmd, f"Benchmarking {dataset} with {queries} queries")

    def run_default_suite(self) -> bool:
        """Run the default benchmark suite."""
        print("🚀 Running Default Benchmark Suite")
        print("=" * 50)

        success = True

        # Setup data
        if not self.setup_data("social", "small"):
            return False

        # Run benchmarks
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        configs = [
            ("social", "simple", 5),
            ("social", "traversal", 5),
            ("social", "aggregation", 3),
            ("social", "variable_length", 3),
        ]

        for dataset, query_type, iterations in configs:
            output_file = f"{self.results_dir}/benchmark_{dataset}_{query_type}_{timestamp}.json"
            if not self.run_benchmark(dataset, query_type, iterations, output_file):
                success = False

        return success

    def run_comprehensive_suite(self) -> bool:
        """Run comprehensive benchmark suite with all configurations."""
        print("🚀 Running Comprehensive Benchmark Suite")
        print("=" * 50)

        success = True
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Setup multiple datasets
        datasets = [("social", "small"), ("ecommerce", "small")]

        for dataset, size in datasets:
            if not self.setup_data(dataset, size):
                success = False
                continue

            # Run all query types
            output_file = f"{self.results_dir}/comprehensive_{dataset}_{size}_{timestamp}.json"
            if not self.run_benchmark(dataset, "all", 3, output_file):
                success = False

        return success

    def run_quick_validation(self) -> bool:
        """Run quick validation benchmark."""
        print("⚡ Running Quick Validation")
        print("=" * 30)

        # Setup minimal data
        if not self.setup_data("social", "small"):
            return False

        # Run minimal benchmark
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{self.results_dir}/quick_validation_{timestamp}.json"

        return self.run_benchmark("social", "simple", 1, output_file)

    def run_comparison_suite(self) -> bool:
        """Run comparative analysis between query types."""
        print("📊 Running Query Type Comparison")
        print("=" * 35)

        success = True
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Setup data
        if not self.setup_data("social", "small"):
            return False

        query_types = ["simple", "traversal", "aggregation", "variable_length"]

        for query_type in query_types:
            output_file = f"{self.results_dir}/compare_{query_type}_{timestamp}.json"
            if not self.run_benchmark("social", query_type, 5, output_file):
                success = False

        # Generate comparison report
        self.generate_comparison_report(timestamp)

        return success

    def generate_comparison_report(self, timestamp: str):
        """Generate a comparison report across query types."""
        print("📈 Generating comparison report...")

        results = {}
        for query_type in ["simple", "traversal", "aggregation", "variable_length"]:
            result_file = f"{self.results_dir}/compare_{query_type}_{timestamp}.json"
            if os.path.exists(result_file):
                with open(result_file, 'r') as f:
                    results[query_type] = json.load(f)

        if not results:
            print("❌ No result files found for comparison")
            return

        # Generate summary
        comparison = {
            "comparison_timestamp": datetime.now().isoformat(),
            "query_types_compared": list(results.keys()),
            "summary": {}
        }

        for query_type, data in results.items():
            if "results" in data:
                successful_queries = sum(1 for r in data["results"] if r["success_rate"] > 0)
                total_queries = len(data["results"])

                # Calculate average performance
                avg_times = []
                for query_result in data["results"]:
                    if query_result["success_rate"] > 0 and "total_time_stats" in query_result:
                        avg_times.append(query_result["total_time_stats"]["mean"])

                comparison["summary"][query_type] = {
                    "successful_queries": successful_queries,
                    "total_queries": total_queries,
                    "success_rate": successful_queries / total_queries if total_queries > 0 else 0,
                    "avg_query_time": statistics.mean(avg_times) if avg_times else 0,
                    "total_benchmark_time": sum(avg_times) if avg_times else 0
                }

        # Save comparison report
        report_file = f"{self.results_dir}/comparison_report_{timestamp}.json"
        with open(report_file, 'w') as f:
            json.dump(comparison, f, indent=2)

        print(f"✅ Comparison report saved: {report_file}")

        # Print summary
        print("\n📊 QUERY TYPE COMPARISON SUMMARY")
        print("=" * 40)
        for query_type, stats in comparison["summary"].items():
            print(f"{query_type.upper()}:")
            print(".3f")
            print(".3f")
            print()

    def show_available_benchmarks(self):
        """Show available benchmark options."""
        print("🎯 Available Benchmark Suites:")
        print()
        print("1. DEFAULT SUITE")
        print("   - Social network dataset (small)")
        print("   - Query types: simple, traversal, aggregation, variable_length")
        print("   - Iterations: 3-5 per query type")
        print("   - Use: Standard performance evaluation")
        print()
        print("2. COMPREHENSIVE SUITE")
        print("   - Social + E-commerce datasets (small)")
        print("   - All query types for each dataset")
        print("   - Iterations: 3 per configuration")
        print("   - Use: Complete feature coverage testing")
        print()
        print("3. QUICK VALIDATION")
        print("   - Social network dataset (small)")
        print("   - Simple queries only")
        print("   - Iterations: 1")
        print("   - Use: Fast functionality check")
        print()
        print("4. COMPARISON SUITE")
        print("   - Social network dataset (small)")
        print("   - All query types (separate runs)")
        print("   - Iterations: 5 per query type")
        print("   - Use: Compare performance across query types")
        print()

def main():
    parser = argparse.ArgumentParser(description="ClickGraph Benchmark Runner")
    parser.add_argument("--comprehensive", action="store_true",
                       help="Run comprehensive benchmark suite")
    parser.add_argument("--quick", action="store_true",
                       help="Run quick validation")
    parser.add_argument("--compare", action="store_true",
                       help="Run query type comparison")
    parser.add_argument("--list", action="store_true",
                       help="List available benchmark suites")

    args = parser.parse_args()

    runner = BenchmarkRunner()

    if args.list:
        runner.show_available_benchmarks()
        return

    # Determine which suite to run
    if args.comprehensive:
        success = runner.run_comprehensive_suite()
    elif args.quick:
        success = runner.run_quick_validation()
    elif args.compare:
        success = runner.run_comparison_suite()
    else:
        # Default suite
        success = runner.run_default_suite()

    if success:
        print("\n🎉 Benchmark suite completed successfully!")
        print(f"📁 Results saved in: {runner.results_dir}/")
        print("\n💡 Next steps:")
        print("   - Review JSON result files for detailed metrics")
        print("   - Compare results across different runs")
        print("   - Analyze performance bottlenecks")
    else:
        print("\n❌ Benchmark suite failed!")
        print("   Check the output above for error details")
        exit(1)

if __name__ == "__main__":
    main()