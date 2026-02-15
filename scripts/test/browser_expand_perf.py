#!/usr/bin/env python3
"""
Browser Expand Performance Test Runner

Quick script to test browser expand query performance without pytest overhead.
Useful for rapid iteration and benchmarking optimization attempts.

Usage:
    python scripts/test/browser_expand_perf.py              # Run all tests
    python scripts/test/browser_expand_perf.py --query-only  # Show queries without executing
    python scripts/test/browser_expand_perf.py --iterations 50  # Run 50 iterations for average
    python scripts/test/browser_expand_perf.py --node-id 100  # Test specific node

Requirements:
    - ClickGraph running on localhost:8080 or CLICKGRAPH_URL
    - ClickHouse with social_benchmark schema
    - requests library
"""

import requests
import time
import json
import sys
import os
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from statistics import mean, stdev


@dataclass
class QueryResult:
    query: str
    elapsed: float
    record_count: int
    success: bool
    error: str = None


class BrowserExpandTester:
    """Test harness for browser expand queries."""
    
    def __init__(self, base_url: str = None):
        self.base_url = base_url or os.getenv(
            "CLICKGRAPH_URL", "http://localhost:8080"
        )
        self.results = []
    
    def execute_query(self, cypher: str, sql_only: bool = False) -> QueryResult:
        """Execute a Cypher query and measure latency."""
        payload = {
            "query": cypher,
            "sql_only": sql_only
        }
        
        start = time.time()
        try:
            response = requests.post(
                f"{self.base_url}/query",
                json=payload,
                timeout=30
            )
            elapsed = time.time() - start
            
            if response.status_code != 200:
                return QueryResult(
                    query=cypher[:80],
                    elapsed=elapsed,
                    record_count=0,
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text[:100]}"
                )
            
            data = response.json()
            if "error" in data:
                return QueryResult(
                    query=cypher[:80],
                    elapsed=elapsed,
                    record_count=0,
                    success=False,
                    error=data["error"]
                )
            
            records = data.get("records", [])
            return QueryResult(
                query=cypher[:80],
                elapsed=elapsed,
                record_count=len(records),
                success=True
            ), data
        
        except Exception as e:
            elapsed = time.time() - start
            return QueryResult(
                query=cypher[:80],
                elapsed=elapsed,
                record_count=0,
                success=False,
                error=str(e)
            ), None
    
    def get_sample_nodes(self, count: int = 5) -> List[int]:
        """Fetch sample node element_ids from the database."""
        query = f"""
        USE social_benchmark
        MATCH (n:User) RETURN id(n) as element_id LIMIT {count}
        """
        
        result, data = self.execute_query(query)
        if not result.success or not data:
            return []
        
        # Extract element_ids from results
        element_ids = []
        for result_item in data.get("results", []):
            if result_item and "element_id" in result_item:
                element_ids.append(result_item["element_id"])
        
        return element_ids
    
    def test_q1_fetch_node(self, element_id: int) -> Tuple[QueryResult, Dict]:
        """Browser Q1: Fetch single node by id."""
        query = f"""
        USE social_benchmark
        MATCH (a) WHERE id(a) = {element_id} RETURN a
        """
        return self.execute_query(query)
    
    def test_q2_expand_unfiltered(self, element_id: int) -> Tuple[QueryResult, Dict]:
        """Browser Q2 (simple): Expand to all adjacent nodes."""
        query = f"""
        USE social_benchmark
        MATCH (a)--(o) WHERE id(a) = {element_id} RETURN o LIMIT 100
        """
        result, data = self.execute_query(query)
        return result, data
    
    def test_q2_expand_filtered(self, element_id: int, exclude_element_ids: List[int] = None) -> Tuple[QueryResult, Dict]:
        """Browser Q2 (realistic): Expand with exclude list."""
        exclude_element_ids = exclude_element_ids or []
        exclude_str = ", ".join(str(i) for i in exclude_element_ids[:5])  # Limit exclude list
        
        if exclude_str:
            query = f"""
            USE social_benchmark
            MATCH (a)--(o) 
            WHERE id(a) = {element_id} AND NOT id(o) IN [{exclude_str}]
            RETURN o LIMIT 100
            """
        else:
            query = f"""
            USE social_benchmark
            MATCH (a)--(o) 
            WHERE id(a) = {element_id}
            RETURN o LIMIT 100
            """
        result, data = self.execute_query(query)
        return result, data
    
    def test_q2_directed(self, element_id: int) -> Tuple[QueryResult, Dict]:
        """Optimization candidate: Use directed patterns."""
        query = f"""
        USE social_benchmark
        MATCH (a) --> (o) WHERE id(a) = {element_id} RETURN o LIMIT 50
        UNION ALL
        MATCH (a) <-- (o) WHERE id(a) = {element_id} RETURN o LIMIT 50
        """
        result, data = self.execute_query(query)
        return result, data
    
    def test_q2_specific_types(self, element_id: int) -> Tuple[QueryResult, Dict]:
        """Optimization candidate: Filter to specific relationship types."""
        query = f"""
        USE social_benchmark
        MATCH (a)-[:FOLLOWS]->(o) WHERE id(a) = {element_id} RETURN o LIMIT 50
        UNION ALL
        MATCH (a)<-[:FOLLOWS]-(o) WHERE id(a) = {element_id} RETURN o LIMIT 50
        UNION ALL
        MATCH (a)-[:AUTHORED]->(o) WHERE id(a) = {element_id} RETURN o LIMIT 50
        UNION ALL
        MATCH (a)-[:LIKED]->(o) WHERE id(a) = {element_id} RETURN o LIMIT 50
        """
        result, data = self.execute_query(query)
        return result, data
    
    def print_result(self, name: str, result: QueryResult, threshold: float = 0.5):
        """Pretty-print a query result."""
        status = "✓" if result.success else "✗"
        color_code = "\033[92m" if result.success else "\033[91m"  # Green or red
        reset_code = "\033[0m"
        
        if result.elapsed > threshold:
            color_code = "\033[93m"  # Yellow for slow
        
        time_ms = result.elapsed * 1000
        perf_note = ""
        if result.success:
            if result.elapsed > 10:
                perf_note = f" ⚠ VERY SLOW ({time_ms:.0f}ms)"
            elif result.elapsed > 1:
                perf_note = f" ⚠ SLOW ({time_ms:.0f}ms)"
            elif result.elapsed > threshold:
                perf_note = f" ⚠ slightly slow ({time_ms:.0f}ms)"
            else:
                perf_note = f" ✓ fast ({time_ms:.0f}ms)"
        
        print(f"{color_code}{status}{reset_code} {name:35} | "
              f"{time_ms:7.1f}ms | Records: {result.record_count:5} {perf_note}")
        
        if result.error:
            print(f"  └─ Error: {result.error}")
    
    def benchmark_query(self, query_fn, iterations: int = 5, **kwargs) -> Dict[str, Any]:
        """Run query multiple times and collect statistics."""
        times = []
        
        for i in range(iterations):
            try:
                result, data = query_fn(**kwargs)
                if result.success:
                    times.append(result.elapsed)
            except:
                pass
        
        if not times:
            return {"error": "All iterations failed"}
        
        return {
            "count": len(times),
            "min_ms": min(times) * 1000,
            "max_ms": max(times) * 1000,
            "avg_ms": mean(times) * 1000,
            "stdev_ms": stdev(times) * 1000 if len(times) > 1 else 0,
        }


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Browser expand performance testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests once (fetches real element_ids from database)
  %(prog)s
  
  # Show generated SQL without executing
  %(prog)s --sql-only
  
  # Benchmark with 20 iterations to get average
  %(prog)s --iterations 20 --benchmark
  
  # Test with specific ClickGraph URL
  %(prog)s --url http://remote-host:8080
        """
    )
    
    parser.add_argument("--url", help="ClickGraph base URL (default: localhost:8080)")
    parser.add_argument("--iterations", type=int, default=1, help="Iterations per test")
    parser.add_argument("--sql-only", action="store_true", help="Show SQL only")
    parser.add_argument("--benchmark", action="store_true", help="Run benchmarks with stats")
    
    args = parser.parse_args()
    
    tester = BrowserExpandTester(args.url)
    
    print("""
╔═════════════════════════════════════════════════════════════════════╗
║     Browser Expand Query Performance Tester                         ║
║                                                                     ║
║  Tests the exact two-query sequence Neo4j Browser sends when       ║
║  users click to expand nodes in the graph visualization            ║
║                                                                     ║
║  Target: <500ms per query for interactive feel                     ║
║  Current: ~6-10s (undirected pattern on 250M-row tables)           ║
║                                                                     ║
║  Note: Uses REAL element_ids from database (not hardcoded values)  ║
╚═════════════════════════════════════════════════════════════════════╝
    """)
    
    if args.sql_only:
        print("=== Sample SQL (will fetch real element_ids when run) ===\n")
        print("Step 1: Fetch sample nodes to get element_ids")
        print("USE social_benchmark")
        print("MATCH (n:User) RETURN n LIMIT 5\n")
        
        print("Step 2: Q1 (Fetch node by element_id):")
        print("USE social_benchmark")
        print("MATCH (a) WHERE id(a) = <element_id> RETURN a\n")
        
        print("Step 3: Q2 (Expand undirected):")
        print("USE social_benchmark")
        print("MATCH (a)--(o) WHERE id(a) = <element_id> RETURN o LIMIT 100\n")
        
        print("Step 4: Q2 (Expand with exclude list):")
        print("USE social_benchmark")
        print("MATCH (a)--(o) WHERE id(a) = <element_id> AND NOT id(o) IN [...] RETURN o LIMIT 100\n")
        
        print("Step 5: Optimization - Directed patterns")
        print("MATCH (a) --> (o) WHERE id(a) = <element_id> RETURN o LIMIT 50")
        print("UNION ALL")
        print("MATCH (a) <-- (o) WHERE id(a) = <element_id> RETURN o LIMIT 50\n")
        
        return 0
    
    # ===== Fetch real nodes and extract element_ids =====
    print("Fetching sample node element_ids from database...\n")
    
    element_ids = tester.get_sample_nodes(count=5)
    if not element_ids:
        print("✗ Failed to fetch sample node element_ids. Exiting.")
        return 1
    
    print(f"✓ Found {len(element_ids)} sample nodes with element_ids:\n")
    
    for i, eid in enumerate(element_ids, 1):
        print(f"  [{i}] element_id={eid}")
    
    print(f"\n✓ Using element_id={element_ids[0]} for testing\n")
    
    # Use first node for testing
    test_element_id = element_ids[0]
    exclude_element_ids = element_ids[1:3] if len(element_ids) > 1 else []
    
    # ===== Run tests =====
    print("=== Single Run ===\n")
    
    r1, _ = tester.test_q1_fetch_node(test_element_id)
    tester.print_result("Q1: Fetch single node", r1, threshold=0.1)
    
    r2, _ = tester.test_q2_expand_unfiltered(test_element_id)
    tester.print_result("Q2: Expand unfiltered", r2)
    
    r3, _ = tester.test_q2_expand_filtered(test_element_id, exclude_element_ids)
    tester.print_result("Q2: Expand with exclude", r3)
    
    r4, _ = tester.test_q2_directed(test_element_id)
    tester.print_result("OPT: Directed patterns", r4)
    
    r5, _ = tester.test_q2_specific_types(test_element_id)
    tester.print_result("OPT: Specific rel types", r5)
    
    # ===== Benchmarks if requested =====
    if args.benchmark and args.iterations > 1:
        print(f"\n=== Benchmarks ({args.iterations} iterations) ===\n")
        
        b1 = tester.benchmark_query(tester.test_q1_fetch_node, args.iterations, 
                                    element_id=test_element_id)
        print(f"Q1 (Fetch):              {b1.get('avg_ms', 'err'):7.1f}ms "
              f"(min:{b1.get('min_ms', 0):6.1f}, max:{b1.get('max_ms', 0):6.1f})")
        
        b2 = tester.benchmark_query(tester.test_q2_expand_unfiltered, args.iterations,
                                    element_id=test_element_id)
        print(f"Q2 (Expand unfiltered): {b2.get('avg_ms', 'err'):7.1f}ms "
              f"(min:{b2.get('min_ms', 0):6.1f}, max:{b2.get('max_ms', 0):6.1f})")
        
        b3 = tester.benchmark_query(tester.test_q2_expand_filtered, args.iterations,
                                    element_id=test_element_id, exclude_element_ids=exclude_element_ids)
        print(f"Q2 (Expand filtered):   {b3.get('avg_ms', 'err'):7.1f}ms "
              f"(min:{b3.get('min_ms', 0):6.1f}, max:{b3.get('max_ms', 0):6.1f})")
        
        b4 = tester.benchmark_query(tester.test_q2_directed, args.iterations,
                                    element_id=test_element_id)
        print(f"OPT (Directed):         {b4.get('avg_ms', 'err'):7.1f}ms "
              f"(min:{b4.get('min_ms', 0):6.1f}, max:{b4.get('max_ms', 0):6.1f})")
        
        b5 = tester.benchmark_query(tester.test_q2_specific_types, args.iterations,
                                    element_id=test_element_id)
        print(f"OPT (Specific types):   {b5.get('avg_ms', 'err'):7.1f}ms "
              f"(min:{b5.get('min_ms', 0):6.1f}, max:{b5.get('max_ms', 0):6.1f})")
    
    print("\n" + "="*70)
    print("Performance Target: <500ms per query for interactive browser")
    print("Current Bottleneck: post_likes_bench (250M rows)")
    print("="*70)
    
    return 0



if __name__ == "__main__":
    sys.exit(main())
