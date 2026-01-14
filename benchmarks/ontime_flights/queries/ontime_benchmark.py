#!/usr/bin/env python3
"""
OnTime Flight Benchmark Queries for ClickGraph

Based on PuppyGraph's ClickHouse benchmark adapted for ClickGraph.
Tests multi-hop flight path queries on denormalized edge tables.

Dataset: OnTime 2020-2022 (~12M flights, 435 airports)
Schema: benchmarks/schemas/ontime_benchmark.yaml

ClickHouse Connection:
  - Host: localhost:18123
  - User: test_user
  - Password: test_pass
  - Database: default
  - Table: flights

Usage:
    python benchmarks/queries/ontime_benchmark.py [--sql-only] [--query Q1|Q2|Q3|Q4|all]
"""

import requests
import json
import time
import argparse
from typing import Optional

# ClickGraph server endpoint
CLICKGRAPH_URL = "http://localhost:8080"
SCHEMA_NAME = "ontime_flights"

# Benchmark queries adapted from PuppyGraph
# Original: https://github.com/nicholaskarlson/ClickHouse-PuppyGraph-test

QUERIES = {
    # Q1: 2-hop connecting flights by month
    # Find all connecting flights from airport 12892 to 12953 in 2022
    # where layover time is at least 100 minutes
    "Q1": {
        "name": "Connecting Flights by Month (2-hop)",
        "description": "Find connecting flight paths with minimum layover time",
        "cypher": """
MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport)
WHERE r1.flight_date = r2.flight_date
  AND r1.crs_arrival_time + 100 <= r2.crs_departure_time
  AND a.id = 12892
  AND c.id = 12953
  AND r1.year = 2022
RETURN r1.month as month, count(*) as path_count
ORDER BY month
""".strip(),
    },
    
    # Q2: Same as Q1 but with actual delay constraint  
    # Flights where actual arrival >= scheduled departure (delayed connection)
    "Q2": {
        "name": "Delayed Connecting Flights (2-hop)",
        "description": "Connecting flights where arrival delay affects connection",
        "cypher": """
MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport)
WHERE r1.flight_date = r2.flight_date
  AND r1.crs_arrival_time + 100 <= r2.crs_departure_time
  AND r1.arrival_time >= r2.departure_time
  AND a.id = 12892
  AND c.id = 12953
  AND r1.year = 2022
RETURN r1.month as month, count(*) as path_count
ORDER BY month
""".strip(),
    },
    
    # Q3: Hub airport analysis - same aircraft turnaround
    # Find airports with most distinct aircraft doing same-day turnarounds
    "Q3": {
        "name": "Hub Airport Analysis (same aircraft)",
        "description": "Top airports by distinct aircraft turnarounds on a single day",
        "cypher": """
MATCH (a:Airport)-[r1:FLIGHT]->(n:Airport)-[r2:FLIGHT]->(c:Airport)
WHERE r1.flight_date = '2022-06-08'
  AND r1.flight_date = r2.flight_date
  AND r1.tail_num = r2.tail_num
  AND r1.tail_num IS NOT NULL
  AND r1.tail_num <> ''
  AND r1.crs_arrival_time < r2.crs_departure_time
RETURN n.code as hub_airport, count(DISTINCT r1.tail_num) as aircraft_count
ORDER BY aircraft_count DESC
LIMIT 10
""".strip(),
    },
    
    # Q4: 3-hop same-aircraft journey
    # Count distinct aircraft flying 3+ legs on a single day
    "Q4": {
        "name": "3-Hop Same Aircraft Journey",
        "description": "Aircraft flying 3+ consecutive legs on a single day",
        "cypher": """
MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport)-[r3:FLIGHT]->(d:Airport)
WHERE r1.flight_date = '2022-08-08'
  AND r1.flight_date = r2.flight_date
  AND r2.flight_date = r3.flight_date
  AND r1.tail_num = r2.tail_num
  AND r2.tail_num = r3.tail_num
  AND r1.tail_num IS NOT NULL
  AND r1.tail_num <> ''
  AND r1.crs_arrival_time < r2.crs_departure_time
  AND r2.crs_arrival_time < r3.crs_departure_time
RETURN count(DISTINCT r1.tail_num) as aircraft_count
""".strip(),
    },
}


def load_schema() -> bool:
    """Load the OnTime schema into ClickGraph."""
    schema_path = "benchmarks/schemas/ontime_benchmark.yaml"
    try:
        with open(schema_path, 'r') as f:
            schema_content = f.read()
        
        response = requests.post(
            f"{CLICKGRAPH_URL}/schemas/load",
            json={
                "schema_name": SCHEMA_NAME,
                "config_content": schema_content,
                "validate_schema": False
            },
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"✅ Schema '{SCHEMA_NAME}' loaded successfully")
            return True
        else:
            print(f"⚠️ Schema load response: {response.text}")
            return response.status_code == 200
    except FileNotFoundError:
        print(f"❌ Schema file not found: {schema_path}")
        return False
    except Exception as e:
        print(f"❌ Failed to load schema: {e}")
        return False


def run_query(query_id: str, sql_only: bool = False) -> Optional[dict]:
    """Run a benchmark query and return results with timing."""
    if query_id not in QUERIES:
        print(f"❌ Unknown query: {query_id}")
        return None
    
    query_info = QUERIES[query_id]
    cypher = query_info["cypher"]
    
    print(f"\n{'='*60}")
    print(f"📊 {query_id}: {query_info['name']}")
    print(f"   {query_info['description']}")
    print(f"{'='*60}")
    print(f"\nCypher Query:\n{cypher}\n")
    
    payload = {
        "query": cypher,
        "schema_name": SCHEMA_NAME,
        "sql_only": sql_only
    }
    
    try:
        start_time = time.time()
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json=payload,
            timeout=300  # 5 min timeout for complex queries
        )
        elapsed = time.time() - start_time
        
        if response.status_code != 200:
            print(f"❌ Query failed: {response.status_code}")
            print(f"   {response.text}")
            return None
        
        result = response.json()
        
        if sql_only:
            print(f"Generated SQL:\n{result.get('generated_sql', result)}")
        else:
            print(f"⏱️ Execution time: {elapsed:.3f}s")
            if 'data' in result:
                print(f"📈 Rows returned: {len(result['data'])}")
                # Show first few results
                for i, row in enumerate(result['data'][:5]):
                    print(f"   {row}")
                if len(result['data']) > 5:
                    print(f"   ... ({len(result['data']) - 5} more rows)")
            else:
                print(f"Result: {json.dumps(result, indent=2)}")
        
        return {
            "query_id": query_id,
            "elapsed": elapsed,
            "result": result,
            "sql_only": sql_only
        }
        
    except requests.exceptions.Timeout:
        print(f"❌ Query timed out after 300s")
        return None
    except Exception as e:
        print(f"❌ Query error: {e}")
        return None


def run_all_queries(sql_only: bool = False) -> list:
    """Run all benchmark queries."""
    results = []
    for query_id in QUERIES:
        result = run_query(query_id, sql_only)
        if result:
            results.append(result)
    return results


def print_summary(results: list):
    """Print benchmark summary."""
    if not results:
        print("\n❌ No results to summarize")
        return
    
    print(f"\n{'='*60}")
    print("📊 BENCHMARK SUMMARY")
    print(f"{'='*60}")
    
    total_time = 0
    for r in results:
        if not r.get("sql_only"):
            total_time += r.get("elapsed", 0)
            row_count = len(r.get("result", {}).get("data", [])) if r.get("result") else 0
            print(f"  {r['query_id']}: {r['elapsed']:.3f}s ({row_count} rows)")
    
    if total_time > 0:
        print(f"\n  Total execution time: {total_time:.3f}s")
        print(f"  Average per query: {total_time/len(results):.3f}s")


def main():
    parser = argparse.ArgumentParser(description="OnTime Flight Benchmark for ClickGraph")
    parser.add_argument("--sql-only", action="store_true", 
                       help="Only generate SQL, don't execute")
    parser.add_argument("--query", type=str, default="all",
                       help="Query to run: Q1, Q2, Q3, Q4, or 'all'")
    parser.add_argument("--skip-schema-load", action="store_true",
                       help="Skip loading schema (if already loaded)")
    args = parser.parse_args()
    
    print("🛫 OnTime Flight Benchmark for ClickGraph")
    print("="*60)
    
    # Load schema first
    if not args.skip_schema_load:
        if not load_schema():
            print("⚠️ Continuing without schema verification...")
    
    # Run queries
    if args.query.lower() == "all":
        results = run_all_queries(args.sql_only)
        if not args.sql_only:
            print_summary(results)
    else:
        query_id = args.query.upper()
        run_query(query_id, args.sql_only)
    
    print("\n✅ Benchmark complete!")


if __name__ == "__main__":
    main()
