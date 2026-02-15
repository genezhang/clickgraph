#!/usr/bin/env python3
"""
OnTime Flight Benchmark - Full Performance Test

Runs all 4 PuppyGraph-style queries across 2021-2023 data,
5 iterations each, measuring execution time.

Usage:
    python benchmarks/queries/run_ontime_benchmark.py
"""

import time
import statistics
import json
import requests
from datetime import datetime

# ClickHouse connection
CLICKHOUSE_URL = "http://localhost:18123"
CLICKHOUSE_USER = "test_user"
CLICKHOUSE_PASSWORD = "test_pass"

# ClickGraph connection (for SQL generation verification)
CLICKGRAPH_URL = "http://localhost:8080"

ITERATIONS = 5
YEARS = [2021, 2022, 2023]

# Sample dates for each year (mid-year weekdays with good flight activity)
SAMPLE_DATES = {
    2021: "2021-06-08",
    2022: "2022-06-08",
    2023: "2023-06-08",
}

# Q4 uses August dates (typically busier)
Q4_DATES = {
    2021: "2021-08-08",
    2022: "2022-08-08",
    2023: "2023-08-08",
}

def run_clickhouse_query(sql: str, timeout: int = 120) -> tuple[dict | None, float]:
    """Execute SQL on ClickHouse and return (result, elapsed_seconds)."""
    start = time.perf_counter()
    try:
        response = requests.post(
            f"{CLICKHOUSE_URL}/?user={CLICKHOUSE_USER}&password={CLICKHOUSE_PASSWORD}",
            data=sql + " FORMAT JSON",
            timeout=timeout
        )
        elapsed = time.perf_counter() - start
        if response.status_code == 200:
            return response.json(), elapsed
        else:
            print(f"  ❌ Error: {response.text[:200]}")
            return None, elapsed
    except requests.exceptions.Timeout:
        elapsed = time.perf_counter() - start
        print(f"  ❌ Timeout after {elapsed:.2f}s")
        return None, elapsed
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"  ❌ Exception: {e}")
        return None, elapsed


def get_queries(year: int) -> dict:
    """Generate SQL queries for a specific year."""
    date = SAMPLE_DATES[year]
    q4_date = Q4_DATES[year]
    
    return {
        "Q1": {
            "name": "2-hop connecting flights by month",
            "sql": f"""
SELECT 
    r1.Month AS month, 
    count(*) AS path_count
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.OriginAirportID = r1.DestAirportID
WHERE r1.OriginAirportID = 12892 
  AND r2.DestAirportID = 12953
  AND r1.Year = {year}
  AND r1.FlightDate = r2.FlightDate
  AND r1.CRSArrTime + 100 <= r2.CRSDepTime
GROUP BY r1.Month
ORDER BY month
"""
        },
        "Q2": {
            "name": "Delayed connecting flights",
            "sql": f"""
SELECT 
    r1.Month AS month, 
    count(*) AS path_count
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.OriginAirportID = r1.DestAirportID
WHERE r1.OriginAirportID = 12892 
  AND r2.DestAirportID = 12953
  AND r1.Year = {year}
  AND r1.FlightDate = r2.FlightDate
  AND r1.CRSArrTime + 100 <= r2.CRSDepTime
  AND r1.ArrTime >= r2.DepTime
GROUP BY r1.Month
ORDER BY month
"""
        },
        "Q3": {
            "name": f"Hub airport analysis ({date})",
            "sql": f"""
SELECT 
    r2.Origin AS hub_airport, 
    count(DISTINCT r1.Tail_Number) AS aircraft_count
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.OriginAirportID = r1.DestAirportID
WHERE r1.FlightDate = '{date}' 
  AND r1.FlightDate = r2.FlightDate
  AND r1.Tail_Number = r2.Tail_Number
  AND r1.Tail_Number IS NOT NULL 
  AND r1.Tail_Number <> ''
  AND r1.CRSArrTime < r2.CRSDepTime
GROUP BY r2.Origin
ORDER BY aircraft_count DESC
LIMIT 10
"""
        },
        "Q4": {
            "name": f"3-hop same aircraft journey ({q4_date})",
            "sql": f"""
SELECT 
    count(DISTINCT r1.Tail_Number) AS aircraft_count
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.OriginAirportID = r1.DestAirportID
INNER JOIN default.flights AS r3 ON r3.OriginAirportID = r2.DestAirportID
WHERE r1.FlightDate = '{q4_date}' 
  AND r1.FlightDate = r2.FlightDate 
  AND r2.FlightDate = r3.FlightDate
  AND r1.Tail_Number = r2.Tail_Number 
  AND r2.Tail_Number = r3.Tail_Number
  AND r1.Tail_Number IS NOT NULL 
  AND r1.Tail_Number <> ''
  AND r1.CRSArrTime < r2.CRSDepTime 
  AND r2.CRSArrTime < r3.CRSDepTime
"""
        },
    }


def run_benchmark():
    """Run the full benchmark suite."""
    print("=" * 70)
    print("OnTime Flight Benchmark - ClickGraph vs ClickHouse")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Years: {YEARS}")
    print(f"Iterations per query: {ITERATIONS}")
    print("=" * 70)
    
    # Results structure: {query_id: {year: [times]}}
    all_results = {}
    sample_outputs = {}
    
    for year in YEARS:
        print(f"\n{'='*70}")
        print(f"YEAR {year}")
        print("=" * 70)
        
        queries = get_queries(year)
        
        for qid, qinfo in queries.items():
            print(f"\n📊 {qid}: {qinfo['name']}")
            
            times = []
            result_sample = None
            
            for i in range(ITERATIONS):
                result, elapsed = run_clickhouse_query(qinfo["sql"])
                if result:
                    times.append(elapsed)
                    if result_sample is None:
                        result_sample = result.get("data", [])[:3]  # First 3 rows
                    print(f"  Run {i+1}: {elapsed*1000:.1f}ms")
                else:
                    print(f"  Run {i+1}: FAILED")
            
            if times:
                avg = statistics.mean(times)
                std = statistics.stdev(times) if len(times) > 1 else 0
                min_t = min(times)
                max_t = max(times)
                print(f"  ✅ Avg: {avg*1000:.1f}ms, Std: {std*1000:.1f}ms, Min: {min_t*1000:.1f}ms, Max: {max_t*1000:.1f}ms")
                
                # Store results
                if qid not in all_results:
                    all_results[qid] = {}
                all_results[qid][year] = {
                    "times": times,
                    "avg": avg,
                    "std": std,
                    "min": min_t,
                    "max": max_t,
                }
                
                # Store sample output
                if qid not in sample_outputs:
                    sample_outputs[qid] = {}
                sample_outputs[qid][year] = result_sample
    
    # Print summary
    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY")
    print("=" * 70)
    
    print("\n### Average Execution Time (ms)\n")
    print(f"| Query | Description | 2021 | 2022 | 2023 | Overall Avg |")
    print(f"|-------|-------------|------|------|------|-------------|")
    
    query_names = {
        "Q1": "2-hop connecting flights",
        "Q2": "Delayed connections",
        "Q3": "Hub airport analysis",
        "Q4": "3-hop aircraft journey",
    }
    
    for qid in ["Q1", "Q2", "Q3", "Q4"]:
        if qid in all_results:
            times_2021 = all_results[qid].get(2021, {}).get("avg", 0) * 1000
            times_2022 = all_results[qid].get(2022, {}).get("avg", 0) * 1000
            times_2023 = all_results[qid].get(2023, {}).get("avg", 0) * 1000
            overall = (times_2021 + times_2022 + times_2023) / 3
            print(f"| {qid} | {query_names[qid]} | {times_2021:.0f} | {times_2022:.0f} | {times_2023:.0f} | {overall:.0f} |")
    
    # Print sample results
    print("\n### Sample Results\n")
    for qid in ["Q1", "Q2", "Q3", "Q4"]:
        if qid in sample_outputs and 2022 in sample_outputs[qid]:
            print(f"**{qid}** (2022 sample):")
            sample = sample_outputs[qid][2022]
            if sample:
                for row in sample[:3]:
                    print(f"  {row}")
            print()
    
    # Save detailed results to JSON
    output = {
        "metadata": {
            "date": datetime.now().isoformat(),
            "iterations": ITERATIONS,
            "years": YEARS,
        },
        "results": all_results,
        "sample_outputs": sample_outputs,
    }
    
    with open("benchmarks/results/ontime_benchmark_detailed.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    print("\n📁 Detailed results saved to: benchmarks/results/ontime_benchmark_detailed.json")
    
    return all_results


if __name__ == "__main__":
    run_benchmark()
