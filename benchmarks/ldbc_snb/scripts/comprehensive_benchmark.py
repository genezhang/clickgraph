#!/usr/bin/env python3
"""
LDBC SNB Comprehensive Benchmark - All 32 Actionable Queries
Tests: 7 IS + 11 IC + 12 BI + 2 BI (restructured) = 32 total

This script tests ALL queries that ClickGraph should be able to execute
based on QUERY_FEATURE_ANALYSIS.md (100% actionable coverage)

Excludes:
- IC10, IC14: External libraries required (Neo4j GDS/APOC)
- BI10, BI15, BI19, BI20: Neo4j GDS required
"""

import requests
import json
import time
import os
from typing import Dict, List, Tuple
from datetime import datetime
from pathlib import Path

BASE_URL = "http://localhost:8080/query"
QUERY_DIR = Path("/home/gz/clickgraph/benchmarks/ldbc_snb/queries/official")

# Convert ISO8601 dates to epoch milliseconds for ClickHouse
def iso_to_epoch(iso_date: str) -> int:
    """Convert ISO8601 datetime string to epoch milliseconds"""
    # Parse format: 2012-01-01T00:00:00.000
    dt = datetime.strptime(iso_date.rstrip('Z'), "%Y-%m-%dT%H:%M:%S.%f")
    return int(dt.timestamp() * 1000)

# Test parameters with EPOCH MILLISECONDS (not ISO strings!)
DEFAULT_PARAMS = {
    "personId": 933,
    "person1Id": 933,
    "person2Id": 10995116278874,
    "messageId": 1099511816755,
    "tag": "Mustafa_Kemal_Atatürk",
    "tagName": "Mustafa_Kemal_Atatürk",
    "firstName": "Yang",
    "countryName": "India",
    "countryXName": "India",
    "countryYName": "China",
    "country1": "India",
    "country2": "China",
    "country": "India",
    "tagClass": "MusicalArtist",
    "tagClassName": "MusicalArtist",
    "minPathDistance": 2,
    "maxPathDistance": 4,
    "limit": 20,
    # Dates as epoch milliseconds
    "date": iso_to_epoch("2012-01-01T00:00:00.000"),
    "date1": iso_to_epoch("2011-11-01T00:00:00.000"),
    "date2": iso_to_epoch("2011-12-01T00:00:00.000"),
    "date3": iso_to_epoch("2012-01-01T00:00:00.000"),
    "maxDate": iso_to_epoch("2012-12-31T23:59:59.999"),
    "startDate": iso_to_epoch("2012-01-01T00:00:00.000"),
    "endDate": iso_to_epoch("2012-03-01T00:00:00.000"),
    "workFromYear": 2010,
    "minDate": iso_to_epoch("2012-01-01T00:00:00.000"),
    "delta": 24,  # hours for BI-17
}

# All 32 actionable queries
QUERY_FILES = {
    # === Interactive Short (7 queries - ALL should work) ===
    "IS1": "interactive/short-1.cypher",
    "IS2": "interactive/short-2.cypher",
    "IS3": "interactive/short-3.cypher",
    "IS4": "interactive/short-4.cypher",
    "IS5": "interactive/short-5.cypher",
    "IS6": "interactive/short-6.cypher",
    "IS7": "interactive/short-7.cypher",
    
    # === Interactive Complex (11 working queries) ===
    "IC1": "interactive/complex-1.cypher",
    "IC2": "interactive/complex-2.cypher",
    "IC3": "interactive/complex-3.cypher",
    "IC4": "interactive/complex-4.cypher",
    "IC5": "interactive/complex-5.cypher",
    "IC6": "interactive/complex-6.cypher",
    "IC7": "interactive/complex-7.cypher",
    "IC8": "interactive/complex-8.cypher",
    "IC9": "interactive/complex-9.cypher",
    # IC10 - BLOCKED: pattern comprehension requires parser extension
    "IC11": "interactive/complex-11.cypher",
    "IC12": "interactive/complex-12.cypher",
    "IC13": "interactive/complex-13.cypher",
    # IC14 - BLOCKED: requires Neo4j GDS
    
    # === Business Intelligence (12 working directly + 2 restructured) ===
    "BI1": "bi/bi-1.cypher",
    "BI2": "bi/bi-2.cypher",
    "BI3": "bi/bi-3.cypher",
    # BI4 - Requires restructuring (CALL subquery)
    "BI5": "bi/bi-5.cypher",
    "BI6": "bi/bi-6.cypher",
    "BI7": "bi/bi-7.cypher",
    "BI8": "bi/bi-8.cypher",
    "BI9": "bi/bi-9.cypher",
    # BI10 - BLOCKED: requires Neo4j APOC
    "BI11": "bi/bi-11.cypher",
    "BI12": "bi/bi-12.cypher",
    "BI13": "bi/bi-13.cypher",
    "BI14": "bi/bi-14.cypher",
    # BI15, BI19, BI20 - BLOCKED: require Neo4j GDS
    # BI16 - Requires restructuring (CALL subquery)
    "BI17": "bi/bi-17.cypher",
    "BI18": "bi/bi-18.cypher",
}

def load_query(file_path: str) -> str:
    """Load query from file and clean it"""
    full_path = QUERY_DIR / file_path
    with open(full_path, 'r') as f:
        content = f.read()
    
    # Remove multi-line comments /* ... */
    import re
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    
    # Remove single-line comments //
    lines = []
    for line in content.split('\n'):
        if '//' in line:
            line = line[:line.index('//')]
        lines.append(line)
    
    query = '\n'.join(lines).strip()
    return query

def substitute_params(query: str, params: Dict) -> str:
    """Replace $param placeholders with actual values"""
    result = query
    # Sort parameters by length (longest first) to avoid overlapping replacements
    # e.g., $country1 should be replaced before $country
    sorted_params = sorted(params.items(), key=lambda x: len(x[0]), reverse=True)
    for param, value in sorted_params:
        placeholder = f"${param}"
        if isinstance(value, str):
            # Escape single quotes in string values
            escaped_value = value.replace("'", "\\'")
            result = result.replace(placeholder, f"'{escaped_value}'")
        elif isinstance(value, bool):
            result = result.replace(placeholder, str(value).lower())
        elif isinstance(value, int):
            result = result.replace(placeholder, str(value))
        elif isinstance(value, float):
            result = result.replace(placeholder, str(value))
        else:
            result = result.replace(placeholder, str(value))
    return result

def test_query(query_id: str, cypher: str, sql_only: bool = False) -> Dict:
    """Test a single query"""
    try:
        start = time.time()
        response = requests.post(
            BASE_URL,
            json={"query": cypher, "sql_only": sql_only},
            timeout=60
        )
        elapsed = (time.time() - start) * 1000
        
        if response.status_code != 200:
            return {
                "query_id": query_id,
                "status": "error",
                "error": f"HTTP {response.status_code}: {response.text[:300]}",
                "time_ms": elapsed
            }
        
        result = response.json()
        
        if sql_only:
            sql = result.get('generated_sql', '')
            return {
                "query_id": query_id,
                "status": "sql_ok" if sql else "sql_error",
                "sql": sql[:500],  # Truncate SQL for display
                "time_ms": elapsed
            }
        else:
            if 'error' in result:
                return {
                    "query_id": query_id,
                    "status": "execution_error",
                    "error": result['error'][:300],
                    "time_ms": elapsed
                }
            
            rows = result.get('results', [])
            return {
                "query_id": query_id,
                "status": "success",
                "row_count": len(rows),
                "time_ms": elapsed
            }
            
    except Exception as e:
        return {
            "query_id": query_id,
            "status": "exception",
            "error": str(e)[:300]
        }

def main():
    print("=" * 80)
    print("LDBC SNB COMPREHENSIVE BENCHMARK - ALL 32 ACTIONABLE QUERIES")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total Queries: {len(QUERY_FILES)}")
    print("=" * 80)
    print()
    
    # Load all queries
    queries = {}
    print("Loading queries...")
    for query_id, file_path in QUERY_FILES.items():
        try:
            queries[query_id] = load_query(file_path)
            print(f"  ✅ {query_id} loaded")
        except Exception as e:
            print(f"  ❌ {query_id} failed to load: {e}")
    
    print(f"\nLoaded: {len(queries)}/{len(QUERY_FILES)} queries")
    print()
    
    # Phase 1: SQL Generation Test
    print("=" * 80)
    print("PHASE 1: SQL GENERATION VALIDATION")
    print("-" * 80)
    
    sql_results = []
    for query_id in sorted(queries.keys()):
        print(f"Testing {query_id}...", end=' ', flush=True)
        cypher = substitute_params(queries[query_id], DEFAULT_PARAMS)
        result = test_query(query_id, cypher, sql_only=True)
        sql_results.append(result)
        
        if result['status'] == 'sql_ok':
            print(f"✅ OK ({result['time_ms']:.1f}ms)")
        else:
            print(f"❌ FAIL")
            if 'error' in result:
                print(f"   Error: {result['error'][:150]}")
    
    sql_ok = sum(1 for r in sql_results if r['status'] == 'sql_ok')
    print(f"\n{'='*80}")
    print(f"SQL Generation: {sql_ok}/{len(queries)} passed ({sql_ok*100//len(queries)}%)")
    print(f"{'='*80}\n")
    
    # Phase 2: Execution Test
    print("=" * 80)
    print("PHASE 2: QUERY EXECUTION TEST")
    print("-" * 80)
    
    exec_results = []
    success_times = []
    
    for query_id in sorted(queries.keys()):
        print(f"Executing {query_id}...", end=' ', flush=True)
        cypher = substitute_params(queries[query_id], DEFAULT_PARAMS)
        result = test_query(query_id, cypher, sql_only=False)
        exec_results.append(result)
        
        if result['status'] == 'success':
            print(f"✅ OK ({result['time_ms']:.1f}ms, {result['row_count']} rows)")
            success_times.append(result['time_ms'])
        elif result['status'] == 'execution_error':
            print(f"❌ EXEC ERROR")
            print(f"   {result['error'][:150]}")
        else:
            print(f"❌ {result['status'].upper()}")
            if 'error' in result:
                print(f"   {result['error'][:150]}")
    
    success = sum(1 for r in exec_results if r['status'] == 'success')
    
    print(f"\n{'='*80}")
    print(f"Execution: {success}/{len(queries)} passed ({success*100//len(queries)}%)")
    print(f"{'='*80}\n")
    
    # Summary
    print("=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"Queries Loaded:     {len(queries)}/{len(QUERY_FILES)}")
    print(f"SQL Generation:     {sql_ok}/{len(queries)} ({sql_ok*100//len(queries)}%)")
    print(f"Query Execution:    {success}/{len(queries)} ({success*100//len(queries)}%)")
    
    if success > 0:
        avg_time = sum(success_times) / len(success_times)
        median_time = sorted(success_times)[len(success_times)//2]
        p95_idx = int(len(success_times) * 0.95)
        p95_time = sorted(success_times)[p95_idx] if p95_idx < len(success_times) else success_times[-1]
        
        print(f"\nPerformance Metrics (Successful Queries):")
        print(f"  Total Query Time:   {sum(success_times):.1f}ms")
        print(f"  Average Latency:    {avg_time:.2f}ms")
        print(f"  Median Latency:     {median_time:.2f}ms")
        print(f"  P95 Latency:        {p95_time:.2f}ms")
        print(f"  Estimated QPS:      {1000/avg_time:.2f}")
    
    # Breakdown by category
    print(f"\nBreakdown by Category:")
    for category in ["IS", "IC", "BI"]:
        cat_results = [r for r in exec_results if r['query_id'].startswith(category)]
        cat_success = sum(1 for r in cat_results if r['status'] == 'success')
        if cat_results:
            print(f"  {category}: {cat_success}/{len(cat_results)} ({cat_success*100//len(cat_results)}%)")
    
    # Failed queries
    failed = [r for r in exec_results if r['status'] != 'success']
    if failed:
        print(f"\nFailed Queries ({len(failed)}):")
        for r in failed:
            print(f"  {r['query_id']}: {r['status']}")
            if 'error' in r:
                print(f"    → {r['error'][:100]}")
    
    print("=" * 80)

if __name__ == "__main__":
    main()
