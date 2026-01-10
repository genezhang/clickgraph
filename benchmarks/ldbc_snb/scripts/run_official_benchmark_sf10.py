#!/usr/bin/env python3
"""
Run official LDBC SNB benchmark on SF10 dataset.
Only runs queries that pass SQL generation audit.

Usage:
    python run_official_benchmark_sf10.py
"""

import requests
import json
import time
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Configuration
CLICKGRAPH_URL = "http://localhost:8080"
SCHEMA_NAME = "ldbc_snb"
BASE_DIR = Path(__file__).parent.parent
QUERIES_DIR = BASE_DIR / "queries" / "official"
RESULTS_DIR = BASE_DIR / "results"

# Sample parameters for testing (SF10 compatible - use actual IDs from dataset)
PARAMS = {
    # Person IDs that exist in SF10
    "personId": 14,  # Hossein Forouhar
    "person1Id": 16,
    "person2Id": 27,
    
    # Other IDs (should be validated against SF10)
    "messageId": 100000,
    "postId": 100000,
    "commentId": 200000,
    "forumId": 10000,
    
    # String parameters
    "tag": "Arnold_Schwarzenegger",
    "tagName": "Arnold_Schwarzenegger",
    "tagClass": "MusicalArtist",
    "tagClassName": "MusicalArtist",
    "country": "India",
    "countryName": "India",
    "countryXName": "India",
    "countryYName": "China",
    "firstName": "Chau",
    "city": "Beijing",
    
    # Numeric parameters
    "limit": 20,
    "maxKnowsLimit": 20,
    "lengthThreshold": 100,
    "workFromYear": 2010,
    
    # Date parameters (Unix timestamps in milliseconds)
    "datetime": 1322697600000,  # 2011-12-01T00:00:00.000
    "date": 1322697600000,
    "startDate": 1293840000000,  # 2011-01-01
    "endDate": 1325376000000,    # 2012-01-01
    "minDate": 1293840000000,
    "maxDate": 1356998400000,    # 2013-01-01
}

# Queries that passed SQL generation audit (17 total)
PASSING_QUERIES = {
    # Business Intelligence (5)
    "BI-3": "bi/bi-3.cypher",
    "BI-6": "bi/bi-6.cypher",
    "BI-7": "bi/bi-7.cypher",
    "BI-9": "bi/bi-9.cypher",
    "BI-18": "bi/bi-18.cypher",
    
    # Interactive Complex (5)
    "IC-2": "interactive/complex-2.cypher",
    "IC-8": "interactive/complex-8.cypher",
    "IC-9": "interactive/complex-9.cypher",
    "IC-11": "interactive/complex-11.cypher",
    "IC-12": "interactive/complex-12.cypher",
    
    # Interactive Short (7)
    "IS-1": "interactive/short-1.cypher",
    "IS-2": "interactive/short-2.cypher",
    "IS-3": "interactive/short-3.cypher",
    "IS-4": "interactive/short-4.cypher",
    "IS-5": "interactive/short-5.cypher",
    "IS-6": "interactive/short-6.cypher",
    "IS-7": "interactive/short-7.cypher",
}


def load_query(file_path: str) -> str:
    """Load and clean query from file."""
    full_path = QUERIES_DIR / file_path
    with open(full_path, 'r') as f:
        content = f.read()
    
    # Remove comments
    import re
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    lines = []
    for line in content.split('\n'):
        if '//' in line:
            line = line[:line.index('//')]
        if line.strip():
            lines.append(line)
    
    return '\n'.join(lines).strip()


def substitute_params(query: str, params: Dict) -> str:
    """Replace $param placeholders with actual values."""
    result = query
    # Sort by length to avoid overlapping replacements
    sorted_params = sorted(params.items(), key=lambda x: len(x[0]), reverse=True)
    
    for param, value in sorted_params:
        placeholder = f"${param}"
        if isinstance(value, str):
            escaped_value = value.replace("'", "\\'")
            result = result.replace(placeholder, f"'{escaped_value}'")
        elif isinstance(value, bool):
            result = result.replace(placeholder, str(value).lower())
        else:
            result = result.replace(placeholder, str(value))
    
    return result


def run_query(query_name: str, query: str) -> Tuple[bool, Optional[int], Optional[float], str]:
    """Execute a query and return (success, row_count, time_sec, message)."""
    try:
        start = time.time()
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={
                "query": query,
                "schema_name": SCHEMA_NAME,
                "parameters": PARAMS
            },
            timeout=300  # 5 min timeout
        )
        elapsed = time.time() - start
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            return True, len(results), elapsed, "SUCCESS"
        else:
            try:
                error = response.json().get("error", response.text)
            except:
                error = response.text
            return False, None, elapsed, f"HTTP {response.status_code}: {error[:200]}"
            
    except Exception as e:
        return False, None, None, f"Exception: {str(e)[:200]}"


def main():
    print("="*80)
    print("  LDBC SNB Official Benchmark - SF10")
    print("="*80)
    print(f"  Server: {CLICKGRAPH_URL}")
    print(f"  Schema: {SCHEMA_NAME}")
    print(f"  Queries: {len(PASSING_QUERIES)}")
    print("="*80)
    print()
    
    results = []
    passed = 0
    failed = 0
    total_time = 0.0
    
    for query_name, file_path in sorted(PASSING_QUERIES.items()):
        print(f"Running {query_name}...", end=" ", flush=True)
        
        # Load and prepare query
        raw_query = load_query(file_path)
        query = substitute_params(raw_query, PARAMS)
        
        # Execute
        success, rows, time_sec, message = run_query(query_name, query)
        
        if success:
            passed += 1
            if time_sec:
                total_time += time_sec
            status_icon = "✅"
            print(f"{status_icon} {rows} rows in {time_sec:.3f}s")
        else:
            failed += 1
            status_icon = "❌"
            print(f"{status_icon} {message}")
        
        results.append({
            "query": query_name,
            "status": "PASS" if success else "FAIL",
            "rows": rows,
            "time_sec": round(time_sec, 3) if time_sec else None,
            "message": message
        })
    
    # Summary
    print()
    print("="*80)
    print("  SUMMARY")
    print("="*80)
    print(f"  Total: {len(PASSING_QUERIES)}")
    print(f"  ✅ Passed: {passed} ({100*passed//len(PASSING_QUERIES)}%)")
    print(f"  ❌ Failed: {failed}")
    print(f"  Total Time: {total_time:.2f}s")
    print(f"  Avg Time: {total_time/passed:.3f}s" if passed > 0 else "  Avg Time: N/A")
    print("="*80)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = RESULTS_DIR / f"official_benchmark_sf10_{timestamp}.json"
    
    output_data = {
        "benchmark": "LDBC SNB Official Queries",
        "scale_factor": "sf10",
        "timestamp": datetime.now().isoformat(),
        "clickgraph_url": CLICKGRAPH_URL,
        "schema_name": SCHEMA_NAME,
        "total_queries": len(PASSING_QUERIES),
        "passed": passed,
        "failed": failed,
        "pass_rate": f"{100*passed//len(PASSING_QUERIES)}%",
        "total_time_sec": round(total_time, 2),
        "avg_time_sec": round(total_time/passed, 3) if passed > 0 else None,
        "results": results
    }
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n📄 Results saved to: {output_file}")
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
