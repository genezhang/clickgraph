#!/usr/bin/env python3
"""
Test all LDBC queries systematically and update status matrix
Usage: ./test_all_ldbc_queries.py
"""

import json
import re
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple

# Configuration
CLICKGRAPH_URL = "http://localhost:8080"
QUERY_BASE = Path("benchmarks/ldbc_snb/queries/official")
SCHEMA_NAME = "ldbc_snb"

# Colors
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    NC = '\033[0m'

def extract_parameters(query_content: str) -> Optional[Dict]:
    """Extract parameters from :params comment"""
    # Look for :params { ... } pattern
    match = re.search(r':params\s*\{([^}]+)\}', query_content)
    if not match:
        return None
    
    param_str = match.group(1).strip()
    # Parse simple key: value format
    params = {}
    for item in param_str.split(','):
        if ':' in item:
            key, value = item.split(':', 1)
            key = key.strip()
            value = value.strip()
            # Try to parse as number
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    # Keep as string, remove quotes if present
                    value = value.strip('"\'')
            params[key] = value
    
    return params if params else None

def clean_query(query_content: str) -> str:
    """Remove comments from query"""
    lines = []
    in_block_comment = False
    for line in query_content.split('\n'):
        # Skip line comments
        if line.strip().startswith('//') or line.strip().startswith(':params'):
            continue
        # Handle block comments
        if '/*' in line:
            in_block_comment = True
        if '*/' in line:
            in_block_comment = False
            continue
        if in_block_comment:
            continue
        lines.append(line)
    return '\n'.join(lines).strip()

def test_query(query_file: Path) -> Tuple[str, Optional[str], Optional[int]]:
    """
    Test a single query
    Returns: (status, error_message, row_count)
    """
    query_name = query_file.stem
    category = query_file.parent.name
    
    print(f"{Colors.YELLOW}Testing: {category}/{query_name}{Colors.NC}")
    
    # Read query
    query_content = query_file.read_text()
    
    # Extract parameters and clean query
    params = extract_parameters(query_content)
    query_clean = clean_query(query_content)
    
    # Build request
    request_data = {
        "query": query_clean,
        "schema_name": SCHEMA_NAME
    }
    if params:
        request_data["parameters"] = params
    
    # Execute query
    try:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json=request_data,
            timeout=30
        )
        
        # Get response body before raising for status
        try:
            result = response.json()
        except:
            result = {"error": response.text}
        
        if response.status_code >= 400:
            error = result.get('error', f"HTTP {response.status_code}")
            print(f"{Colors.RED}  ✗ FAIL: {error[:100]}{Colors.NC}")
            return "FAIL", error, None
            
        if 'error' in result:
            error = result['error']
            print(f"{Colors.RED}  ✗ FAIL: {error[:100]}{Colors.NC}")
            return "FAIL", error, None
        elif 'results' in result:
            rows = len(result['results'])
            print(f"{Colors.GREEN}  ✓ PASS ({rows} rows){Colors.NC}")
            return "PASS", None, rows
        else:
            error = "Invalid response format"
            print(f"{Colors.RED}  ✗ ERROR: {error}{Colors.NC}")
            return "ERROR", error, None
            
    except requests.exceptions.Timeout:
        error = "Query timeout (30s)"
        print(f"{Colors.RED}  ✗ TIMEOUT: {error}{Colors.NC}")
        return "TIMEOUT", error, None
    except Exception as e:
        error = str(e)
        print(f"{Colors.RED}  ✗ ERROR: {error}{Colors.NC}")
        return "ERROR", error, None

def main():
    results = []
    stats = {"total": 0, "pass": 0, "fail": 0, "error": 0, "timeout": 0}
    
    # Test Interactive Short queries
    print("\n" + "="*50)
    print("Testing Interactive Short (IS) Queries")
    print("="*50)
    for query_file in sorted(QUERY_BASE.glob("interactive/short-*.cypher")):
        status, error, rows = test_query(query_file)
        results.append({
            "category": "interactive-short",
            "query": query_file.stem,
            "status": status,
            "error": error,
            "rows": rows
        })
        stats["total"] += 1
        stats[status.lower()] = stats.get(status.lower(), 0) + 1
    
    # Test Interactive Complex queries
    print("\n" + "="*50)
    print("Testing Interactive Complex (IC) Queries")
    print("="*50)
    for query_file in sorted(QUERY_BASE.glob("interactive/complex-*.cypher")):
        status, error, rows = test_query(query_file)
        results.append({
            "category": "interactive-complex",
            "query": query_file.stem,
            "status": status,
            "error": error,
            "rows": rows
        })
        stats["total"] += 1
        stats[status.lower()] = stats.get(status.lower(), 0) + 1
    
    # Test BI queries
    print("\n" + "="*50)
    print("Testing Business Intelligence (BI) Queries")
    print("="*50)
    for query_file in sorted(QUERY_BASE.glob("bi/*.cypher")):
        status, error, rows = test_query(query_file)
        results.append({
            "category": "bi",
            "query": query_file.stem,
            "status": status,
            "error": error,
            "rows": rows
        })
        stats["total"] += 1
        stats[status.lower()] = stats.get(status.lower(), 0) + 1
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"benchmarks/ldbc_snb/test_results_{timestamp}.json"
    
    output = {
        "timestamp": datetime.now().isoformat(),
        "results": results,
        "summary": stats
    }
    
    with open(results_file, 'w') as f:
        json.dump(output, f, indent=2)
    
    # Summary
    pass_rate = (stats["pass"] / stats["total"] * 100) if stats["total"] > 0 else 0
    
    print("\n" + "="*50)
    print("Summary")
    print("="*50)
    print(f"Total:     {stats['total']}")
    print(f"{Colors.GREEN}Pass:      {stats['pass']}{Colors.NC}")
    print(f"{Colors.RED}Fail:      {stats['fail']}{Colors.NC}")
    print(f"Error:     {stats.get('error', 0)}")
    print(f"Timeout:   {stats.get('timeout', 0)}")
    print(f"Pass Rate: {pass_rate:.1f}%")
    print(f"\nResults saved to: {results_file}")
    print(f"\nView failures:")
    print(f"  cat {results_file} | jq '.results[] | select(.status==\"FAIL\")'")

if __name__ == "__main__":
    main()
