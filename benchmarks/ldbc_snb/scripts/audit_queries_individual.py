#!/usr/bin/env python3
"""
Individual Query Audit Tool
Tests each LDBC query one by one with detailed error analysis
"""

import requests
import json
import re
from pathlib import Path
from datetime import datetime

BASE_URL = "http://localhost:8080/query"
QUERY_DIR = Path("/home/gz/clickgraph/benchmarks/ldbc_snb/queries/official")

def iso_to_epoch(iso_date: str) -> int:
    """Convert ISO8601 datetime string to epoch milliseconds"""
    dt = datetime.strptime(iso_date.rstrip('Z'), "%Y-%m-%dT%H:%M:%S.%f")
    return int(dt.timestamp() * 1000)

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

QUERIES = {
    "IS1": "interactive/short-1.cypher",
    "IS2": "interactive/short-2.cypher",
    "IS3": "interactive/short-3.cypher",
    "IS4": "interactive/short-4.cypher",
    "IS5": "interactive/short-5.cypher",
    "IS6": "interactive/short-6.cypher",
    "IS7": "interactive/short-7.cypher",
    "IC1": "interactive/complex-1.cypher",
    "IC2": "interactive/complex-2.cypher",
    "IC3": "interactive/complex-3.cypher",
    "IC4": "interactive/complex-4.cypher",
    "IC5": "interactive/complex-5.cypher",
    "IC6": "interactive/complex-6.cypher",
    "IC7": "interactive/complex-7.cypher",
    "IC8": "interactive/complex-8.cypher",
    "IC9": "interactive/complex-9.cypher",
    "IC11": "interactive/complex-11.cypher",
    "IC12": "interactive/complex-12.cypher",
    "IC13": "interactive/complex-13.cypher",
    "BI1": "bi/bi-1.cypher",
    "BI2": "bi/bi-2.cypher",
    "BI3": "bi/bi-3.cypher",
    "BI5": "bi/bi-5.cypher",
    "BI6": "bi/bi-6.cypher",
    "BI7": "bi/bi-7.cypher",
    "BI8": "bi/bi-8.cypher",
    "BI9": "bi/bi-9.cypher",
    "BI11": "bi/bi-11.cypher",
    "BI12": "bi/bi-12.cypher",
    "BI13": "bi/bi-13.cypher",
    "BI14": "bi/bi-14.cypher",
    "BI17": "bi/bi-17.cypher",
    "BI18": "bi/bi-18.cypher",
}

def load_query(file_path: str) -> str:
    """Load and clean query from file"""
    full_path = QUERY_DIR / file_path
    with open(full_path, 'r') as f:
        content = f.read()
    
    # Remove multi-line comments /* ... */
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    
    # Remove single-line comments //
    lines = []
    for line in content.split('\n'):
        if '//' in line:
            line = line[:line.index('//')]
        lines.append(line)
    
    query = '\n'.join(lines).strip()
    return query

def substitute_params(query: str, params: dict) -> str:
    """Replace $param placeholders with actual values"""
    result = query
    # Sort parameters by length (longest first) to avoid overlapping replacements
    # e.g., $country1 should be replaced before $country
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

def categorize_error(error_msg: str) -> tuple:
    """Categorize error type and extract key information"""
    error_lower = error_msg.lower()
    
    if "parsing" in error_lower or "opencypher" in error_lower:
        return "PARSING", extract_parse_error(error_msg)
    elif "no relationship schema found" in error_lower:
        return "SCHEMA_MISSING_REL", extract_missing_rel(error_msg)
    elif "property" in error_lower and "not found" in error_lower:
        return "SCHEMA_MISSING_PROP", extract_missing_prop(error_msg)
    elif "invalid relation query" in error_lower:
        return "ANALYZER_RELATION", extract_relation_error(error_msg)
    elif "clickhouse error" in error_lower:
        return "SQL_GENERATION", extract_sql_error(error_msg)
    elif "with clause validation" in error_lower:
        return "WITH_CLAUSE", error_msg
    elif "no select items found" in error_lower:
        return "RENDER_PLAN", "No select items found"
    else:
        return "OTHER", error_msg[:200]

def extract_parse_error(msg):
    match = re.search(r'errors: \["([^"]+)"', msg)
    return match.group(1) if match else msg[:100]

def extract_missing_rel(msg):
    match = re.search(r'from ([A-Za-z]+) to ([A-Za-z]+)', msg)
    return match.group(0) if match else msg[:100]

def extract_missing_prop(msg):
    match = re.search(r"Property '([^']+)' not found on (node|edge) '([^']+)'", msg)
    if match:
        return f"{match.group(1)} on {match.group(3)}"
    return msg[:100]

def extract_relation_error(msg):
    match = re.search(r'Invalid relation query - (\w+)', msg)
    return match.group(1) if match else msg[:100]

def extract_sql_error(msg):
    if "Multiple table expression" in msg:
        return "Multiple table expression (duplicate alias)"
    elif "Unknown table expression" in msg:
        return "Unknown table expression identifier"
    elif "Unknown expression" in msg:
        match = re.search(r"Unknown expression.*?`([^`]+)`", msg)
        return f"Unknown: {match.group(1)}" if match else "Unknown expression"
    return msg[:100]

def test_single_query(query_id: str, query_path: str):
    """Test a single query and return detailed results"""
    try:
        # Load query
        cypher = load_query(query_path)
        cypher_with_params = substitute_params(cypher, DEFAULT_PARAMS)
        
        # Test execution
        response = requests.post(BASE_URL, json={"query": cypher_with_params}, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            rows = len(result.get('results', []))
            return {
                'status': 'SUCCESS',
                'rows': rows,
                'query_preview': cypher_with_params[:150]
            }
        else:
            error_msg = response.text
            error_type, error_detail = categorize_error(error_msg)
            return {
                'status': 'FAILED',
                'error_type': error_type,
                'error_detail': error_detail,
                'query_preview': cypher_with_params[:150]
            }
    except Exception as e:
        return {
            'status': 'ERROR',
            'error_type': 'EXCEPTION',
            'error_detail': str(e)[:200],
            'query_preview': ''
        }

def main():
    print("=" * 80)
    print("LDBC QUERY INDIVIDUAL AUDIT")
    print("=" * 80)
    print()
    
    results = {}
    error_summary = {}
    
    # Test each query
    for query_id in sorted(QUERIES.keys()):
        query_path = QUERIES[query_id]
        print(f"\n{'='*80}")
        print(f"Testing {query_id}: {query_path}")
        print(f"{'='*80}")
        
        result = test_single_query(query_id, query_path)
        results[query_id] = result
        
        if result['status'] == 'SUCCESS':
            print(f"✅ SUCCESS - {result['rows']} rows")
            print(f"   Query: {result['query_preview']}...")
        else:
            print(f"❌ FAILED - {result['error_type']}")
            print(f"   Detail: {result['error_detail']}")
            print(f"   Query: {result['query_preview']}...")
            
            # Track error types
            error_type = result['error_type']
            if error_type not in error_summary:
                error_summary[error_type] = []
            error_summary[error_type].append(query_id)
        
        # Pause for user input
        response = input("\nPress Enter to continue, 's' to skip remaining, 'q' to quit: ").strip().lower()
        if response == 'q':
            print("\nAudit stopped by user.")
            break
        elif response == 's':
            # Test remaining quickly without pause
            for remaining_id in sorted(QUERIES.keys()):
                if remaining_id > query_id:
                    result = test_single_query(remaining_id, QUERIES[remaining_id])
                    results[remaining_id] = result
                    if result['status'] != 'SUCCESS':
                        error_type = result['error_type']
                        if error_type not in error_summary:
                            error_summary[error_type] = []
                        error_summary[error_type].append(remaining_id)
            break
    
    # Final summary
    print("\n" + "=" * 80)
    print("AUDIT SUMMARY")
    print("=" * 80)
    
    success_count = sum(1 for r in results.values() if r['status'] == 'SUCCESS')
    total_count = len(results)
    
    print(f"\nTotal Queries Tested: {total_count}")
    print(f"Successful: {success_count}/{total_count} ({100*success_count/total_count:.1f}%)")
    print(f"Failed: {total_count - success_count}")
    
    print("\n" + "-" * 80)
    print("ERROR BREAKDOWN")
    print("-" * 80)
    
    for error_type, queries in sorted(error_summary.items()):
        print(f"\n{error_type}: {len(queries)} queries")
        for q in queries:
            detail = results[q].get('error_detail', '')[:80]
            print(f"  - {q}: {detail}")
    
    # Save detailed results
    with open('/tmp/ldbc_query_audit.json', 'w') as f:
        json.dump(results, f, indent=2)
    print("\n✓ Detailed results saved to /tmp/ldbc_query_audit.json")

if __name__ == "__main__":
    main()
