#!/usr/bin/env python3
"""
Check SQL generation for LDBC queries
"""
import requests
import json
from pathlib import Path

BASE_URL = "http://localhost:8080/query"
QUERY_DIR = Path("/home/gz/clickgraph/benchmarks/ldbc_snb/queries/official")

# Queries that were previously failing
TEST_QUERIES = {
    "IS7": ("interactive/short-7.cypher", {"messageId": 1099511816755}),
    "IC1": ("interactive/complex-1.cypher", {
        "personId": 14,
        "firstName": "Hossein"
    }),
    "IC3": ("interactive/complex-3.cypher", {
        "personId": 14,
        "countryXName": "India",
        "countryYName": "China",
        "startDate": 1325404800000,  # 2012-01-01
        "endDate": 1335830400000     # 2012-05-01
    }),
    "BI8": ("bi/bi-8.cypher", {
        "tag": "Mustafa_Kemal_Atatürk"
    }),
}

def load_query(file_path: str) -> str:
    """Load query from file"""
    full_path = QUERY_DIR / file_path
    with open(full_path, 'r') as f:
        content = f.read()
    
    # Remove comments
    lines = []
    for line in content.split('\n'):
        if '//' in line:
            line = line[:line.index('//')]
        if line.strip():
            lines.append(line)
    
    return '\n'.join(lines).strip()

def check_sql(query_name: str, file_path: str, params: dict):
    """Check SQL generation for a query"""
    print(f"\n{'='*80}")
    print(f"Testing {query_name}: {file_path}")
    print(f"{'='*80}")
    
    query = load_query(file_path)
    print(f"\nCypher Query (first 200 chars):\n{query[:200]}...")
    
    response = requests.post(
        BASE_URL,
        json={
            'query': query,
            'parameters': params,
            'schema_name': 'ldbc_snb',
            'sql_only': True
        },
        timeout=10
    )
    
    if response.status_code != 200:
        print(f"\n❌ ERROR: {response.status_code}")
        print(response.text[:500])
        return False
    
    result = response.json()
    sql = result.get('generated_sql', '')
    
    print(f"\nGenerated SQL (first 500 chars):")
    print(sql[:500])
    print("...")
    print(f"\nSQL Length: {len(sql)} characters")
    
    # Check for common issues
    issues = []
    if '_cte_1' in sql and '_cte AS' not in sql.replace('_cte_1', '_cte'):
        issues.append("⚠️  CTE reference mismatch (e.g., _cte_1 referenced but not defined)")
    if 'ON message.' in sql and 'JOIN with_' in sql:
        issues.append("⚠️  Suspicious JOIN condition (joining on CTE column)")
    if sql.count('WHERE') > 3:
        issues.append("ℹ️  Multiple WHERE clauses (check for duplication)")
    
    if issues:
        print("\nPotential Issues:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("\n✅ No obvious issues detected")
    
    return True

if __name__ == "__main__":
    print("Checking LDBC SQL Generation")
    print(f"Server: {BASE_URL}")
    
    for query_name, (file_path, params) in TEST_QUERIES.items():
        try:
            check_sql(query_name, file_path, params)
        except Exception as e:
            print(f"\n❌ Exception for {query_name}: {e}")
    
    print(f"\n{'='*80}")
    print("Done!")
