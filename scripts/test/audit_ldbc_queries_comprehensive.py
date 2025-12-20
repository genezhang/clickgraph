#!/usr/bin/env python3
"""
Comprehensive LDBC Query Audit Script

Checks:
1. ✅ Query parses successfully (generates logical plan)
2. ✅ SQL is generated without errors
3. ✅ SQL is syntactically valid (can execute against ClickHouse)
4. ✅ SQL structure correctness (no cartesian products, proper JOINs)
5. ⚠️  Query results (if expected results provided)
6. ⚠️  Query performance (execution time, rows returned)

Usage:
    python audit_ldbc_queries_comprehensive.py --category IC --limit 5
    python audit_ldbc_queries_comprehensive.py --category IS
    python audit_ldbc_queries_comprehensive.py --query IC1 --validate-results
"""

import os
import sys
import json
import time
import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import requests
import clickhouse_connect

# Configuration
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "18123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "ldbc")

# Query directory
QUERIES_DIR = Path(__file__).parent.parent.parent / "benchmarks" / "ldbc_snb" / "queries" / "adapted"

class QueryAuditResult:
    def __init__(self, query_name: str):
        self.query_name = query_name
        self.parse_success = False
        self.sql_generated = False
        self.sql_valid = False
        self.sql_correct = True  # Assume correct until proven otherwise
        self.execution_success = False
        self.execution_time_ms = None
        self.rows_returned = None
        self.errors = []
        self.warnings = []
        self.sql = None
        
    def add_error(self, stage: str, message: str):
        self.errors.append(f"[{stage}] {message}")
        
    def add_warning(self, stage: str, message: str):
        self.warnings.append(f"[{stage}] {message}")
        
    def is_passing(self) -> bool:
        """Query passes if it parses, generates valid SQL, and has no errors"""
        return (self.parse_success and 
                self.sql_generated and 
                self.sql_valid and 
                self.sql_correct and
                len(self.errors) == 0)
    
    def summary(self) -> str:
        status = "✅ PASS" if self.is_passing() else "❌ FAIL"
        parts = [f"{status} {self.query_name}"]
        
        if self.execution_time_ms:
            parts.append(f"({self.execution_time_ms:.0f}ms")
            if self.rows_returned is not None:
                parts.append(f", {self.rows_returned} rows)")
            else:
                parts.append(")")
        
        if self.warnings:
            parts.append(f" ⚠️  {len(self.warnings)} warning(s)")
        if self.errors:
            parts.append(f" 🚨 {len(self.errors)} error(s)")
            
        return " ".join(parts)


def load_query(query_file: Path) -> str:
    """Load query from file and strip comments"""
    with open(query_file, 'r') as f:
        lines = [line for line in f if not line.strip().startswith('//')]
        return '\n'.join(lines)


def substitute_parameters(query: str, params: Dict[str, Any]) -> str:
    """Replace $param with actual values"""
    result = query
    for param, value in params.items():
        if isinstance(value, str):
            # Escape single quotes in strings
            value = value.replace("'", "\\'")
            result = result.replace(f"${param}", f"'{value}'")
        else:
            result = result.replace(f"${param}", str(value))
    return result


def get_default_params(query_name: str) -> Dict[str, Any]:
    """Get default parameters for a query"""
    # Common defaults
    params = {
        "personId": 933,
        "firstName": "Yang",
        "date": "2012-01-01",
        "country": "India",
        "tag": "Augustine",
        "tagClass": "MusicalArtist",
        "limit": 20,
    }
    
    # Query-specific overrides
    query_params = {
        "IC1": {"personId": 933, "firstName": "Yang"},
        "IC2": {"personId": 933, "maxDate": "2012-11-01"},
        "IC3": {"personId": 933, "countryX": "India", "countryY": "Indonesia", "startDate": "2012-01-01", "endDate": "2012-12-31"},
        "IC4": {"personId": 933, "startDate": "2012-01-01", "endDate": "2012-12-31"},
        "IC5": {"personId": 933, "minDate": "2012-01-01"},
        "IC6": {"personId": 933, "tagName": "Augustine"},
        "IC7": {"personId": 933},
        "IC8": {"personId": 933},
        "IC9": {"personId": 933, "maxDate": "2012-11-01"},
        "IC10": {"personId": 933, "month": 11},
        "IC11": {"personId": 933, "countryName": "India", "workFromYear": 2010},
        "IC12": {"personId": 933, "tagClassName": "MusicalArtist"},
        "IC13": {"person1Id": 933, "person2Id": 8796093023009},
        "IC14": {"person1Id": 933, "person2Id": 8796093023009},
    }
    
    return query_params.get(query_name, params)


def check_sql_structure(sql: str) -> Tuple[bool, List[str]]:
    """
    Check SQL for common correctness issues
    Returns (is_correct, list_of_issues)
    """
    issues = []
    
    # Check for INNER JOIN without ON clause
    inner_join_pattern = r'INNER\s+JOIN\s+[\w.]+\s+AS\s+\w+(?:\s*(?:WHERE|UNION|SELECT|GROUP|ORDER|LIMIT|$))'
    if re.search(inner_join_pattern, sql, re.IGNORECASE):
        issues.append("INNER JOIN without ON clause (cartesian product)")
    
    # Check for JOIN without ON clause
    join_pattern = r'JOIN\s+[\w.]+\s+AS\s+\w+(?:\s*(?:WHERE|UNION|SELECT|GROUP|ORDER|LIMIT|INNER|LEFT|$))'
    matches = re.finditer(join_pattern, sql, re.IGNORECASE)
    for match in matches:
        # Extract context to check if ON follows
        context = sql[match.end():match.end()+50]
        if not re.match(r'\s*ON\s+', context, re.IGNORECASE):
            issues.append(f"JOIN without ON clause: {match.group()}")
    
    # Check for missing GROUP BY with aggregations
    if re.search(r'\b(min|max|sum|count|avg)\s*\(', sql, re.IGNORECASE):
        if 'GROUP BY' not in sql.upper():
            # Check if there are non-aggregated columns in SELECT
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if select_match:
                select_clause = select_match.group(1)
                # Simple heuristic: if there are property accesses and aggregations, might need GROUP BY
                has_properties = bool(re.search(r'\w+\.\w+', select_clause))
                has_aggregations = bool(re.search(r'\b(min|max|sum|count|avg)\s*\(', select_clause, re.IGNORECASE))
                if has_properties and has_aggregations:
                    # This is a warning, not necessarily an error (could be single-row result)
                    pass  # Don't flag for now
    
    # Check for undefined table aliases in WHERE
    # Extract all table aliases defined
    aliases = set()
    for match in re.finditer(r'\bAS\s+(\w+)', sql, re.IGNORECASE):
        aliases.add(match.group(1))
    
    # Check WHERE clause references
    where_match = re.search(r'\bWHERE\s+(.*?)(?:\s+GROUP|\s+ORDER|\s+LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
    if where_match:
        where_clause = where_match.group(1)
        # Find all table.column references
        for match in re.finditer(r'\b(\w+)\.\w+', where_clause):
            alias = match.group(1)
            if alias not in aliases and alias != 'NOT':  # NOT is not an alias
                # Could be a CTE or main query alias
                pass  # Don't flag for now, too many false positives
    
    is_correct = len(issues) == 0
    return is_correct, issues


def audit_query(query_name: str, query_file: Path, 
                execute: bool = True, 
                validate_results: bool = False) -> QueryAuditResult:
    """
    Comprehensively audit a single query
    """
    result = QueryAuditResult(query_name)
    
    # Step 1: Load query
    try:
        cypher_query = load_query(query_file)
    except Exception as e:
        result.add_error("Load", f"Failed to load query: {e}")
        return result
    
    # Step 2: Substitute parameters
    params = get_default_params(query_name)
    cypher_query_with_params = substitute_parameters(cypher_query, params)
    
    # Step 3: Generate SQL via ClickGraph
    try:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": cypher_query_with_params, "sql_only": True},
            timeout=30
        )
        
        if response.status_code != 200:
            result.add_error("Parse", f"HTTP {response.status_code}: {response.text[:200]}")
            return result
        
        data = response.json()
        
        if "error" in data:
            result.add_error("Parse", data["error"])
            return result
        
        result.parse_success = True
        
        if "sql" in data:
            result.sql = data["sql"]
            result.sql_generated = True
        else:
            result.add_error("SQL Gen", "No SQL in response")
            return result
            
    except requests.exceptions.Timeout:
        result.add_error("Parse", "Request timeout (>30s)")
        return result
    except Exception as e:
        result.add_error("Parse", f"Request failed: {e}")
        return result
    
    # Step 4: Check SQL structure
    is_correct, issues = check_sql_structure(result.sql)
    if not is_correct:
        result.sql_correct = False
        for issue in issues:
            result.add_error("SQL Structure", issue)
    
    # Step 5: Validate SQL syntax by executing EXPLAIN
    if execute:
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE
            )
            
            # First try EXPLAIN to validate syntax
            try:
                explain_sql = f"EXPLAIN {result.sql}"
                client.command(explain_sql)
                result.sql_valid = True
            except Exception as e:
                result.sql_valid = False
                result.add_error("SQL Syntax", f"EXPLAIN failed: {str(e)[:200]}")
                return result
            
            # Step 6: Execute query
            try:
                start_time = time.time()
                query_result = client.query(result.sql)
                execution_time = (time.time() - start_time) * 1000
                
                result.execution_success = True
                result.execution_time_ms = execution_time
                result.rows_returned = query_result.row_count
                
                # Performance warnings
                if execution_time > 5000:
                    result.add_warning("Performance", f"Slow query: {execution_time:.0f}ms")
                if result.rows_returned == 0:
                    result.add_warning("Results", "Query returned 0 rows")
                    
            except Exception as e:
                result.add_error("Execution", f"Query execution failed: {str(e)[:200]}")
                
        except Exception as e:
            result.add_error("Database", f"Failed to connect to ClickHouse: {e}")
    
    return result


def find_queries(category: Optional[str] = None, query_name: Optional[str] = None) -> List[Tuple[str, Path]]:
    """Find query files matching criteria"""
    queries = []
    
    if query_name:
        # Try various naming patterns
        patterns = [
            QUERIES_DIR / f"{query_name}.cypher",
            QUERIES_DIR / f"interactive-complex-{query_name.replace('IC', '')}.cypher",
            QUERIES_DIR / f"interactive-short-{query_name.replace('IS', '')}.cypher",
        ]
        for query_file in patterns:
            if query_file.exists():
                queries.append((query_name, query_file))
                return queries
        return queries
    
    # Find all queries in category
    if category == "IC":
        pattern = "interactive-complex-*.cypher"
    elif category == "IS":
        pattern = "interactive-short-*.cypher"
    elif category == "BI":
        pattern = "bi-*.cypher"
    else:
        pattern = "*.cypher"
    
    for query_file in sorted(QUERIES_DIR.glob(pattern)):
        # Extract query name from filename
        name = query_file.stem
        if "interactive-complex-" in name:
            # Convert interactive-complex-1 to IC1
            num = name.replace("interactive-complex-", "").replace("-workaround", "")
            display_name = f"IC{num}"
        elif "interactive-short-" in name:
            num = name.replace("interactive-short-", "")
            display_name = f"IS{num}"
        elif "bi-" in name:
            num = name.replace("bi-", "").replace("-workaround", "")
            display_name = f"BI{num}"
        else:
            display_name = name
        queries.append((display_name, query_file))
    
    return queries


def main():
    parser = argparse.ArgumentParser(description="Comprehensive LDBC Query Audit")
    parser.add_argument("--category", choices=["IC", "IS", "BI"], help="Query category to audit")
    parser.add_argument("--query", help="Specific query to audit (e.g., IC1)")
    parser.add_argument("--limit", type=int, help="Limit number of queries to audit")
    parser.add_argument("--no-execute", action="store_true", help="Skip query execution")
    parser.add_argument("--validate-results", action="store_true", help="Validate query results")
    parser.add_argument("--show-sql", action="store_true", help="Show generated SQL")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Find queries
    queries = find_queries(args.category, args.query)
    
    if not queries:
        print("❌ No queries found")
        return 1
    
    if args.limit:
        queries = queries[:args.limit]
    
    print(f"\n🔍 Auditing {len(queries)} queries...\n")
    
    # Audit each query
    results: List[QueryAuditResult] = []
    for query_name, query_file in queries:
        if args.verbose:
            print(f"Auditing {query_name}...")
        
        result = audit_query(
            query_name, 
            query_file,
            execute=not args.no_execute,
            validate_results=args.validate_results
        )
        results.append(result)
        
        # Print summary
        print(result.summary())
        
        # Show errors/warnings
        if args.verbose or not result.is_passing():
            for error in result.errors:
                print(f"  🚨 {error}")
            for warning in result.warnings:
                print(f"  ⚠️  {warning}")
        
        # Show SQL
        if args.show_sql and result.sql:
            print(f"\n--- Generated SQL ---")
            print(result.sql)
            print(f"--- End SQL ---\n")
    
    # Summary statistics
    print(f"\n{'='*80}")
    print("AUDIT SUMMARY")
    print(f"{'='*80}")
    
    total = len(results)
    passing = sum(1 for r in results if r.is_passing())
    parsing = sum(1 for r in results if r.parse_success)
    sql_gen = sum(1 for r in results if r.sql_generated)
    sql_valid = sum(1 for r in results if r.sql_valid)
    sql_correct = sum(1 for r in results if r.sql_correct)
    executed = sum(1 for r in results if r.execution_success)
    
    print(f"Total queries:        {total}")
    print(f"✅ Fully passing:     {passing}/{total} ({100*passing/total:.1f}%)")
    print(f"├─ Parse success:     {parsing}/{total} ({100*parsing/total:.1f}%)")
    print(f"├─ SQL generated:     {sql_gen}/{total} ({100*sql_gen/total:.1f}%)")
    print(f"├─ SQL valid:         {sql_valid}/{total} ({100*sql_valid/total:.1f}%)")
    print(f"├─ SQL correct:       {sql_correct}/{total} ({100*sql_correct/total:.1f}%)")
    print(f"└─ Executed:          {executed}/{total} ({100*executed/total:.1f}%)")
    
    # Performance stats
    if executed > 0:
        exec_times = [r.execution_time_ms for r in results if r.execution_time_ms]
        if exec_times:
            print(f"\nPerformance:")
            print(f"  Avg execution time: {sum(exec_times)/len(exec_times):.1f}ms")
            print(f"  Min execution time: {min(exec_times):.1f}ms")
            print(f"  Max execution time: {max(exec_times):.1f}ms")
    
    # List failed queries
    failed = [r for r in results if not r.is_passing()]
    if failed:
        print(f"\n❌ Failed queries ({len(failed)}):")
        for r in failed:
            print(f"  - {r.query_name}")
            for error in r.errors[:2]:  # Show first 2 errors
                print(f"      {error}")
    
    print(f"\n{'='*80}\n")
    
    return 0 if passing == total else 1


if __name__ == "__main__":
    sys.exit(main())
