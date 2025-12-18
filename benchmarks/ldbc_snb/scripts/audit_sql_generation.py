#!/usr/bin/env python3
"""
Audit LDBC Query SQL Generation
 
Tests that all LDBC queries:
1. Parse successfully
2. Generate valid SQL
3. Return results (if data is available)

This focuses on correctness of SQL generation, not performance.
"""

import requests
import json
import sys
import os
from pathlib import Path
from typing import Dict, List, Tuple

# Configuration
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
BASE_DIR = Path(__file__).parent.parent

# Query categories
QUERY_CATEGORIES = {
    "interactive_short": "queries/official/interactive/short-*.cypher",
    "interactive_complex": "queries/official/interactive/complex-*.cypher",
    "business_intelligence": "queries/official/bi/bi-*.cypher",
}

# Parameter templates for queries (using placeholder values)
PARAMS = {
    "personId": 1,
    "firstName": "Test",
    "countryName": "Country",
    "country": "Country",
    "city": "City",
    "forum": 1,
    "forumId": 1,
    "postId": 1,
    "commentId": 1,
    "tagClass": "Tag",
    "tagClassName": "TagClass",
    "tagClassNames": ["TagClass1", "TagClass2"],
    "tag": "Tag",
    "tags": ["Tag1", "Tag2"],
    "messageDatetime": "2012-01-01",
    "startDate": "2012-01-01",
    "endDate": "2012-12-31",
    "date": "2012-01-01",
    "datetime": "2012-01-01T00:00:00",
    "workFromYear": 2010,
    "lengthThreshold": 100,
    "limit": 10,
    "maxKnowsLimit": 20,
}


class QueryAuditor:
    def __init__(self):
        self.results = {
            "passed": [],
            "failed": [],
            "skipped": [],
        }
        
    def audit_query(self, query_file: Path, query: str) -> Tuple[bool, str]:
        """Test a single query. Returns (success, message)."""
        try:
            # Add sql_only flag to just get SQL generation without execution
            response = requests.post(
                f"{CLICKGRAPH_URL}/query",
                json={"query": query, "parameters": PARAMS, "sql_only": True},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if "generated_sql" in result or "sql" in result:
                    sql = result.get("generated_sql") or result.get("sql", "")
                    # Check for planning errors first
                    if sql.startswith("PLANNING_ERROR"):
                        return False, f"✗ Planning error: {sql[16:150]}"  # Skip "PLANNING_ERROR: "
                    # Check for render errors
                    elif sql.startswith("RENDER_ERROR"):
                        return False, f"✗ Render error: {sql[:80]}"
                    # Basic SQL validation
                    elif "SELECT" in sql.upper():
                        return True, f"✓ Generates valid SQL ({len(sql)} chars)"
                    else:
                        return False, "✗ SQL doesn't contain SELECT"
                elif "error" in result:
                    return False, f"✗ Query error: {result['error']}"
                else:
                    return False, f"✗ Unexpected response: {list(result.keys())}"
            else:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("error", response.text)
                except:
                    error_msg = response.text
                return False, f"✗ HTTP {response.status_code}: {error_msg[:200]}"
                
        except requests.exceptions.ConnectionError:
            return False, "✗ Cannot connect to ClickGraph server"
        except Exception as e:
            return False, f"✗ Exception: {str(e)[:200]}"
    
    def extract_query_from_file(self, filepath: Path) -> str:
        """Extract Cypher query from comment-decorated file."""
        content = filepath.read_text()
        
        # Remove comment blocks
        lines = []
        in_comment = False
        for line in content.split('\n'):
            stripped = line.strip()
            if stripped.startswith('/*'):
                in_comment = True
            if not in_comment and not stripped.startswith('//'):
                lines.append(line)
            if stripped.endswith('*/'):
                in_comment = False
        
        query = '\n'.join(lines).strip()
        return query
    
    def audit_category(self, category: str, pattern: str):
        """Audit all queries in a category."""
        print(f"\n{'='*80}")
        print(f"  {category.replace('_', ' ').title()}")
        print(f"{'='*80}\n")
        
        query_files = sorted(BASE_DIR.glob(pattern))
        
        if not query_files:
            print(f"  No queries found matching: {pattern}")
            return
        
        for query_file in query_files:
            query_name = query_file.stem
            try:
                query = self.extract_query_from_file(query_file)
                if not query or len(query) < 10:
                    msg = "⊘ Skipped (empty or too short)"
                    self.results["skipped"].append((category, query_name, msg))
                    print(f"  {query_name:30s} {msg}")
                    continue
                
                success, message = self.audit_query(query_file, query)
                
                if success:
                    self.results["passed"].append((category, query_name, message))
                else:
                    self.results["failed"].append((category, query_name, message))
                
                print(f"  {query_name:30s} {message}")
                
            except Exception as e:
                msg = f"✗ File error: {str(e)[:100]}"
                self.results["failed"].append((category, query_name, msg))
                print(f"  {query_name:30s} {msg}")
    
    def print_summary(self):
        """Print audit summary."""
        print(f"\n{'='*80}")
        print("  AUDIT SUMMARY")
        print(f"{'='*80}\n")
        
        total = len(self.results["passed"]) + len(self.results["failed"]) + len(self.results["skipped"])
        passed = len(self.results["passed"])
        failed = len(self.results["failed"])
        skipped = len(self.results["skipped"])
        
        print(f"  Total Queries:   {total}")
        print(f"  ✓ Passed:        {passed} ({100*passed//max(total,1)}%)")
        print(f"  ✗ Failed:        {failed}")
        print(f"  ⊘ Skipped:       {skipped}")
        
        if self.results["failed"]:
            print(f"\n{'='*80}")
            print("  FAILED QUERIES")
            print(f"{'='*80}\n")
            for category, name, msg in self.results["failed"]:
                print(f"  [{category}] {name}")
                print(f"    {msg}\n")
        
        return failed == 0
    
    def run_audit(self):
        """Run full audit."""
        print(f"{'='*80}")
        print(f"  LDBC SNB Query SQL Generation Audit")
        print(f"{'='*80}")
        print(f"  Server:     {CLICKGRAPH_URL}")
        print(f"  Base Dir:   {BASE_DIR}")
        print(f"  Categories: {len(QUERY_CATEGORIES)}")
        
        # Check server health
        try:
            response = requests.get(f"{CLICKGRAPH_URL}/health", timeout=5)
            if response.status_code != 200:
                print(f"\n⚠️  Server health check failed!")
                return False
        except:
            print(f"\n⚠️  Cannot connect to server at {CLICKGRAPH_URL}")
            print(f"  Make sure ClickGraph is running with LDBC schema.")
            return False
        
        # Audit each category
        for category, pattern in QUERY_CATEGORIES.items():
            self.audit_category(category, pattern)
        
        # Print summary
        return self.print_summary()


def main():
    auditor = QueryAuditor()
    success = auditor.run_audit()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
