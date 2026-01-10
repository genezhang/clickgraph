#!/usr/bin/env python3
"""
Audit Official LDBC Queries Only

This script tests ONLY official LDBC SNB queries (not simplified adaptations).
Use this for accurate benchmarking and validation against LDBC spec.
"""

import requests
import json
import sys
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime

# Configuration
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "official_sql"
DATABASE = "ldbc_snb"  # Schema name in ClickGraph

# Sample parameters for queries (matching LDBC spec)
PARAMS = {
    # Date parameters (Unix timestamps in milliseconds)
    "datetime": 1322697600000,  # 2011-12-01T00:00:00.000
    "date": 1322697600000,      # 2011-12-01
    "startDate": 1293840000000,  # 2011-01-01
    "endDate": 1325376000000,    # 2012-01-01
    "minDate": 1293840000000,
    "maxDate": 1356998400000,    # 2013-01-01
    
    # Person/entity IDs
    "personId": 933,
    "person1Id": 933,
    "person2Id": 8796093022390,
    "messageId": 618475290625,
    "postId": 618475290625,
    "commentId": 1099511628185,
    "forumId": 618475290625,
    
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
    "limit": 10,
    "maxKnowsLimit": 20,
    "lengthThreshold": 100,
    "workFromYear": 2010,
}


class OfficialQueryAuditor:
    def __init__(self):
        self.results = []
        self.queries_processed = 0
        self.queries_passed = 0
        self.queries_failed = 0
        
        # Create output directory
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        
    def extract_query_from_file(self, filepath: Path) -> Optional[str]:
        """Extract Cypher query from official query file."""
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
        if query and 'MATCH' in query:
            return query
        return None
    
    def get_sql(self, query: str) -> Tuple[bool, str, Optional[str]]:
        """Get generated SQL for a query."""
        try:
            response = requests.post(
                f"{CLICKGRAPH_URL}/query",
                json={
                    "query": query,
                    "schema_name": DATABASE,
                    "parameters": PARAMS,
                    "sql_only": True
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                sql = result.get("generated_sql") or result.get("sql", "")
                
                if sql.startswith("PLANNING_ERROR"):
                    return False, f"Planning: {sql[16:200]}", None
                elif sql.startswith("RENDER_ERROR"):
                    return False, f"Render: {sql[:200]}", None
                elif "SELECT" in sql.upper():
                    return True, f"OK ({len(sql)} chars)", sql
                else:
                    return False, f"Invalid SQL", None
            else:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("error", response.text)
                except:
                    error_msg = response.text
                return False, f"HTTP {response.status_code}: {error_msg[:200]}", None
                
        except Exception as e:
            return False, f"Exception: {str(e)[:200]}", None
    
    def save_sql(self, query_name: str, cypher: str, sql: str, status: str):
        """Save generated SQL to file."""
        filename = f"{query_name}.sql"
        filepath = RESULTS_DIR / filename
        
        with open(filepath, 'w') as f:
            f.write(f"-- LDBC Official Query: {query_name}\n")
            f.write(f"-- Status: {status}\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n")
            f.write(f"-- Database: {DATABASE}\n")
            f.write("\n")
            f.write("-- Original Cypher Query:\n")
            for line in cypher.split('\n'):
                f.write(f"-- {line}\n")
            f.write("\n")
            f.write("-- Generated ClickHouse SQL:\n")
            f.write(sql)
            f.write("\n")
    
    def process_query_file(self, filepath: Path, category: str):
        """Process a single official query file."""
        query_name = f"{category}-{filepath.stem}"
        
        query = self.extract_query_from_file(filepath)
        if not query:
            return
        
        self.queries_processed += 1
        success, message, sql = self.get_sql(query)
        
        status = "✓" if success else "✗"
        print(f"  {query_name:30s} {status} {message}")
        
        result = {
            "query": query_name,
            "file": str(filepath.relative_to(BASE_DIR)),
            "category": category,
            "status": "pass" if success else "fail",
            "message": message,
        }
        
        if success and sql:
            self.queries_passed += 1
            self.save_sql(query_name, query, sql, "PASS")
            result["sql_file"] = f"{query_name}.sql"
        else:
            self.queries_failed += 1
            result["sql_file"] = None
        
        self.results.append(result)
    
    def audit_category(self, category_name: str, query_dir: Path):
        """Audit all queries in a category."""
        print(f"\n{'='*80}")
        print(f"  {category_name}")
        print(f"{'='*80}\n")
        
        if not query_dir.exists():
            print(f"  Directory not found: {query_dir}")
            return
        
        query_files = sorted(query_dir.glob("*.cypher"))
        print(f"  Found {len(query_files)} official queries\n")
        
        for query_file in query_files:
            self.process_query_file(query_file, category_name)
    
    def generate_report(self):
        """Generate summary report."""
        report_path = RESULTS_DIR / "official_audit_report.md"
        
        with open(report_path, 'w') as f:
            f.write("# LDBC Official Queries - SQL Generation Audit\n\n")
            f.write(f"**Generated**: {datetime.now().isoformat()}\n\n")
            f.write(f"**Database**: {DATABASE}\n\n")
            f.write(f"**ClickGraph**: {CLICKGRAPH_URL}\n\n")
            f.write("**Note**: This audit includes ONLY official LDBC SNB queries.\n\n")
            
            f.write("## Summary\n\n")
            f.write(f"- Total Official Queries: {self.queries_processed}\n")
            f.write(f"- ✓ SQL Generation Success: {self.queries_passed}\n")
            f.write(f"- ✗ SQL Generation Failed: {self.queries_failed}\n")
            success_rate = 100 * self.queries_passed // max(self.queries_processed, 1)
            f.write(f"- Success Rate: {success_rate}%\n\n")
            
            # Group by category
            categories = {}
            for result in self.results:
                cat = result['category']
                if cat not in categories:
                    categories[cat] = {'pass': 0, 'fail': 0, 'queries': []}
                categories[cat]['queries'].append(result)
                if result['status'] == 'pass':
                    categories[cat]['pass'] += 1
                else:
                    categories[cat]['fail'] += 1
            
            f.write("## Results by Category\n\n")
            for cat, data in sorted(categories.items()):
                total = data['pass'] + data['fail']
                rate = 100 * data['pass'] // max(total, 1)
                f.write(f"### {cat}: {data['pass']}/{total} ({rate}%)\n\n")
                
            f.write("## Detailed Results\n\n")
            f.write("| Query | Category | Status | Message |\n")
            f.write("|-------|----------|--------|----------|\n")
            
            for result in self.results:
                query = result['query']
                cat = result['category']
                status = "✓" if result['status'] == 'pass' else "✗"
                message = result['message'][:60]
                f.write(f"| {query} | {cat} | {status} | {message} |\n")
            
            f.write("\n## Failed Queries\n\n")
            failed = [r for r in self.results if r['status'] == 'fail']
            if failed:
                for result in failed:
                    f.write(f"### {result['query']}\n\n")
                    f.write(f"**Category**: {result['category']}\n\n")
                    f.write(f"**Error**: {result['message']}\n\n")
            else:
                f.write("No failed queries! 🎉\n\n")
            
            f.write("## Benchmarking Recommendation\n\n")
            f.write("For official LDBC SNB benchmarking, use only queries marked with ✓.\n\n")
            f.write("These queries match the official LDBC specification and can be compared\n")
            f.write("with results from other graph databases (Neo4j, TigerGraph, etc.)\n")
        
        print(f"\n📄 Report saved to: {report_path}")
    
    def run(self):
        """Run audit for official queries."""
        print(f"{'='*80}")
        print(f"  LDBC Official Queries SQL Generation Audit")
        print(f"{'='*80}")
        print(f"  Server:     {CLICKGRAPH_URL}")
        print(f"  Database:   {DATABASE}")
        print(f"  Output Dir: {RESULTS_DIR}")
        
        # Check server health
        try:
            response = requests.get(f"{CLICKGRAPH_URL}/health", timeout=5)
            if response.status_code != 200:
                print(f"\n⚠️  Server health check failed!")
                return False
        except:
            print(f"\n⚠️  Cannot connect to server at {CLICKGRAPH_URL}")
            return False
        
        # Audit official queries
        official_dir = BASE_DIR / "queries" / "official"
        
        # Business Intelligence queries
        self.audit_category("BI", official_dir / "bi")
        
        # Interactive Complex queries
        self.audit_category("IC", official_dir / "interactive")
        
        # Generate report
        print(f"\n{'='*80}")
        print(f"  Generating Report")
        print(f"{'='*80}")
        self.generate_report()
        
        print(f"\n{'='*80}")
        print(f"  SUMMARY - Official Queries Only")
        print(f"{'='*80}")
        print(f"  Total:  {self.queries_processed}")
        print(f"  ✓ Pass: {self.queries_passed}")
        print(f"  ✗ Fail: {self.queries_failed}")
        success_rate = 100 * self.queries_passed // max(self.queries_processed, 1)
        print(f"  Rate:   {success_rate}%")
        print(f"{'='*80}\n")
        
        print("💡 Use these results for official LDBC benchmarking.")
        print("   Adapted queries (BI-1a, AGG-1, etc.) are excluded.\n")
        
        return self.queries_failed == 0


def main():
    auditor = OfficialQueryAuditor()
    success = auditor.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
