#!/usr/bin/env python3
"""
Capture Generated SQL for LDBC Queries

This script:
1. Reads all adapted LDBC queries
2. Sends them to ClickGraph's /query endpoint with sql_only=true
3. Captures the generated SQL
4. Saves to results/generated_sql/ for analysis
5. Generates a summary report for correctness review
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
RESULTS_DIR = BASE_DIR / "results" / "generated_sql"
DATABASE = "ldbc"

# Sample parameters for queries
PARAMS = {
    "personId": 933,  # Valid person ID from dataset
    "person1Id": 933,
    "person2Id": 8796093022390,
    "firstName": "Chau",
    "countryName": "India",
    "country": "India",
    "countryXName": "India",
    "countryYName": "China",
    "country1": "India",
    "country2": "China",
    "city": "Beijing",
    "forum": 618475290625,
    "forumId": 618475290625,
    "postId": 618475290625,
    "commentId": 1099511628185,
    "messageId": 618475290625,
    "tagClass": "MusicalArtist",
    "tagClassName": "MusicalArtist",
    "tagClassNames": ["MusicalArtist", "Actor"],
    "tag": "Arnold_Schwarzenegger",
    "tagName": "Arnold_Schwarzenegger",
    "tags": ["Arnold_Schwarzenegger", "Jackie_Chan"],
    "messageDatetime": "2012-01-01",
    "startDate": "2011-01-01",
    "minDate": "2011-01-01",
    "maxDate": "2012-12-31",
    "endDate": "2012-12-31",
    "date": "2012-01-01",
    "datetime": "2012-01-01T00:00:00.000",
    "workFromYear": 2010,
    "lengthThreshold": 100,
    "limit": 10,
    "maxKnowsLimit": 20,
    "month": 1,
    "languages": ["en", "es"],
}


class SQLCapture:
    def __init__(self):
        self.results = []
        self.queries_processed = 0
        self.queries_failed = 0
        
        # Create output directory
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        
    def extract_queries_from_file(self, filepath: Path) -> List[Tuple[str, str]]:
        """
        Extract multiple queries from a file with section comments.
        Returns list of (query_name, query_text) tuples.
        """
        content = filepath.read_text()
        queries = []
        current_query = []
        current_name = None
        
        for line in content.split('\n'):
            stripped = line.strip()
            
            # Detect section headers like "// BI-1. Posting Summary" or "// BI-1a:"
            if stripped.startswith('//') and any(marker in stripped for marker in ['BI-', 'IC-', 'IS-', 'AGG-', 'COMPLEX-']):
                # Save previous query if exists
                if current_query and current_name:
                    query_text = '\n'.join(current_query).strip()
                    if query_text and 'MATCH' in query_text:
                        queries.append((current_name, query_text))
                
                # Extract query name from comment
                # Examples: "// BI-1. Posting Summary" -> "BI-1"
                #           "// BI-1a: Message count" -> "BI-1a"
                for part in stripped.split():
                    if any(marker in part for marker in ['BI-', 'IC-', 'IS-', 'AGG-', 'COMPLEX-']):
                        current_name = part.replace(':', '').replace('.', '')
                        break
                
                current_query = []
                continue
            
            # Skip comment lines
            if stripped.startswith('//') or stripped.startswith('/*') or stripped.startswith('*') or stripped.endswith('*/'):
                continue
            
            # Skip section separators
            if '=' * 10 in line:
                continue
                
            # Add non-empty lines to current query
            if stripped:
                current_query.append(line)
        
        # Save last query
        if current_query and current_name:
            query_text = '\n'.join(current_query).strip()
            if query_text and 'MATCH' in query_text:
                queries.append((current_name, query_text))
        
        return queries
    
    def extract_single_query(self, filepath: Path) -> Optional[Tuple[str, str]]:
        """Extract a single query from standalone file."""
        content = filepath.read_text()
        
        # Remove comments
        lines = []
        for line in content.split('\n'):
            stripped = line.strip()
            if not stripped.startswith('//') and not stripped.startswith('/*') and not stripped.endswith('*/'):
                if '=' * 10 not in line:
                    lines.append(line)
        
        query = '\n'.join(lines).strip()
        if query and 'MATCH' in query:
            query_name = filepath.stem
            return (query_name, query)
        return None
    
    def get_sql(self, query: str) -> Tuple[bool, str, Optional[str]]:
        """
        Get generated SQL for a query.
        Returns (success, message, sql)
        """
        try:
            response = requests.post(
                f"{CLICKGRAPH_URL}/query",
                json={
                    "query": query,
                    "database": DATABASE,
                    "parameters": PARAMS,
                    "sql_only": True
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                sql = result.get("generated_sql") or result.get("sql", "")
                
                if sql.startswith("PLANNING_ERROR"):
                    return False, f"Planning error: {sql[16:200]}", None
                elif sql.startswith("RENDER_ERROR"):
                    return False, f"Render error: {sql[:200]}", None
                elif "SELECT" in sql.upper():
                    return True, f"Success ({len(sql)} chars)", sql
                else:
                    return False, f"Invalid SQL (no SELECT): {sql[:100]}", None
            else:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("error", response.text)
                except:
                    error_msg = response.text
                return False, f"HTTP {response.status_code}: {error_msg[:300]}", None
                
        except requests.exceptions.ConnectionError:
            return False, "Cannot connect to ClickGraph server", None
        except Exception as e:
            return False, f"Exception: {str(e)[:200]}", None
    
    def save_sql(self, query_name: str, cypher: str, sql: str, status: str):
        """Save generated SQL to file."""
        filename = f"{query_name}.sql"
        filepath = RESULTS_DIR / filename
        
        with open(filepath, 'w') as f:
            f.write(f"-- LDBC Query: {query_name}\n")
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
    
    def process_multi_query_file(self, filepath: Path):
        """Process file containing multiple queries."""
        print(f"\n{'='*80}")
        print(f"  File: {filepath.relative_to(BASE_DIR)}")
        print(f"{'='*80}")
        
        queries = self.extract_queries_from_file(filepath)
        print(f"  Found {len(queries)} queries in file\n")
        
        for query_name, cypher in queries:
            self.queries_processed += 1
            success, message, sql = self.get_sql(cypher)
            
            status = "✓ PASS" if success else "✗ FAIL"
            print(f"  {query_name:20s} {status:8s} {message}")
            
            if success and sql:
                self.save_sql(query_name, cypher, sql, "PASS")
                self.results.append({
                    "query": query_name,
                    "file": str(filepath.relative_to(BASE_DIR)),
                    "status": "pass",
                    "message": message,
                    "sql_file": f"{query_name}.sql"
                })
            else:
                self.queries_failed += 1
                self.results.append({
                    "query": query_name,
                    "file": str(filepath.relative_to(BASE_DIR)),
                    "status": "fail",
                    "message": message,
                    "sql_file": None
                })
    
    def process_single_query_file(self, filepath: Path):
        """Process file containing single query."""
        query_data = self.extract_single_query(filepath)
        if not query_data:
            return
        
        query_name, cypher = query_data
        self.queries_processed += 1
        
        success, message, sql = self.get_sql(cypher)
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"  {query_name:30s} {status:8s} {message}")
        
        if success and sql:
            self.save_sql(query_name, cypher, sql, "PASS")
            self.results.append({
                "query": query_name,
                "file": str(filepath.relative_to(BASE_DIR)),
                "status": "pass",
                "message": message,
                "sql_file": f"{query_name}.sql"
            })
        else:
            self.queries_failed += 1
            self.results.append({
                "query": query_name,
                "file": str(filepath.relative_to(BASE_DIR)),
                "status": "fail",
                "message": message,
                "sql_file": None
            })
    
    def generate_report(self):
        """Generate summary report."""
        report_path = RESULTS_DIR / "audit_report.md"
        
        with open(report_path, 'w') as f:
            f.write("# LDBC Query SQL Generation Audit Report\n\n")
            f.write(f"**Generated**: {datetime.now().isoformat()}\n\n")
            f.write(f"**Database**: {DATABASE}\n\n")
            f.write(f"**ClickGraph**: {CLICKGRAPH_URL}\n\n")
            
            f.write("## Summary\n\n")
            f.write(f"- Total Queries Processed: {self.queries_processed}\n")
            f.write(f"- ✓ Passed: {self.queries_processed - self.queries_failed}\n")
            f.write(f"- ✗ Failed: {self.queries_failed}\n")
            f.write(f"- Success Rate: {100*(self.queries_processed - self.queries_failed)//max(self.queries_processed,1)}%\n\n")
            
            f.write("## Results by Query\n\n")
            f.write("| Query | Status | Message | SQL File |\n")
            f.write("|-------|--------|---------|----------|\n")
            
            for result in self.results:
                query = result['query']
                status = "✓" if result['status'] == 'pass' else "✗"
                message = result['message'][:60]
                sql_file = result.get('sql_file', 'N/A')
                f.write(f"| {query} | {status} | {message} | {sql_file} |\n")
            
            f.write("\n## Failed Queries\n\n")
            failed = [r for r in self.results if r['status'] == 'fail']
            if failed:
                for result in failed:
                    f.write(f"### {result['query']}\n\n")
                    f.write(f"**File**: {result['file']}\n\n")
                    f.write(f"**Error**: {result['message']}\n\n")
            else:
                f.write("No failed queries! 🎉\n\n")
            
            f.write("## Next Steps\n\n")
            f.write("1. Review generated SQL files in `results/generated_sql/`\n")
            f.write("2. Verify SQL correctness against LDBC spec\n")
            f.write("3. Check for optimization opportunities\n")
            f.write("4. Test SQL execution against ClickHouse\n")
            f.write("5. Compare results with Neo4j/expected outputs\n")
        
        print(f"\n📄 Report saved to: {report_path}")
    
    def run(self):
        """Run SQL capture for all adapted queries."""
        print(f"{'='*80}")
        print(f"  LDBC Query SQL Capture")
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
        
        # Find all adapted query files
        adapted_dir = BASE_DIR / "queries" / "adapted"
        
        # Multi-query files
        multi_files = [
            adapted_dir / "bi-queries-adapted.cypher",
            adapted_dir / "ldbc-interactive-adapted.cypher",
        ]
        
        for filepath in multi_files:
            if filepath.exists():
                self.process_multi_query_file(filepath)
        
        # Single-query files
        print(f"\n{'='*80}")
        print(f"  Single Query Files")
        print(f"{'='*80}\n")
        
        single_files = [f for f in adapted_dir.glob("*.cypher") 
                       if f.name not in ['bi-queries-adapted.cypher', 'ldbc-interactive-adapted.cypher']]
        
        for filepath in sorted(single_files):
            self.process_single_query_file(filepath)
        
        # Generate report
        print(f"\n{'='*80}")
        print(f"  Generating Report")
        print(f"{'='*80}")
        self.generate_report()
        
        print(f"\n{'='*80}")
        print(f"  SUMMARY")
        print(f"{'='*80}")
        print(f"  Total:  {self.queries_processed}")
        print(f"  ✓ Pass: {self.queries_processed - self.queries_failed}")
        print(f"  ✗ Fail: {self.queries_failed}")
        print(f"  Rate:   {100*(self.queries_processed - self.queries_failed)//max(self.queries_processed,1)}%")
        print(f"{'='*80}\n")
        
        return self.queries_failed == 0


def main():
    capture = SQLCapture()
    success = capture.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
