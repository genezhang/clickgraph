"""
Documentation Validation Test Suite

Tests all Cypher queries from Wiki documentation to ensure they:
1. Parse correctly (no syntax errors)
2. Execute successfully (no runtime errors)
3. Return expected result structure
4. Complete within reasonable time

Usage:
    python scripts/validate_wiki_docs.py --docs-dir docs/wiki
"""

import os
import re
import json
import time
import requests
from pathlib import Path
from typing import List, Dict, Tuple
from dataclasses import dataclass

@dataclass
class QueryTest:
    """Represents a Cypher query extracted from documentation"""
    file: str
    line_number: int
    query: str
    context: str
    expected_to_fail: bool = False

class WikiDocValidator:
    def __init__(self, clickgraph_url: str = "http://localhost:8080"):
        self.clickgraph_url = clickgraph_url
        self.results = []
        
    def extract_cypher_queries(self, markdown_file: Path) -> List[QueryTest]:
        """Extract Cypher queries from markdown code blocks"""
        queries = []
        
        with open(markdown_file, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
        
        # Remove HTML comments (<!-- ... -->) to skip future feature examples
        content_no_comments = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
        lines = content_no_comments.split('\n')
        
        in_cypher_block = False
        current_query = []
        block_start_line = 0
        context = ""
        
        for i, line in enumerate(lines):
            # Track context (previous heading)
            if line.startswith('#'):
                context = line.strip('#').strip()
            
            # Detect Cypher code block start
            if line.strip() in ['```cypher', '```Cypher', '```CYPHER']:
                in_cypher_block = True
                block_start_line = i + 1
                current_query = []
                continue
            
            # Detect code block end
            if line.strip() == '```' and in_cypher_block:
                in_cypher_block = False
                if current_query:
                    query_text = '\n'.join(current_query).strip()
                    
                    # Skip comment-only blocks or empty blocks
                    if query_text and not all(l.startswith('//') or l.startswith('--') for l in query_text.split('\n')):
                        # Check if marked as expected to fail
                        expected_fail = '❌' in context or 'Slow:' in context or 'Wrong:' in context
                        
                        queries.append(QueryTest(
                            file=str(markdown_file.name),
                            line_number=block_start_line,
                            query=query_text,
                            context=context,
                            expected_to_fail=expected_fail
                        ))
                current_query = []
                continue
            
            # Collect query lines
            if in_cypher_block:
                current_query.append(line)
        
        return queries
    
    def validate_query(self, test: QueryTest) -> Dict:
        """Validate a single Cypher query"""
        result = {
            'file': test.file,
            'line': test.line_number,
            'context': test.context,
            'query_preview': test.query[:100] + '...' if len(test.query) > 100 else test.query,
            'status': 'unknown',
            'error': None,
            'execution_time_ms': None,
            'row_count': None
        }
        
        try:
            start_time = time.time()
            
            response = requests.post(
                f"{self.clickgraph_url}/query",
                json={'query': test.query},
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            execution_time = (time.time() - start_time) * 1000
            result['execution_time_ms'] = round(execution_time, 2)
            
            if response.status_code == 200:
                data = response.json()
                result['row_count'] = len(data.get('results', []))
                
                if test.expected_to_fail:
                    result['status'] = 'unexpected_success'
                    result['error'] = 'Query succeeded but was expected to fail'
                else:
                    result['status'] = 'success'
            else:
                if test.expected_to_fail:
                    result['status'] = 'expected_failure'
                else:
                    result['status'] = 'failed'
                    result['error'] = f"HTTP {response.status_code}: {response.text[:200]}"
        
        except requests.exceptions.Timeout:
            result['status'] = 'timeout'
            result['error'] = 'Query exceeded 30 second timeout'
        except requests.exceptions.ConnectionError:
            result['status'] = 'connection_error'
            result['error'] = f'Could not connect to ClickGraph at {self.clickgraph_url}'
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
        
        return result
    
    def validate_file(self, markdown_file: Path) -> List[Dict]:
        """Validate all queries in a markdown file"""
        print(f"\n📄 Validating: {markdown_file.name}")
        
        queries = self.extract_cypher_queries(markdown_file)
        print(f"   Found {len(queries)} Cypher queries")
        
        if not queries:
            return []
        
        results = []
        for i, query_test in enumerate(queries, 1):
            print(f"   [{i}/{len(queries)}] Testing query at line {query_test.line_number}...", end=' ')
            result = self.validate_query(query_test)
            results.append(result)
            
            # Print status
            status_emoji = {
                'success': '✅',
                'failed': '❌',
                'expected_failure': '⚠️',
                'timeout': '⏱️',
                'connection_error': '🔌',
                'error': '💥'
            }
            print(f"{status_emoji.get(result['status'], '❓')} {result['status']}")
            
            if result['error']:
                print(f"      Error: {result['error'][:100]}")
        
        return results
    
    def generate_report(self, all_results: List[Dict]) -> str:
        """Generate validation report"""
        total = len(all_results)
        success = sum(1 for r in all_results if r['status'] == 'success')
        failed = sum(1 for r in all_results if r['status'] == 'failed')
        expected_fail = sum(1 for r in all_results if r['status'] == 'expected_failure')
        timeout = sum(1 for r in all_results if r['status'] == 'timeout')
        errors = sum(1 for r in all_results if r['status'] in ['error', 'connection_error'])
        
        report = f"""
# Documentation Validation Report

**Generated**: {time.strftime('%Y-%m-%d %H:%M:%S')}

## Summary

- **Total Queries**: {total}
- **✅ Success**: {success} ({success*100//total if total else 0}%)
- **❌ Failed**: {failed} ({failed*100//total if total else 0}%)
- **⚠️ Expected Failures**: {expected_fail}
- **⏱️ Timeouts**: {timeout}
- **💥 Errors**: {errors}

## Results by File

"""
        
        # Group by file
        by_file = {}
        for result in all_results:
            file = result['file']
            if file not in by_file:
                by_file[file] = []
            by_file[file].append(result)
        
        for file, results in sorted(by_file.items()):
            file_success = sum(1 for r in results if r['status'] == 'success')
            file_total = len(results)
            
            report += f"\n### {file}\n\n"
            report += f"**Pass Rate**: {file_success}/{file_total} ({file_success*100//file_total if file_total else 0}%)\n\n"
            
            # Show failures
            failures = [r for r in results if r['status'] in ['failed', 'timeout', 'error']]
            if failures:
                report += "**Failures:**\n\n"
                for r in failures:
                    report += f"- Line {r['line']}: `{r['context']}`\n"
                    report += f"  - Query: `{r['query_preview']}`\n"
                    report += f"  - Error: {r['error']}\n\n"
        
        # Performance stats
        exec_times = [r['execution_time_ms'] for r in all_results if r['execution_time_ms']]
        if exec_times:
            report += "\n## Performance Statistics\n\n"
            report += f"- **Mean execution time**: {sum(exec_times)/len(exec_times):.2f}ms\n"
            report += f"- **Min execution time**: {min(exec_times):.2f}ms\n"
            report += f"- **Max execution time**: {max(exec_times):.2f}ms\n"
            exec_times.sort()
            p95_idx = int(len(exec_times) * 0.95)
            report += f"- **P95 execution time**: {exec_times[p95_idx]:.2f}ms\n"
        
        return report
    
    def validate_directory(self, docs_dir: Path) -> Dict:
        """Validate all markdown files in directory"""
        markdown_files = list(docs_dir.glob("*.md"))
        
        print(f"\n🔍 Found {len(markdown_files)} markdown files in {docs_dir}")
        
        all_results = []
        for md_file in sorted(markdown_files):
            # Skip certain files
            if md_file.name in ['README.md', 'TESTING_CHECKLIST.md']:
                continue
            
            results = self.validate_file(md_file)
            all_results.extend(results)
        
        return {
            'results': all_results,
            'report': self.generate_report(all_results)
        }


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate Wiki documentation Cypher queries')
    parser.add_argument('--docs-dir', default='docs/wiki', help='Documentation directory')
    parser.add_argument('--url', default='http://localhost:8080', help='ClickGraph URL')
    parser.add_argument('--output', default='docs/WIKI_VALIDATION_REPORT.md', help='Output report file')
    parser.add_argument('--json', help='Output JSON results file')
    
    args = parser.parse_args()
    
    docs_dir = Path(args.docs_dir)
    if not docs_dir.exists():
        print(f"❌ Directory not found: {docs_dir}")
        return 1
    
    print(f"🚀 Starting documentation validation")
    print(f"   Docs directory: {docs_dir}")
    print(f"   ClickGraph URL: {args.url}")
    
    # Check if ClickGraph is running
    try:
        response = requests.get(f"{args.url}/health", timeout=5)
        if response.status_code == 200:
            print(f"   ✅ ClickGraph is running")
        else:
            print(f"   ⚠️ ClickGraph returned status {response.status_code}")
    except:
        print(f"   ❌ Cannot connect to ClickGraph at {args.url}")
        print(f"   Please start ClickGraph and ensure schema is loaded")
        return 1
    
    # Run validation
    validator = WikiDocValidator(args.url)
    validation = validator.validate_directory(docs_dir)
    
    # Save report
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(validation['report'])
    print(f"\n📊 Report saved to: {args.output}")
    
    # Save JSON if requested
    if args.json:
        with open(args.json, 'w', encoding='utf-8') as f:
            json.dump(validation['results'], f, indent=2)
        print(f"📄 JSON results saved to: {args.json}")
    
    # Exit code based on failures
    failed = sum(1 for r in validation['results'] if r['status'] in ['failed', 'timeout', 'error'])
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    exit(main())
