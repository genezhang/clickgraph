#!/usr/bin/env python3
"""
LDBC SNB BI Query Runner for ClickGraph - ORIGINAL QUERIES

This version uses the ORIGINAL LDBC BI query patterns without workarounds.
Use this to track actual ClickGraph bugs that need fixing.

Usage:
    python run_bi_queries_original.py                    # Run all queries
    python run_bi_queries_original.py --query bi-6      # Run specific query
    python run_bi_queries_original.py --sql-only        # Show SQL without executing
"""

import argparse
import json
import time
import sys
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import requests

# ClickGraph server configuration
CLICKGRAPH_URL = "http://localhost:8080"
SCHEMA_PATH = "./benchmarks/ldbc_snb/schemas/ldbc_snb_datagen.yaml"

@dataclass
class QueryResult:
    """Result of a query execution"""
    name: str
    query: str
    sql: Optional[str] = None
    data: Optional[List[Dict]] = None
    row_count: int = 0
    duration_ms: float = 0.0
    error: Optional[str] = None
    success: bool = False

# =============================================================================
# ORIGINAL LDBC BI Queries - NO WORKAROUNDS
# These queries match the official LDBC BI workload patterns
# =============================================================================
BI_QUERIES_ORIGINAL = {
    # BI-1: Posting Summary
    "bi-1a": """
        MATCH (message:Post)
        RETURN count(*) AS totalPosts
    """,
    
    "bi-1b": """
        MATCH (comment:Comment)
        RETURN count(*) AS totalComments
    """,
    
    # BI-2: Tag Evolution
    "bi-2a": """
        MATCH (post:Post)-[:POST_HAS_TAG]->(tag:Tag)
        RETURN tag.name AS tagName, count(post) AS postCount
        ORDER BY postCount DESC
        LIMIT 20
    """,
    
    "bi-2b": """
        MATCH (comment:Comment)-[:COMMENT_HAS_TAG]->(tag:Tag)
        RETURN tag.name AS tagName, count(comment) AS commentCount
        ORDER BY commentCount DESC
        LIMIT 20
    """,
    
    # BI-3: Popular Topics (simplified)
    "bi-3": """
        MATCH (forum:Forum)-[:CONTAINER_OF]->(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
        RETURN forum.title AS forumTitle, tag.name AS tagName, count(*) AS postCount
        ORDER BY postCount DESC
        LIMIT 20
    """,
    
    # BI-4: Top Message Creators
    "bi-4a": """
        MATCH (forum:Forum)-[:HAS_MEMBER]->(person:Person)
        RETURN forum.id AS forumId, forum.title AS forumTitle, count(person) AS memberCount
        ORDER BY memberCount DESC
        LIMIT 20
    """,
    
    "bi-4b": """
        MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)
        RETURN person.id AS personId, person.firstName AS firstName, 
               person.lastName AS lastName, count(post) AS postCount
        ORDER BY postCount DESC
        LIMIT 20
    """,
    
    # BI-5: Most Active Posters
    "bi-5": """
        MATCH (post:Post)-[:HAS_CREATOR]->(person:Person)
        MATCH (post)-[:POST_HAS_TAG]->(tag:Tag)
        RETURN person.id AS personId, person.firstName AS firstName, 
               tag.name AS tagName, count(*) AS postCount
        ORDER BY postCount DESC
        LIMIT 20
    """,
    
    # BI-6: Authoritative Users - ORIGINAL with OPTIONAL MATCH
    # BUG: OPTIONAL MATCH join ordering causes "Unknown identifier" error
    "bi-6": """
        MATCH (post:Post)-[:HAS_CREATOR]->(person:Person)
        OPTIONAL MATCH (post)<-[:LIKES]-(liker:Person)
        RETURN person.id AS personId, person.firstName AS firstName,
               count(DISTINCT liker) AS likerCount
        ORDER BY likerCount DESC
        LIMIT 20
    """,
    
    # BI-7: Related Topics
    "bi-7": """
        MATCH (post:Post)-[:POST_HAS_TAG]->(tag:Tag)
        MATCH (post)<-[:REPLY_OF_POST]-(comment:Comment)-[:COMMENT_HAS_TAG]->(relatedTag:Tag)
        WHERE relatedTag.id <> tag.id
        RETURN tag.name AS originalTag, relatedTag.name AS relatedTag, 
               count(DISTINCT comment) AS commentCount
        ORDER BY commentCount DESC
        LIMIT 20
    """,
    
    # BI-8: Central Person for a Tag
    "bi-8": """
        MATCH (tag:Tag)<-[:HAS_INTEREST]-(person:Person)
        RETURN tag.name AS tagName, count(person) AS interestedCount
        ORDER BY interestedCount DESC
        LIMIT 20
    """,
    
    # BI-9: Top Thread Initiators - ORIGINAL with OPTIONAL MATCH
    # BUG: OPTIONAL MATCH join ordering causes "Unknown identifier" error
    "bi-9": """
        MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)
        OPTIONAL MATCH (post)<-[:REPLY_OF_POST]-(reply:Comment)
        RETURN person.id AS personId, person.firstName AS firstName,
               count(DISTINCT post) AS threadCount, count(DISTINCT reply) AS replyCount
        ORDER BY replyCount DESC
        LIMIT 20
    """,
    
    # BI-10: Experts in Social Circle (parameterized - starts from specific person)
    # NOTE: Original query without WHERE is too expensive on sf10 (scans all 1.7M KNOWS)
    "bi-10": """
        MATCH (person:Person {id: 14})-[:KNOWS*1..2]->(expert:Person)
        MATCH (expert)<-[:HAS_CREATOR]-(post:Post)
        RETURN DISTINCT expert.id AS expertId, expert.firstName AS firstName,
               count(post) AS postCount
        ORDER BY postCount DESC
        LIMIT 20
    """,
    
    # BI-11: Friend Triangles
    "bi-11": """
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(a)
        WHERE a.id < b.id AND b.id < c.id
        RETURN count(*) AS triangleCount
    """,
    
    # BI-12: Message Count Distribution - ORIGINAL with OPTIONAL MATCH and WITH
    # BUG: WITH clause alias not properly resolved in GROUP BY
    "bi-12": """
        MATCH (person:Person)
        OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)
        WITH person, count(post) AS postCount
        RETURN postCount AS messageCount, count(person) AS personCount
        ORDER BY personCount DESC
        LIMIT 20
    """,
    
    # BI-14: International Dialog - ORIGINAL with undirected KNOWS
    # BUG: Undirected relationship pattern causes join ordering issues
    "bi-14": """
        MATCH (person1:Person)-[:KNOWS]-(person2:Person)
        MATCH (person1)-[:IS_LOCATED_IN]->(city1:Place)
        MATCH (person2)-[:IS_LOCATED_IN]->(city2:Place)
        WHERE city1.id <> city2.id
        RETURN person1.id AS person1Id, person2.id AS person2Id,
               city1.name AS city1Name, city2.name AS city2Name
        LIMIT 20
    """,
    
    # BI-18: Friend Recommendation - ORIGINAL with undirected and NOT pattern
    # BUG: Undirected pattern + NOT (anti-join) not supported
    "bi-18": """
        MATCH (person1:Person)-[:KNOWS]-(mutual:Person)-[:KNOWS]-(person2:Person)
        WHERE person1.id <> person2.id AND NOT (person1)-[:KNOWS]-(person2)
        RETURN person1.id AS person1Id, person2.id AS person2Id,
               count(DISTINCT mutual) AS mutualFriendCount
        ORDER BY mutualFriendCount DESC
        LIMIT 20
    """,
    
    # Aggregation queries - these work correctly
    "agg-stats": """
        MATCH (p:Person) RETURN 'Person' AS type, count(*) AS cnt
    """,
    
    "agg-posts": """
        MATCH (p:Post) RETURN count(*) AS totalPosts
    """,
    
    "agg-comments": """
        MATCH (c:Comment) RETURN count(*) AS totalComments
    """,
    
    "agg-forums": """
        MATCH (f:Forum) RETURN count(*) AS totalForums
    """,
    
    "agg-tags": """
        MATCH (t:Tag) RETURN count(*) AS totalTags
    """,
    
    "agg-knows": """
        MATCH (p1:Person)-[:KNOWS]->(p2:Person)
        RETURN count(*) AS totalKnows
    """,
    
    # Geographic distribution
    "geo-dist": """
        MATCH (p:Person)-[:IS_LOCATED_IN]->(city:Place)-[:IS_PART_OF]->(country:Place)
        RETURN country.name AS country, count(p) AS personCount
        ORDER BY personCount DESC
        LIMIT 20
    """,
    
    # Forum activity
    "forum-activity": """
        MATCH (forum:Forum)-[:CONTAINER_OF]->(post:Post)
        RETURN forum.title AS forumTitle, count(post) AS postCount
        ORDER BY postCount DESC
        LIMIT 20
    """,
    
    # Tag class analysis
    "tag-class": """
        MATCH (tag:Tag)-[:HAS_TYPE]->(tc:TagClass)
        RETURN tc.name AS tagClassName, count(tag) AS tagCount
        ORDER BY tagCount DESC
        LIMIT 20
    """,
}

# Known bugs that cause query failures
KNOWN_BUGS = {
    "bi-6": {
        "issue": "OPTIONAL MATCH join ordering",
        "description": "LEFT JOIN generated before table alias is defined",
        "error_pattern": "Unknown expression or function identifier `liker.id`",
        "tracking": "KNOWN_ISSUES.md - OPTIONAL MATCH with relationship patterns"
    },
    "bi-9": {
        "issue": "OPTIONAL MATCH join ordering", 
        "description": "LEFT JOIN generated before table alias is defined",
        "error_pattern": "Unknown expression or function identifier `reply.id`",
        "tracking": "KNOWN_ISSUES.md - OPTIONAL MATCH with relationship patterns"
    },
    "bi-12": {
        "issue": "WITH clause alias resolution",
        "description": "Alias from WITH clause not resolved in subsequent GROUP BY",
        "error_pattern": "Unknown expression identifier `postCount`",
        "tracking": "KNOWN_ISSUES.md - WITH clause aliasing"
    },
    "bi-14": {
        "issue": "Undirected relationship join ordering",
        "description": "Undirected pattern causes table reference before definition",
        "error_pattern": "Unknown expression or function identifier `person2.id`",
        "tracking": "KNOWN_ISSUES.md - Undirected relationship patterns"
    },
    "bi-18": {
        "issue": "NOT pattern (anti-join) not supported",
        "description": "NOT (a)-[:REL]-(b) pattern requires anti-join SQL generation",
        "error_pattern": "NOT pattern not supported or connection error",
        "tracking": "KNOWN_ISSUES.md - Anti-join patterns"
    },
}

# Server URL (can be overridden)
_server_url = CLICKGRAPH_URL

def run_query(query_name: str, query: str, sql_only: bool = False, server_url: str = None) -> QueryResult:
    """Execute a single query against ClickGraph"""
    url = server_url or _server_url
    result = QueryResult(name=query_name, query=query.strip())
    
    try:
        payload = {
            "query": query.strip(),
            "schema_path": SCHEMA_PATH,
            "sql_only": sql_only
        }
        
        start_time = time.time()
        response = requests.post(
            f"{url}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        result.duration_ms = (time.time() - start_time) * 1000
        
        if response.status_code == 200:
            data = response.json()
            if sql_only:
                result.sql = data.get("sql", "")
                result.success = True
            else:
                result.data = data.get("data", [])
                result.sql = data.get("sql", "")
                result.row_count = len(result.data) if result.data else 0
                result.success = True
        else:
            result.error = f"HTTP {response.status_code}: {response.text}"
            
    except requests.exceptions.Timeout:
        result.error = "Query timeout (60s)"
    except requests.exceptions.ConnectionError:
        result.error = f"Cannot connect to ClickGraph at {url}"
    except Exception as e:
        result.error = str(e)
    
    return result


def print_result(result: QueryResult, verbose: bool = False):
    """Print query result in a readable format"""
    status = "✅" if result.success else "❌"
    known_bug = KNOWN_BUGS.get(result.name)
    
    if known_bug and not result.success:
        status = "🐛"  # Known bug
    
    print(f"\n{status} {result.name} ({result.duration_ms:.1f}ms)")
    
    if result.error:
        print(f"   Error: {result.error[:200]}...")
        if known_bug:
            print(f"   Known Bug: {known_bug['issue']}")
        return
    
    if result.sql and verbose:
        print(f"   SQL: {result.sql[:200]}..." if len(result.sql) > 200 else f"   SQL: {result.sql}")
    
    if result.data:
        print(f"   Rows: {result.row_count}")
        for i, row in enumerate(result.data[:3]):
            print(f"   [{i+1}] {row}")
        if result.row_count > 3:
            print(f"   ... and {result.row_count - 3} more rows")


def run_benchmark(queries: Dict[str, str], server_url: str = None) -> Dict[str, Any]:
    """Run all queries and return benchmark summary"""
    results = []
    passed = 0
    failed = 0
    known_bugs_hit = 0
    total_time = 0.0
    
    print("\n" + "="*60)
    print("LDBC SNB BI Benchmark - ORIGINAL QUERIES")
    print("="*60)
    
    for name, query in queries.items():
        result = run_query(name, query, server_url=server_url)
        results.append(result)
        
        if result.success:
            passed += 1
        else:
            failed += 1
            if name in KNOWN_BUGS:
                known_bugs_hit += 1
        total_time += result.duration_ms
        
        print_result(result, verbose=False)
    
    print("\n" + "="*60)
    print("BENCHMARK SUMMARY")
    print("="*60)
    print(f"Total queries: {len(queries)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed} (Known bugs: {known_bugs_hit}, New failures: {failed - known_bugs_hit})")
    print(f"Success rate: {passed/len(queries)*100:.1f}%")
    print(f"Total time: {total_time:.1f}ms")
    print(f"Avg time per query: {total_time/len(queries):.1f}ms")
    
    if known_bugs_hit > 0:
        print("\n" + "-"*60)
        print("KNOWN BUGS HIT:")
        for r in results:
            if not r.success and r.name in KNOWN_BUGS:
                bug = KNOWN_BUGS[r.name]
                print(f"  🐛 {r.name}: {bug['issue']}")
    
    return {
        "total": len(queries),
        "passed": passed,
        "failed": failed,
        "known_bugs_hit": known_bugs_hit,
        "new_failures": failed - known_bugs_hit,
        "success_rate": passed/len(queries)*100,
        "total_time_ms": total_time,
        "results": [
            {
                "name": r.name,
                "success": r.success,
                "duration_ms": r.duration_ms,
                "row_count": r.row_count,
                "error": r.error,
                "known_bug": r.name in KNOWN_BUGS if not r.success else None
            }
            for r in results
        ]
    }


def main():
    parser = argparse.ArgumentParser(description="Run LDBC BI queries (ORIGINAL) against ClickGraph")
    parser.add_argument("--query", "-q", help="Run specific query by name (e.g., bi-6)")
    parser.add_argument("--sql-only", action="store_true", help="Show generated SQL only")
    parser.add_argument("--benchmark", "-b", action="store_true", help="Run full benchmark")
    parser.add_argument("--list", "-l", action="store_true", help="List available queries")
    parser.add_argument("--bugs", action="store_true", help="List known bugs")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--url", default=CLICKGRAPH_URL, help="ClickGraph URL")
    parser.add_argument("--output", "-o", help="Output results to JSON file")
    
    args = parser.parse_args()
    server_url = args.url
    
    if args.list:
        print("Available BI Queries (ORIGINAL):")
        print("-" * 40)
        for name in sorted(BI_QUERIES_ORIGINAL.keys()):
            bug_marker = " 🐛" if name in KNOWN_BUGS else ""
            print(f"  {name}{bug_marker}")
        return
    
    if args.bugs:
        print("Known Bugs in BI Queries:")
        print("-" * 60)
        for name, bug in KNOWN_BUGS.items():
            print(f"\n🐛 {name}: {bug['issue']}")
            print(f"   Description: {bug['description']}")
            print(f"   Error: {bug['error_pattern']}")
        return
    
    if args.query:
        if args.query not in BI_QUERIES_ORIGINAL:
            print(f"Unknown query: {args.query}")
            print(f"Available: {', '.join(sorted(BI_QUERIES_ORIGINAL.keys()))}")
            sys.exit(1)
        
        query = BI_QUERIES_ORIGINAL[args.query]
        result = run_query(args.query, query, sql_only=args.sql_only, server_url=server_url)
        print_result(result, verbose=args.verbose or args.sql_only)
        
        if args.sql_only and result.sql:
            print(f"\nGenerated SQL:\n{result.sql}")
        
        if not result.success and args.query in KNOWN_BUGS:
            bug = KNOWN_BUGS[args.query]
            print(f"\n🐛 This is a KNOWN BUG: {bug['issue']}")
            print(f"   {bug['description']}")
        
        sys.exit(0 if result.success else 1)
    
    # Default: run benchmark
    summary = run_benchmark(BI_QUERIES_ORIGINAL, server_url=server_url)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\nResults saved to {args.output}")
    
    sys.exit(0 if summary["new_failures"] == 0 else 1)


if __name__ == "__main__":
    main()
