#!/usr/bin/env python3
"""
LDBC SNB BI Query Runner for ClickGraph

Runs LDBC Business Intelligence queries against ClickGraph and reports results.
Perfect for testing analytical query capabilities with ClickHouse backend.

Usage:
    python run_bi_queries.py                    # Run all queries
    python run_bi_queries.py --query bi-1      # Run specific query
    python run_bi_queries.py --sql-only        # Show SQL without executing
    python run_bi_queries.py --benchmark       # Run with timing
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

# LDBC BI Queries - adapted for ClickGraph
BI_QUERIES = {
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
    
    # BI-6: Authoritative Users (simplified - no OPTIONAL MATCH)
    "bi-6": """
        MATCH (post:Post)-[:HAS_CREATOR]->(person:Person)
        MATCH (post)<-[:LIKES]-(liker:Person)
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
    
    # BI-9: Top Thread Initiators (simplified - no OPTIONAL MATCH)
    "bi-9": """
        MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)
        MATCH (post)<-[:REPLY_OF_POST]-(reply:Comment)
        RETURN person.id AS personId, person.firstName AS firstName,
               count(DISTINCT post) AS threadCount, count(DISTINCT reply) AS replyCount
        ORDER BY replyCount DESC
        LIMIT 20
    """,
    
    # BI-10: Experts in Social Circle (simplified)
    "bi-10": """
        MATCH (person:Person)-[:KNOWS*1..2]->(expert:Person)
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
    
    # BI-12: Message Count Distribution (count posts per author)
    "bi-12": """
        MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)
        RETURN person.id AS personId, count(post) AS messageCount
        ORDER BY messageCount DESC
        LIMIT 20
    """,
    
    # BI-14: International Dialog (simplified - direct knows query)
    "bi-14": """
        MATCH (person1:Person)-[:KNOWS]->(person2:Person)
        MATCH (person1)-[:IS_LOCATED_IN]->(city1:Place)
        MATCH (person2)-[:IS_LOCATED_IN]->(city2:Place)
        WHERE city1.id <> city2.id
        RETURN person1.id AS person1Id, person2.id AS person2Id,
               city1.name AS city1Name, city2.name AS city2Name
        LIMIT 20
    """,
    
    # BI-18: Friend Recommendation (directed pattern)
    "bi-18": """
        MATCH (person1:Person)-[:KNOWS]->(mutual:Person)-[:KNOWS]->(person2:Person)
        WHERE person1.id <> person2.id
        RETURN person1.id AS person1Id, person2.id AS person2Id,
               count(DISTINCT mutual) AS mutualFriendCount
        ORDER BY mutualFriendCount DESC
        LIMIT 20
    """,
    
    # Aggregation queries - ClickHouse strengths
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
    print(f"\n{status} {result.name} ({result.duration_ms:.1f}ms)")
    
    if result.error:
        print(f"   Error: {result.error}")
        return
    
    if result.sql and verbose:
        print(f"   SQL: {result.sql[:200]}..." if len(result.sql) > 200 else f"   SQL: {result.sql}")
    
    if result.data:
        print(f"   Rows: {result.row_count}")
        # Show first few results
        for i, row in enumerate(result.data[:5]):
            print(f"   [{i+1}] {row}")
        if result.row_count > 5:
            print(f"   ... and {result.row_count - 5} more rows")


def run_benchmark(queries: Dict[str, str], server_url: str = None) -> Dict[str, Any]:
    """Run all queries and return benchmark summary"""
    results = []
    passed = 0
    failed = 0
    total_time = 0.0
    
    print("\n" + "="*60)
    print("LDBC SNB BI Benchmark - ClickGraph")
    print("="*60)
    
    for name, query in queries.items():
        result = run_query(name, query, server_url=server_url)
        results.append(result)
        
        if result.success:
            passed += 1
        else:
            failed += 1
        total_time += result.duration_ms
        
        print_result(result, verbose=False)
    
    print("\n" + "="*60)
    print("BENCHMARK SUMMARY")
    print("="*60)
    print(f"Total queries: {len(queries)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Success rate: {passed/len(queries)*100:.1f}%")
    print(f"Total time: {total_time:.1f}ms")
    print(f"Avg time per query: {total_time/len(queries):.1f}ms")
    
    return {
        "total": len(queries),
        "passed": passed,
        "failed": failed,
        "success_rate": passed/len(queries)*100,
        "total_time_ms": total_time,
        "results": [
            {
                "name": r.name,
                "success": r.success,
                "duration_ms": r.duration_ms,
                "row_count": r.row_count,
                "error": r.error
            }
            for r in results
        ]
    }


def main():
    parser = argparse.ArgumentParser(description="Run LDBC BI queries against ClickGraph")
    parser.add_argument("--query", "-q", help="Run specific query by name (e.g., bi-1a)")
    parser.add_argument("--sql-only", action="store_true", help="Show generated SQL only")
    parser.add_argument("--benchmark", "-b", action="store_true", help="Run full benchmark")
    parser.add_argument("--list", "-l", action="store_true", help="List available queries")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--url", default=CLICKGRAPH_URL, help="ClickGraph URL")
    parser.add_argument("--output", "-o", help="Output results to JSON file")
    
    args = parser.parse_args()
    
    server_url = args.url
    
    if args.list:
        print("Available BI Queries:")
        print("-" * 40)
        for name in sorted(BI_QUERIES.keys()):
            print(f"  {name}")
        return
    
    if args.query:
        if args.query not in BI_QUERIES:
            print(f"Unknown query: {args.query}")
            print(f"Available: {', '.join(sorted(BI_QUERIES.keys()))}")
            sys.exit(1)
        
        query = BI_QUERIES[args.query]
        result = run_query(args.query, query, sql_only=args.sql_only, server_url=server_url)
        print_result(result, verbose=args.verbose or args.sql_only)
        
        if args.sql_only and result.sql:
            print(f"\nGenerated SQL:\n{result.sql}")
        
        sys.exit(0 if result.success else 1)
    
    # Default: run benchmark
    summary = run_benchmark(BI_QUERIES, server_url=server_url)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\nResults saved to {args.output}")
    
    sys.exit(0 if summary["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
