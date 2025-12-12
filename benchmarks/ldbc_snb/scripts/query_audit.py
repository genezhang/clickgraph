#!/usr/bin/env python3
"""
LDBC SNB Query Audit Script for ClickGraph

This script:
1. Tests each query for SQL generation correctness
2. Executes the query and validates results
3. Measures execution time
4. Compares results against expected outputs
"""

import json
import requests
import time
import sys
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime

CLICKGRAPH_URL = "http://localhost:8080/query"
GRAPH_NAME = "ldbc_snb"

@dataclass
class QueryResult:
    query_name: str
    cypher: str
    sql: Optional[str]
    success: bool
    error: Optional[str]
    row_count: int
    execution_time_ms: float
    results: Optional[List[Dict]]

def run_query(name: str, cypher: str, params: Dict[str, Any] = None, 
              sql_only: bool = False, show_sql: bool = True) -> QueryResult:
    """Execute a Cypher query and return results."""
    
    # Replace parameters in query
    query = cypher
    if params:
        for key, value in params.items():
            placeholder = f"${key}"
            if isinstance(value, str):
                replacement = f"'{value}'"
            else:
                replacement = str(value)
            query = query.replace(placeholder, replacement)
    
    payload = {
        "graph_name": GRAPH_NAME,
        "query": query,
        "sql_only": sql_only
    }
    
    start_time = time.time()
    
    try:
        response = requests.post(CLICKGRAPH_URL, json=payload, timeout=60)
        elapsed_ms = (time.time() - start_time) * 1000
        
        # Parse response - may have debug output mixed in
        response_text = response.text
        
        # Find the last complete JSON object in the response
        # This handles cases where debug output is mixed with JSON
        json_text = None
        
        # Try to find JSON that starts with {"results or {"sql_query or {"error
        for marker in ['{"results":', '{"sql_query":', '{"generated_sql":', '{"error":']:
            idx = response_text.rfind(marker)
            if idx >= 0:
                # Find the matching closing brace
                candidate = response_text[idx:]
                # Try to parse it
                try:
                    data = json.loads(candidate)
                    json_text = candidate
                    break
                except json.JSONDecodeError:
                    # Try to find just the JSON part (up to next newline with non-JSON)
                    pass
        
        if json_text is None:
            # Fallback: try parsing the whole response
            data = response.json()
        
        if sql_only:
            sql = data.get("sql_query") or data.get("generated_sql", "")
            if "PLANNING_ERROR" in sql:
                return QueryResult(
                    query_name=name,
                    cypher=query,
                    sql=sql,
                    success=False,
                    error=sql,
                    row_count=0,
                    execution_time_ms=elapsed_ms,
                    results=None
                )
            return QueryResult(
                query_name=name,
                cypher=query,
                sql=sql,
                success=True,
                error=None,
                row_count=0,
                execution_time_ms=elapsed_ms,
                results=None
            )
        else:
            results = data.get("results", [])
            error = data.get("error")
            
            return QueryResult(
                query_name=name,
                cypher=query,
                sql=None,
                success=error is None,
                error=error,
                row_count=len(results),
                execution_time_ms=elapsed_ms,
                results=results[:5] if results else None  # Keep first 5 for display
            )
            
    except requests.exceptions.ConnectionError:
        return QueryResult(
            query_name=name,
            cypher=query,
            sql=None,
            success=False,
            error="Connection failed - is ClickGraph server running?",
            row_count=0,
            execution_time_ms=0,
            results=None
        )
    except Exception as e:
        return QueryResult(
            query_name=name,
            cypher=query,
            sql=None,
            success=False,
            error=str(e),
            row_count=0,
            execution_time_ms=0,
            results=None
        )

# =============================================================================
# Interactive Short Queries (IS1-IS7)
# =============================================================================

IS_QUERIES = {
    "IS1": {
        "name": "Person Profile",
        "description": "Get person profile with city",
        "cypher": """
MATCH (n:Person)-[:IS_LOCATED_IN]->(city:City)
WHERE n.id = $personId
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.birthday AS birthday,
    n.locationIP AS locationIP,
    n.browserUsed AS browserUsed,
    city.name AS cityName,
    n.gender AS gender
""",
        "params": {"personId": 14}
    },
    "IS2": {
        "name": "Recent Posts",
        "description": "Get recent posts by a person",
        "cypher": """
MATCH (p:Person)<-[:HAS_CREATOR]-(post:Post)
WHERE p.id = $personId
RETURN
    post.id AS messageId,
    post.content AS messageContent,
    post.creationDate AS messageCreationDate
ORDER BY post.creationDate DESC
LIMIT 10
""",
        "params": {"personId": 14}
    },
    "IS3": {
        "name": "Friends",
        "description": "Get friends of a person with relationship date",
        "cypher": """
MATCH (n:Person)-[r:KNOWS]-(friend:Person)
WHERE n.id = $personId
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY r.creationDate DESC, friend.id ASC
LIMIT 10
""",
        "params": {"personId": 14}
    },
    "IS4": {
        "name": "Message Content",
        "description": "Get content of a message",
        "cypher": """
MATCH (m:Post)
WHERE m.id = $messageId
RETURN m.content AS content, m.creationDate AS creationDate
""",
        "params": {"messageId": 8796099313749}
    },
    "IS5": {
        "name": "Message Creator",
        "description": "Get creator of a message",
        "cypher": """
MATCH (post:Post)-[:HAS_CREATOR]->(p:Person)
WHERE post.id = $messageId
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName
""",
        "params": {"messageId": 8796099313749}
    },
    "IS6": {
        "name": "Forum of Message",
        "description": "Get forum containing a post",
        "cypher": """
MATCH (post:Post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE post.id = $messageId
RETURN
    forum.id AS forumId,
    forum.title AS forumTitle
""",
        "params": {"messageId": 8796099313749}
    },
    "IS7": {
        "name": "Replies to Message",
        "description": "Get replies to a post with authors",
        "cypher": """
MATCH (post:Post)<-[:REPLY_OF_POST]-(comment:Comment)-[:COMMENT_HAS_CREATOR]->(p:Person)
WHERE post.id = $messageId
RETURN
    comment.id AS commentId,
    comment.content AS commentContent,
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName
ORDER BY comment.creationDate DESC
LIMIT 10
""",
        "params": {"messageId": 8796099313749}
    }
}

# =============================================================================
# Interactive Complex Queries (IC1-IC14)
# Official LDBC SNB queries, some simplified for ClickGraph compatibility
# =============================================================================

IC_QUERIES = {
    "IC1": {
        "name": "Friends with Name (Variable Path)",
        "description": "Find friends within 3 hops with given first name",
        "cypher": """
MATCH (p:Person)-[:KNOWS*1..3]-(friend:Person)
WHERE p.id = $personId AND friend.firstName = $firstName AND friend.id <> $personId
RETURN DISTINCT
    friend.id AS friendId,
    friend.firstName AS friendFirstName,
    friend.lastName AS friendLastName
ORDER BY friend.lastName ASC, friend.id ASC
LIMIT 20
""",
        "params": {"personId": 14, "firstName": "Ali"}
    },
    "IC2": {
        "name": "Recent Friend Posts",
        "description": "Get recent posts from friends",
        "cypher": """
MATCH (p:Person)-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)
WHERE p.id = $personId AND post.creationDate <= $maxDate
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    post.id AS postId,
    post.content AS postContent,
    post.creationDate AS postCreationDate
ORDER BY postCreationDate DESC, post.id ASC
LIMIT 20
""",
        "params": {"personId": 14, "maxDate": 1354320000000}
    },
    "IC3": {
        "name": "Friends in Countries (Simplified)",
        "description": "Friends who posted in specific country",
        "cypher": """
MATCH (person:Person)-[:KNOWS]->(friend:Person)<-[:HAS_CREATOR]-(post:Post)-[:POST_IS_LOCATED_IN]->(country:Country)
WHERE person.id = $personId AND country.name = $countryName
RETURN
    friend.id AS friendId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    count(post) AS postCount
ORDER BY postCount DESC, friendId ASC
LIMIT 20
""",
        "params": {"personId": 14, "countryName": "China"}
    },
    "IC4": {
        "name": "New Topics (Simplified)",
        "description": "Tags friends posted about in time window",
        "cypher": """
MATCH (person:Person)-[:KNOWS]->(friend:Person)<-[:HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
WHERE person.id = $personId 
  AND post.creationDate >= $startDate 
  AND post.creationDate < $endDate
RETURN tag.name AS tagName, count(post) AS postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10
""",
        "params": {"personId": 14, "startDate": 1275350400000, "endDate": 1277856000000}
    },
    "IC5": {
        "name": "New Groups (Simplified)",
        "description": "Forums friends belong to",
        "cypher": """
MATCH (person:Person)-[:KNOWS]->(friend:Person)<-[:HAS_MEMBER]-(forum:Forum)
WHERE person.id = $personId
RETURN
    forum.id AS forumId,
    forum.title AS forumTitle,
    count(DISTINCT friend) AS memberCount
ORDER BY memberCount DESC, forum.id ASC
LIMIT 20
""",
        "params": {"personId": 14}
    },
    "IC7": {
        "name": "Recent Likers (Simplified)",
        "description": "People who liked person's posts",
        "cypher": """
MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:LIKES_POST]-(liker:Person)
WHERE person.id = $personId
RETURN DISTINCT
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    count(*) AS likeCount
ORDER BY likeCount DESC, personId ASC
LIMIT 20
""",
        "params": {"personId": 14}
    },
    "IC8": {
        "name": "Recent Replies",
        "description": "Replies to person's posts",
        "cypher": """
MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF_POST]-(comment:Comment)-[:COMMENT_HAS_CREATOR]->(author:Person)
WHERE person.id = $personId
RETURN
    author.id AS personId,
    author.firstName AS personFirstName,
    author.lastName AS personLastName,
    comment.id AS commentId,
    comment.content AS commentContent,
    comment.creationDate AS commentCreationDate
ORDER BY commentCreationDate DESC, comment.id ASC
LIMIT 20
""",
        "params": {"personId": 14}
    },
    "IC9": {
        "name": "FoF Posts (Simplified)",
        "description": "Recent posts from friends and friends-of-friends",
        "cypher": """
MATCH (root:Person)-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)
WHERE root.id = $personId AND friend.id <> $personId AND post.creationDate < $maxDate
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    post.id AS postId,
    post.content AS postContent
ORDER BY post.creationDate DESC, post.id ASC
LIMIT 20
""",
        "params": {"personId": 14, "maxDate": 1354320000000}
    },
    "IC11": {
        "name": "Job Referral (Simplified)",
        "description": "Friends working at companies in specific country",
        "cypher": """
MATCH (person:Person)-[:KNOWS]-(friend:Person)-[:WORK_AT]->(company:Organisation)-[:ORG_IS_LOCATED_IN]->(place:Place)
WHERE person.id = $personId AND place.name = $countryName
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    company.name AS organizationName
ORDER BY personId ASC, company.name DESC
LIMIT 10
""",
        "params": {"personId": 14, "countryName": "China"}
    },
    "IC12": {
        "name": "Expert Search (Simplified)",
        "description": "Friends who commented on posts with specific tag",
        "cypher": """
MATCH (person:Person)-[:KNOWS]->(friend:Person)<-[:COMMENT_HAS_CREATOR]-(comment:Comment)-[:REPLY_OF_POST]->(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
WHERE person.id = $personId AND tag.name = $tagName
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    count(comment) AS replyCount
ORDER BY replyCount DESC, personId ASC
LIMIT 20
""",
        "params": {"personId": 14, "tagName": "China"}
    },
    "IC13": {
        "name": "Path Between Two People",
        "description": "Check if path exists between two people (simplified)",
        "cypher": """
MATCH (person1:Person)-[:KNOWS*1..4]-(person2:Person)
WHERE person1.id = $person1Id AND person2.id = $person2Id
RETURN DISTINCT person2.id AS connectedPersonId
LIMIT 1
""",
        "params": {"person1Id": 14, "person2Id": 14466}
    }
}

# =============================================================================
# Business Intelligence Queries (BI)
# =============================================================================

BI_QUERIES = {
    "BI1": {
        "name": "Post Count",
        "description": "Total message count",
        "cypher": """
MATCH (post:Post)
RETURN count(*) AS messageCount
"""
    },
    "BI2": {
        "name": "Posts per Tag",
        "description": "Count posts by tag",
        "cypher": """
MATCH (post:Post)-[:POST_HAS_TAG]->(tag:Tag)
RETURN 
    tag.name AS tagName,
    count(post) AS postCount
ORDER BY postCount DESC
LIMIT 20
"""
    },
    "BI3": {
        "name": "Forum Members",
        "description": "Count forum members",
        "cypher": """
MATCH (forum:Forum)-[:HAS_MEMBER]->(person:Person)
RETURN 
    forum.id AS forumId,
    forum.title AS forumTitle,
    count(person) AS memberCount
ORDER BY memberCount DESC
LIMIT 20
"""
    },
    "BI4": {
        "name": "Top Content Creators",
        "description": "Most active post creators",
        "cypher": """
MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)
RETURN 
    person.id AS personId,
    person.firstName AS firstName,
    person.lastName AS lastName,
    count(post) AS postCount
ORDER BY postCount DESC, person.id ASC
LIMIT 20
"""
    },
    "BI5": {
        "name": "Tag Engagement",
        "description": "Posts about specific tag",
        "cypher": """
MATCH (tag:Tag)<-[:POST_HAS_TAG]-(post:Post)-[:HAS_CREATOR]->(person:Person)
WHERE tag.name = $tagName
RETURN 
    person.id AS personId,
    person.firstName AS firstName,
    person.lastName AS lastName,
    count(post) AS postCount
ORDER BY postCount DESC, person.id ASC
LIMIT 20
""",
        "params": {"tagName": "Wolfgang_Amadeus_Mozart"}
    },
    "BI6": {
        "name": "Related Tags",
        "description": "Tags related through comments",
        "cypher": """
MATCH (tag:Tag)<-[:POST_HAS_TAG]-(post:Post)
WHERE tag.name = $tagName
MATCH (post)<-[:REPLY_OF_POST]-(comment:Comment)-[:COMMENT_HAS_TAG]->(relatedTag:Tag)
WHERE relatedTag.id <> tag.id
RETURN 
    relatedTag.name AS relatedTagName,
    count(DISTINCT comment) AS commentCount
ORDER BY commentCount DESC, relatedTagName
LIMIT 20
""",
        "params": {"tagName": "Enrique_Iglesias"}
    }
}

def print_separator():
    print("=" * 80)

def print_result(result: QueryResult, show_sql: bool = True, verbose: bool = False):
    """Print query result in a formatted way."""
    status = "✅ PASS" if result.success else "❌ FAIL"
    
    print(f"\n{status} {result.query_name}")
    print(f"   Time: {result.execution_time_ms:.1f}ms | Rows: {result.row_count}")
    
    if result.error:
        print(f"   Error: {result.error[:100]}")
    
    if verbose and result.results:
        print(f"   Sample: {json.dumps(result.results[0], indent=2)[:200]}...")

def run_query_set(queries: Dict, name: str, verbose: bool = False):
    """Run a set of queries and return results."""
    print_separator()
    print(f"Running {name} Queries")
    print_separator()
    
    results = []
    passed = 0
    failed = 0
    
    for query_id, query_def in queries.items():
        params = query_def.get("params", {})
        result = run_query(
            f"{query_id}: {query_def['name']}",
            query_def["cypher"],
            params
        )
        results.append(result)
        
        print_result(result, verbose=verbose)
        
        if result.success:
            passed += 1
        else:
            failed += 1
    
    print(f"\n{name} Summary: {passed}/{len(queries)} passed, {failed} failed")
    return results


def run_sql_audit(queries: Dict, name: str):
    """Audit SQL generation for queries."""
    print_separator()
    print(f"SQL Audit: {name} Queries")
    print_separator()
    
    for query_id, query_def in queries.items():
        params = query_def.get("params", {})
        result = run_query(
            f"{query_id}: {query_def['name']}",
            query_def["cypher"],
            params,
            sql_only=True
        )
        
        status = "✅" if result.success else "❌"
        print(f"\n{status} {result.query_name}")
        
        if result.sql:
            # Clean up SQL for display
            sql_lines = result.sql.strip().split('\n')
            sql_preview = '\n'.join(sql_lines[:10])
            if len(sql_lines) > 10:
                sql_preview += f"\n   ... ({len(sql_lines) - 10} more lines)"
            print(f"   SQL:\n{sql_preview}")


def run_benchmark(queries: Dict, name: str, iterations: int = 3):
    """Run queries multiple times to get performance statistics."""
    print_separator()
    print(f"Benchmark: {name} Queries ({iterations} iterations)")
    print_separator()
    
    benchmark_results = []
    
    for query_id, query_def in queries.items():
        params = query_def.get("params", {})
        times = []
        last_result = None
        
        # Warmup run
        _ = run_query(f"{query_id}", query_def["cypher"], params, show_sql=False)
        
        # Timed runs
        for i in range(iterations):
            result = run_query(
                f"{query_id}: {query_def['name']}",
                query_def["cypher"],
                params,
                show_sql=False
            )
            if result.success:
                times.append(result.execution_time_ms)
            last_result = result
        
        if times:
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            print(f"✅ {query_id}: {query_def['name']}")
            print(f"   Avg: {avg_time:.1f}ms | Min: {min_time:.1f}ms | Max: {max_time:.1f}ms | Rows: {last_result.row_count}")
            benchmark_results.append({
                "query_id": query_id,
                "name": query_def['name'],
                "avg_ms": avg_time,
                "min_ms": min_time,
                "max_ms": max_time,
                "row_count": last_result.row_count
            })
        else:
            print(f"❌ {query_id}: {query_def['name']} - All iterations failed")
            benchmark_results.append({
                "query_id": query_id,
                "name": query_def['name'],
                "error": last_result.error if last_result else "Unknown error"
            })
    
    return benchmark_results


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="LDBC Query Audit")
    parser.add_argument("--is", dest="run_is", action="store_true", help="Run IS queries")
    parser.add_argument("--ic", dest="run_ic", action="store_true", help="Run IC queries")
    parser.add_argument("--bi", dest="run_bi", action="store_true", help="Run BI queries")
    parser.add_argument("--all", action="store_true", help="Run all queries")
    parser.add_argument("--sql", action="store_true", help="Show SQL only (no execution)")
    parser.add_argument("--benchmark", action="store_true", help="Run performance benchmark")
    parser.add_argument("--iterations", type=int, default=3, help="Benchmark iterations (default: 3)")
    parser.add_argument("--output", "-o", type=str, help="Output JSON file for benchmark results")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    # Default to all if nothing specified
    if not (args.run_is or args.run_ic or args.run_bi or args.all):
        args.all = True
    
    print(f"\nLDBC SNB Query Audit - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"ClickGraph URL: {CLICKGRAPH_URL}")
    print(f"Graph: {GRAPH_NAME}\n")
    
    all_results = []
    benchmark_data = {}
    
    if args.benchmark:
        # Performance benchmark mode
        if args.run_is or args.all:
            benchmark_data["IS"] = run_benchmark(IS_QUERIES, "Interactive Short", args.iterations)
        if args.run_ic or args.all:
            benchmark_data["IC"] = run_benchmark(IC_QUERIES, "Interactive Complex", args.iterations)
        if args.run_bi or args.all:
            benchmark_data["BI"] = run_benchmark(BI_QUERIES, "Business Intelligence", args.iterations)
        
        # Calculate totals
        all_benchmarks = []
        for category in benchmark_data.values():
            all_benchmarks.extend(category)
        
        total_queries = len(all_benchmarks)
        successful = sum(1 for b in all_benchmarks if "avg_ms" in b)
        avg_time = sum(b.get("avg_ms", 0) for b in all_benchmarks if "avg_ms" in b) / successful if successful > 0 else 0
        
        print_separator()
        print(f"\nBENCHMARK SUMMARY: {successful}/{total_queries} queries successful")
        print(f"Average execution time: {avg_time:.1f}ms")
        
        if args.output:
            import json
            output_data = {
                "timestamp": datetime.now().isoformat(),
                "iterations": args.iterations,
                "results": benchmark_data
            }
            with open(args.output, 'w') as f:
                json.dump(output_data, f, indent=2)
            print(f"\nResults saved to: {args.output}")
    
    elif args.sql:
        # SQL-only audit
        if args.run_is or args.all:
            run_sql_audit(IS_QUERIES, "Interactive Short")
        if args.run_ic or args.all:
            run_sql_audit(IC_QUERIES, "Interactive Complex")
        if args.run_bi or args.all:
            run_sql_audit(BI_QUERIES, "Business Intelligence")
    else:
        # Full execution
        if args.run_is or args.all:
            all_results.extend(run_query_set(IS_QUERIES, "Interactive Short", args.verbose))
        if args.run_ic or args.all:
            all_results.extend(run_query_set(IC_QUERIES, "Interactive Complex", args.verbose))
        if args.run_bi or args.all:
            all_results.extend(run_query_set(BI_QUERIES, "Business Intelligence", args.verbose))
        
        # Final summary
        print_separator()
        total = len(all_results)
        passed = sum(1 for r in all_results if r.success)
        failed = total - passed
        avg_time = sum(r.execution_time_ms for r in all_results) / total if total > 0 else 0
        
        print(f"\nOVERALL RESULTS: {passed}/{total} queries passed ({100*passed/total:.1f}%)")
        print(f"Average execution time: {avg_time:.1f}ms")
        
        if failed > 0:
            print("\nFailed queries:")
            for r in all_results:
                if not r.success:
                    print(f"  - {r.query_name}: {r.error[:80] if r.error else 'Unknown error'}")

if __name__ == "__main__":
    main()
