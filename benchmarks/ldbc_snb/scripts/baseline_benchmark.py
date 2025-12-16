#!/usr/bin/env python3
"""
LDBC SNB Baseline Benchmark - Working Queries Only
Tests queries that work with current ClickGraph capabilities
"""
import requests
import json
import time
from datetime import datetime

BASE_URL = "http://localhost:8080/query"

# Use actual IDs from SF10 dataset
QUERIES = {
    # === BASIC QUERIES ===
    "Node Counts - Person": "MATCH (p:Person) RETURN count(p) AS cnt",
    "Node Counts - Post": "MATCH (p:Post) RETURN count(p) AS cnt",
    "Node Counts - Comment": "MATCH (c:Comment) RETURN count(c) AS cnt",
    "Node Counts - Forum": "MATCH (f:Forum) RETURN count(f) AS cnt",
    
    "Edge Counts - KNOWS": "MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt",
    "Edge Counts - HAS_CREATOR": "MATCH ()-[r:HAS_CREATOR]->() RETURN count(r) AS cnt",
    
    # === 1-HOP TRAVERSALS ===
    "IS3 - Person Friends": """
        MATCH (p:Person {id:47})-[r:KNOWS]-(friend:Person)
        RETURN friend.id, friend.firstName, friend.lastName
        ORDER BY friend.id LIMIT 10
    """,
    
    "Person's Posts": """
        MATCH (post:Post)-[:HAS_CREATOR]->(p:Person {id:47})
        RETURN post.id, post.content, post.creationDate
        ORDER BY post.creationDate DESC LIMIT 10
    """,
    
    "Person's Comments": """
        MATCH (comment:Comment)-[:COMMENT_HAS_CREATOR]->(p:Person {id:47})
        RETURN comment.id, comment.content, comment.creationDate
        ORDER BY comment.creationDate DESC LIMIT 10
    """,
    
    # === 2-HOP TRAVERSALS ===
    "Friend of Friend": """
        MATCH (p:Person {id:47})-[:KNOWS]->(f:Person)-[:KNOWS]->(fof:Person)
        WHERE fof.id <> 47
        RETURN fof.id, fof.firstName, count(*) AS commonFriends
        ORDER BY commonFriends DESC, fof.id LIMIT 10
    """,
    
    "Friends' Posts": """
        MATCH (p:Person {id:47})-[:KNOWS]-(friend:Person),
              (post:Post)-[:HAS_CREATOR]->(friend)
        RETURN post.id, friend.firstName, post.content
        ORDER BY post.creationDate DESC LIMIT 10
    """,
    
    # === AGGREGATIONS ===
    "Posts per Person": """
        MATCH (post:Post)-[:HAS_CREATOR]->(person:Person)
        RETURN person.id, person.firstName, count(post) AS postCount
        ORDER BY postCount DESC LIMIT 10
    """,
    
    "Friends per Person": """
        MATCH (p:Person)-[:KNOWS]->()
        RETURN p.id, p.firstName, count(*) AS friendCount
        ORDER BY friendCount DESC LIMIT 10
    """,
    
    "Likes per Post": """
        MATCH (person:Person)-[:LIKES_POST]->(post:Post)
        RETURN post.id, count(person) AS likeCount
        ORDER BY likeCount DESC LIMIT 10
    """,
    
    # === PATTERN MATCHING ===
    "Mutual Friends": """
        MATCH (p1:Person {id:47})-[:KNOWS]->(mutual:Person)<-[:KNOWS]-(p2:Person {id:14})
        RETURN mutual.id, mutual.firstName
        LIMIT 10
    """,
    
    "Posts Tagged": """
        MATCH (post:Post)-[:HAS_TAG]->(tag:Tag)
        WHERE tag.id = 1
        RETURN post.id, post.content
        LIMIT 10
    """,
}

def run_benchmark():
    print("="*80)
    print("LDBC SNB BASELINE BENCHMARK - ClickGraph on SF10")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    print()
    
    results = []
    total_time = 0
    total_rows = 0
    
    for name, query in QUERIES.items():
        try:
            start = time.time()
            resp = requests.post(BASE_URL, json={"query": query}, timeout=30)
            elapsed = (time.time() - start) * 1000
            
            if resp.status_code == 200:
                data = resp.json()
                row_count = len(data.get("results", []))
                print(f"✅ {name:35s} {elapsed:7.1f}ms  {row_count:5d} rows")
                results.append((name, True, elapsed, row_count))
                total_time += elapsed
                total_rows += row_count
            else:
                error = resp.text[:100]
                print(f"❌ {name:35s} HTTP {resp.status_code}: {error}")
                results.append((name, False, elapsed, 0))
        except Exception as e:
            print(f"❌ {name:35s} {str(e)[:80]}")
            results.append((name, False, 0, 0))
    
    print()
    print("="*80)
    print("BENCHMARK RESULTS")
    print("="*80)
    
    passed = sum(1 for _, ok, _, _ in results if ok)
    total = len(results)
    
    print(f"Queries Passed: {passed}/{total} ({passed*100//total}%)")
    
    if passed > 0:
        avg_time = total_time / passed
        print(f"Total Query Time: {total_time:.1f}ms")
        print(f"Average Latency: {avg_time:.1f}ms")
        print(f"Total Rows: {total_rows}")
        print(f"Estimated QPS: {1000/avg_time:.1f}")
        
        # Latency breakdown
        times = [t for _, ok, t, _ in results if ok]
        times.sort()
        print()
        print("Latency Distribution:")
        print(f"  Min: {times[0]:.1f}ms")
        print(f"  Median: {times[len(times)//2]:.1f}ms")
        print(f"  P95: {times[int(len(times)*0.95)]:.1f}ms")
        print(f"  Max: {times[-1]:.1f}ms")

if __name__ == "__main__":
    run_benchmark()
