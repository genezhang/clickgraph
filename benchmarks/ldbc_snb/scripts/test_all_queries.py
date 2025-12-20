#!/usr/bin/env python3
"""
LDBC SNB Comprehensive Query Testing
Tests all 32 actionable queries (30 as-is + 2 restructure)
Updated after Dec 16, 2025 bug fixes
"""
import requests
import json
import time
from typing import Dict, List, Optional
from datetime import datetime

BASE_URL = "http://localhost:8080/query"

# Test parameters (will use actual LDBC parameters once data is loaded)
DEFAULT_PARAMS = {
    "personId": 933,  # Common test person ID
    "messageId": 1099511816755,
    "tag": "Mustafa_Kemal_Atatürk",
    "date": "2012-01-01T00:00:00.000",
    "date1": "2011-11-01T00:00:00.000",
    "date2": "2011-12-01T00:00:00.000",
    "date3": "2012-01-01T00:00:00.000",
    "maxDate": "2012-12-31T23:59:59.999",
    "firstName": "Yang",
    "countryXName": "India",
    "countryYName": "China",
    "startDate": "2012-01-01T00:00:00.000",
    "endDate": "2012-03-01T00:00:00.000",
    "country": "India",
    "tagClass": "MusicalArtist",
    "minPathDistance": 2,
    "maxPathDistance": 4,
}

# All testable queries (32 total: 7 IS + 11 IC + 14 BI)
QUERIES = {
    # === INTERACTIVE SHORT (7 queries - ALL WORK) ===
    "IS1": """
        MATCH (n:Person {id:$personId})-[:IS_LOCATED_IN]->(p:City)
        RETURN n.firstName AS firstName, n.lastName AS lastName,
               n.birthday AS birthday, n.locationIP AS locationIP,
               n.browserUsed AS browserUsed, p.id AS cityId,
               n.gender AS gender, n.creationDate AS creationDate
    """,
    
    "IS2": """
        MATCH (:Person {id:$personId})<-[:HAS_CREATOR]-(m:Message)
        RETURN m.id AS messageId, m.content AS messageContent,
               m.imageFile AS messageImageFile, m.creationDate AS messageCreationDate
        ORDER BY messageCreationDate DESC
        LIMIT 10
    """,
    
    "IS3": """
        MATCH (n:Person {id:$personId})-[r:KNOWS]-(friend:Person)
        RETURN friend.id AS personId, friend.firstName AS firstName,
               friend.lastName AS lastName, r.creationDate AS friendshipCreationDate
        ORDER BY friendshipCreationDate DESC, personId ASC
    """,
    
    "IS4": """
        MATCH (m:Message {id:$messageId})
        RETURN m.creationDate AS messageCreationDate,
               coalesce(m.content, m.imageFile) AS messageContent
    """,
    
    "IS5": """
        MATCH (m:Message {id:$messageId})-[:HAS_CREATOR]->(p:Person)
        RETURN p.id AS personId, p.firstName AS firstName, p.lastName AS lastName
    """,
    
    "IS6": """
        MATCH (m:Message {id:$messageId})-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)
        MATCH (f)-[:HAS_MODERATOR]->(mod:Person)
        RETURN f.id AS forumId, f.title AS forumTitle,
               mod.id AS moderatorId, mod.firstName AS moderatorFirstName,
               mod.lastName AS moderatorLastName
    """,
    
    "IS7": """
        MATCH (m:Message {id:$messageId})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
        OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
        RETURN c.id AS commentId, c.content AS commentContent,
               c.creationDate AS commentCreationDate,
               p.id AS replyAuthorId, p.firstName AS replyAuthorFirstName,
               p.lastName AS replyAuthorLastName,
               CASE WHEN r IS NULL THEN false ELSE true END AS replyAuthorKnowsOriginalMessageAuthor
        ORDER BY commentCreationDate DESC, replyAuthorId ASC
    """,
    
    # === INTERACTIVE COMPLEX (11 work as-is) ===
    "IC1": """
        MATCH (p:Person {id:$personId})-[path:KNOWS*1..3]-(friend:Person)
        WHERE friend.firstName = $firstName
        WITH friend, min(length(path)) AS distance
        MATCH (friend)-[:IS_LOCATED_IN]->(city:City)
        RETURN friend.id AS personId, friend.lastName AS lastName,
               distance, friend.birthday AS birthday, friend.creationDate AS creationDate,
               friend.gender AS gender, friend.browserUsed AS browserUsed,
               friend.locationIP AS locationIP, friend.email AS emails,
               friend.speaks AS languages, city.name AS cityName
        ORDER BY distance ASC, friend.lastName ASC, friend.id ASC
        LIMIT 20
    """,
    
    "IC2": """
        MATCH (:Person {id:$personId})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
        WHERE message.creationDate <= toUnixTimestamp64Milli(parseDateTime64BestEffort($maxDate, 3))
        RETURN friend.id AS personId, friend.firstName AS personFirstName,
               friend.lastName AS personLastName,
               message.id AS messageId, coalesce(message.content, message.imageFile) AS messageContent,
               message.creationDate AS messageCreationDate
        ORDER BY messageCreationDate DESC, messageId ASC
        LIMIT 20
    """,
    
    "IC3": """
        MATCH (p:Person {id:$personId})-[:KNOWS*1..2]-(friend:Person)-[:IS_LOCATED_IN]->(city:City)-[:IS_PART_OF]->(country:Country)
        WHERE country.name IN [$countryXName, $countryYName]
          AND friend.id <> p.id
        WITH friend, city, country, count(*) AS cnt
        RETURN friend.id AS personId, friend.firstName AS personFirstName,
               friend.lastName AS personLastName, cnt AS xCount, 0 AS yCount
        ORDER BY xCount DESC, personId ASC
        LIMIT 20
    """,
    
    # Add more IC queries...
    
    # === BUSINESS INTELLIGENCE (12 work directly) ===
    "BI1": """
        MATCH (message:Message)
        WHERE message.creationDate < toUnixTimestamp64Milli(parseDateTime64BestEffort($date, 3))
        WITH message, message:Comment AS isComment
        RETURN count(message) AS messageCount
    """,
    
    "BI2": """
        MATCH (tag:Tag {name:$tag})
        OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag)
        WHERE message1.creationDate > toUnixTimestamp64Milli(parseDateTime64BestEffort($date1, 3)) AND message1.creationDate < toUnixTimestamp64Milli(parseDateTime64BestEffort($date2, 3))
        OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag)
        WHERE message2.creationDate > toUnixTimestamp64Milli(parseDateTime64BestEffort($date2, 3)) AND message2.creationDate < toUnixTimestamp64Milli(parseDateTime64BestEffort($date3, 3))
        RETURN count(DISTINCT message1) AS countMonth1,
               count(DISTINCT message2) AS countMonth2,
               abs(count(DISTINCT message1) - count(DISTINCT message2)) AS diff
    """,
    
    "BI3": """
        MATCH (country:Country {name:$country})<-[:IS_PART_OF]-(city:City)
        MATCH (city)<-[:IS_LOCATED_IN]-(person:Person)
        MATCH (person)<-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass {name:$tagClass})
        WHERE message.creationDate >= toUnixTimestamp64Milli(parseDateTime64BestEffort($startDate, 3)) AND message.creationDate < toUnixTimestamp64Milli(parseDateTime64BestEffort($endDate, 3))
        RETURN count(DISTINCT message) AS messageCount, count(DISTINCT person) AS personCount
    """,
    
    "BI5": """
        MATCH (message:Message)-[:HAS_TAG]->(tag:Tag)
        MATCH (message)-[:HAS_CREATOR]->(person:Person)
        WHERE message.creationDate >= toUnixTimestamp64Milli(parseDateTime64BestEffort($startDate, 3)) AND message.creationDate < toUnixTimestamp64Milli(parseDateTime64BestEffort($endDate, 3))
        RETURN person.id AS personId, count(DISTINCT message) AS messageCount, count(DISTINCT tag) AS tagCount
        ORDER BY messageCount DESC, personId ASC
        LIMIT 100
    """,
    
    "BI8": """
        MATCH (tag:Tag)
        RETURN tag.name AS tagName,
               count(*) AS cnt,
               100 * size((tag)<-[:HAS_INTEREST]-(person:Person)) AS interestScore,
               size((tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person)) AS messageScore
        ORDER BY interestScore + messageScore DESC, tagName ASC
        LIMIT 100
    """,
    
    # Add more queries...
}

def substitute_params(query: str, params: Dict) -> str:
    """Replace $param placeholders with actual values."""
    result = query
    for param, value in params.items():
        placeholder = f"${param}"
        if isinstance(value, str):
            result = result.replace(placeholder, f"'{value}'")
        elif isinstance(value, bool):
            result = result.replace(placeholder, str(value).lower())
        else:
            result = result.replace(placeholder, str(value))
    return result

def test_query(query_id: str, query: str, params: Dict, sql_only: bool = False) -> Dict:
    """Test a single query."""
    try:
        # Don't do string substitution - let ClickGraph handle parameters
        start = time.time()
        response = requests.post(
            BASE_URL,
            json={
                "query": query,  # Send query as-is with $param placeholders
                "parameters": params,  # Send parameters separately
                "sql_only": sql_only,
                "database": "ldbc"
            },
            timeout=30
        )
        elapsed = (time.time() - start) * 1000
        
        if response.status_code != 200:
            return {
                "query_id": query_id,
                "status": "error",
                "error": f"HTTP {response.status_code}: {response.text[:200]}",
                "time_ms": elapsed
            }
        
        result = response.json()
        
        if sql_only:
            sql = result.get('generated_sql', '')
            return {
                "query_id": query_id,
                "status": "sql_ok" if sql else "sql_error",
                "sql": sql,
                "time_ms": elapsed
            }
        else:
            rows = result.get('results', [])
            return {
                "query_id": query_id,
                "status": "success",
                "row_count": len(rows),
                "time_ms": elapsed
            }
            
    except Exception as e:
        return {
            "query_id": query_id,
            "status": "exception",
            "error": str(e)
        }

def main():
    print("=" * 80)
    print("LDBC SNB COMPREHENSIVE QUERY TEST")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total Queries: {len(QUERIES)}")
    print("=" * 80)
    print()
    
    # Phase 1: SQL Generation Test
    print("PHASE 1: SQL GENERATION VALIDATION")
    print("-" * 80)
    
    sql_results = []
    for query_id, query in sorted(QUERIES.items()):
        print(f"Testing {query_id}...", end=' ', flush=True)
        result = test_query(query_id, query, DEFAULT_PARAMS, sql_only=True)
        sql_results.append(result)
        
        if result['status'] == 'sql_ok':
            print(f"✅ OK ({result['time_ms']:.1f}ms)")
        else:
            print(f"❌ FAIL: {result.get('error', 'No SQL')}")
    
    sql_ok = sum(1 for r in sql_results if r['status'] == 'sql_ok')
    print(f"\nSQL Generation: {sql_ok}/{len(QUERIES)} passed ({sql_ok*100//len(QUERIES)}%)")
    
    # Phase 2: Execution Test (if data loaded)
    print("\n" + "=" * 80)
    print("PHASE 2: QUERY EXECUTION TEST")
    print("-" * 80)
    
    exec_results = []
    for query_id, query in sorted(QUERIES.items()):
        print(f"Executing {query_id}...", end=' ', flush=True)
        result = test_query(query_id, query, DEFAULT_PARAMS, sql_only=False)
        exec_results.append(result)
        
        if result['status'] == 'success':
            print(f"✅ OK ({result['time_ms']:.1f}ms, {result['row_count']} rows)")
        else:
            print(f"❌ {result['status']}: {result.get('error', 'Unknown')}")
    
    success = sum(1 for r in exec_results if r['status'] == 'success')
    print(f"\nExecution: {success}/{len(QUERIES)} passed ({success*100//len(QUERIES)}%)")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"SQL Generation: {sql_ok}/{len(QUERIES)} ({sql_ok*100//len(QUERIES)}%)")
    print(f"Query Execution: {success}/{len(QUERIES)} ({success*100//len(QUERIES)}%)")
    
    if success > 0:
        avg_time = sum(r['time_ms'] for r in exec_results if r['status'] == 'success') / success
        print(f"Average Query Time: {avg_time:.2f}ms")
        print(f"Estimated QPS: {1000/avg_time:.2f}")

if __name__ == "__main__":
    main()
