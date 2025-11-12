#!/usr/bin/env python3
"""
Final Benchmark Test - Clean version without Unicode emojis for Windows
Tests the 10 benchmark queries after all bug fixes
"""
import requests
import json

SERVER_URL = "http://localhost:8080"

BENCHMARK_QUERIES = [
    {
        "name": "simple_node_lookup",
        "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.user_id LIMIT 1"
    },
    {
        "name": "node_filter",
        "query": "MATCH (u:User) WHERE u.user_id < 10 RETURN u.name, u.email LIMIT 5"
    },
    {
        "name": "direct_relationships",
        "query": "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, u2.user_id LIMIT 5"
    },
    {
        "name": "multi_hop",
        "query": "MATCH (u1:User)-[:FOLLOWS]->()-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN DISTINCT u2.name, u2.user_id LIMIT 5"
    },
    {
        "name": "friends_of_friends",
        "query": "MATCH (u:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof:User) WHERE u.user_id = 1 RETURN DISTINCT fof.name, fof.user_id LIMIT 5"
    },
    {
        "name": "variable_length_2",
        "query": "MATCH (u1:User)-[:FOLLOWS*2]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, u2.user_id LIMIT 5"
    },
    {
        "name": "variable_length_range",
        "query": "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name, u2.user_id LIMIT 10"
    },
    {
        "name": "shortest_path",
        "query": "MATCH (u1:User)-[:FOLLOWS*]-(u2:User) WHERE u1.user_id = 1 AND u2.user_id = 10 RETURN u1.name, u2.name, u1.user_id, u2.user_id LIMIT 10"
    },
    {
        "name": "follower_count",
        "query": "MATCH (u:User)<-[:FOLLOWS]-(follower) RETURN u.name, u.user_id, COUNT(follower) as follower_count ORDER BY follower_count DESC LIMIT 5"
    },
    {
        "name": "mutual_follows",
        "query": "MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u1) RETURN u1.name, u2.name, u1.user_id, u2.user_id LIMIT 5"
    },
    # Parameter + Function queries (lightweight addition for feature validation)
    {
        "name": "param_filter_with_function",
        "query": "MATCH (u:User) WHERE u.user_id < $maxId RETURN toUpper(u.name) AS name, u.user_id LIMIT 5",
        "parameters": {"maxId": 100}
    },
    {
        "name": "function_in_aggregation",
        "query": "MATCH (u:User)<-[:FOLLOWS]-(follower) WHERE u.user_id < $threshold RETURN toUpper(u.name) AS name, COUNT(follower) AS count ORDER BY count DESC LIMIT 5",
        "parameters": {"threshold": 1000}
    },
    {
        "name": "math_function_with_param",
        "query": "MATCH (u:User) WHERE abs(u.user_id - $targetId) < $tolerance RETURN u.name, u.user_id, abs(u.user_id - $targetId) AS distance LIMIT 5",
        "parameters": {"targetId": 500, "tolerance": 10}
    },
    {
        "name": "param_in_variable_path",
        "query": "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) WHERE u1.user_id = $startId RETURN u2.name, u2.user_id LIMIT 10",
        "parameters": {"startId": 1}
    }
]

def test_query(query_name, cypher_query, parameters=None):
    """Test a single query"""
    try:
        payload = {"query": cypher_query}
        if parameters:
            payload["parameters"] = parameters
            
        response = requests.post(
            f"{SERVER_URL}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            # Response is directly an array, not wrapped in {"data": [...]}
            if isinstance(result, list):
                count = len(result)
                return True, count, result[0] if count > 0 else None
            else:
                return False, f"Unexpected response format: {type(result)}", None
        else:
            return False, f"HTTP {response.status_code}: {response.text[:200]}", None
    except Exception as e:
        return False, f"Exception: {str(e)[:100]}", None

def main():
    print("=" * 80)
    print("FINAL BENCHMARK TEST - After Bug Fixes #1, #2, and #3")
    print("=" * 80)
    print()
    
    results = []
    passed = 0
    failed = 0
    
    for i, test in enumerate(BENCHMARK_QUERIES, 1):
        name = test["name"]
        query = test["query"]
        parameters = test.get("parameters")
        
        print(f"{i}. {name}")
        print(f"   Query: {query[:60]}..." if len(query) > 60 else f"   Query: {query}")
        if parameters:
            print(f"   Parameters: {parameters}")
        
        success, info, sample = test_query(name, query, parameters)
        
        if success:
            print(f"   [PASS] Returned {info} rows")
            if sample:
                print(f"   Sample: {sample}")
            passed += 1
            results.append(("PASS", name, info))
        else:
            print(f"   [FAIL] {info}")
            failed += 1
            results.append(("FAIL", name, info))
        print()
    
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    for status, name, info in results:
        symbol = "[PASS]" if status == "PASS" else "[FAIL]"
        print(f"{symbol} {name}")
    
    print()
    print(f"Total: {len(BENCHMARK_QUERIES)} queries")
    print(f"Passed: {passed} ({passed/len(BENCHMARK_QUERIES)*100:.1f}%)")
    print(f"Failed: {failed} ({failed/len(BENCHMARK_QUERIES)*100:.1f}%)")
    print("=" * 80)
    
    return passed == len(BENCHMARK_QUERIES)

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
