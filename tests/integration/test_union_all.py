"""
Test top-level UNION ALL queries with separate RETURN clauses
"""
import pytest
import requests
import json

BASE_URL = "http://localhost:8080"

def test_union_all_simple():
    """Test simple UNION ALL with two queries"""
    query = """
    MATCH (u:User) WHERE u.user_id = 1
    RETURN u.name AS name
    UNION ALL
    MATCH (u:User) WHERE u.user_id = 2
    RETURN u.name AS name
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "schema_name": "social_benchmark"}
    )
    
    assert response.status_code == 200, f"Failed: {response.text}"
    data = response.json()
    print(json.dumps(data, indent=2))
    
    # Response format: {"results": [...]}
    assert "results" in data
    assert len(data["results"]) == 2

def test_union_all_with_distinct_and_limit():
    """Test UNION ALL where each branch has DISTINCT and LIMIT"""
    query = """
    MATCH (n:User) WHERE n.user_id < 5
    RETURN DISTINCT "node" as entity, n.name AS name LIMIT 2
    UNION ALL
    MATCH (p:Post) WHERE p.post_id < 5
    RETURN DISTINCT "post" AS entity, p.content AS name LIMIT 2
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "schema_name": "social_benchmark"}
    )
    
    assert response.status_code == 200, f"Failed: {response.text}"
    data = response.json()
    print(json.dumps(data, indent=2))
    
    # Should return 2-4 rows depending on data (at least 2 users)
    assert len(data["results"]) >= 2

def test_union_all_nodes_and_relationships():
    """Test the Neodash use case: nodes with property UNION ALL relationships with property"""
    query = """
    MATCH (n:User) WHERE n.user_id = 1
    RETURN DISTINCT "node" as entity, n.name AS name
    UNION ALL
    MATCH (u1:User)-[r:FOLLOWS]->(u2:User) WHERE u1.user_id = 1
    RETURN DISTINCT "relationship" AS entity, concat(u1.name, ' follows ', u2.name) AS name
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "schema_name": "social_benchmark"}
    )
    
    assert response.status_code == 200, f"Failed: {response.text}"
    data = response.json()
    print(json.dumps(data, indent=2))
    
    # Should return at least 2 rows (1 node + at least 1 relationship)
    assert len(data["results"]) >= 2

if __name__ == "__main__":
    # Run tests
    print("Test 1: Simple UNION ALL")
    test_union_all_simple()
    print("✅ PASSED\n")
    
    print("Test 2: UNION ALL with DISTINCT and LIMIT")
    test_union_all_with_distinct_and_limit()
    print("✅ PASSED\n")
    
    print("Test 3: Nodes and Relationships UNION")
    test_union_all_nodes_and_relationships()
    print("✅ PASSED\n")
    
    print("🎉 All tests passed!")
