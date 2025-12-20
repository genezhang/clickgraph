"""
Test collect(node) + UNWIND pattern with tuple property mapping

These tests verify that after collecting nodes and unwinding them,
property access works correctly using tuple indices.
"""

import pytest
import requests

BASE_URL = "http://localhost:8080"


def execute_query(query: str):
    """Execute a Cypher query against ClickGraph"""
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query},
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json()


def test_collect_unwind_single_property():
    """Test basic collect + UNWIND with single property access"""
    query = """
    MATCH (u:User)
    WITH u, collect(u) as users
    UNWIND users as user
    RETURN user.name
    LIMIT 3
    """
    result = execute_query(query)
    
    assert "results" in result
    assert len(result["results"]) == 3
    assert all("user.name" in row for row in result["results"])
    # Verify we got actual names, not nulls
    assert all(row["user.name"] is not None for row in result["results"])


def test_collect_unwind_multiple_properties():
    """Test collect + UNWIND with multiple property access"""
    query = """
    MATCH (u:User)
    WITH u, collect(u) as users
    UNWIND users as user
    RETURN user.name, user.email, user.city
    LIMIT 3
    """
    result = execute_query(query)
    
    assert "results" in result
    assert len(result["results"]) == 3
    
    for row in result["results"]:
        assert "user.name" in row
        assert "user.email" in row
        assert "user.city" in row
        # Verify all properties have values
        assert row["user.name"] is not None
        assert row["user.email"] is not None
        assert row["user.city"] is not None


def test_collect_unwind_with_ordering():
    """Test collect + UNWIND with explicit ordering"""
    query = """
    MATCH (u:User)
    WITH u, collect(u) as users
    UNWIND users as user
    RETURN user.name, user.city
    ORDER BY user.city
    LIMIT 3
    """
    result = execute_query(query)
    
    assert "results" in result
    assert len(result["results"]) == 3
    assert all("user.name" in row for row in result["results"])
    assert all("user.city" in row for row in result["results"])
    
    # Verify ordering (cities should be sorted)
    cities = [row["user.city"] for row in result["results"]]
    assert cities == sorted(cities), "Results should be ordered by city"


def test_collect_unwind_with_aggregate():
    """Test collect + UNWIND combined with aggregation"""
    query = """
    MATCH (u:User)
    WITH u, collect(u) as users
    UNWIND users as user
    RETURN user.country, count(*) as user_count
    ORDER BY user_count DESC
    LIMIT 3
    """
    result = execute_query(query)
    
    assert "results" in result
    assert len(result["results"]) > 0
    
    for row in result["results"]:
        assert "user.country" in row
        assert "user_count" in row
        assert isinstance(row["user_count"], int)
        assert row["user_count"] > 0


def test_collect_unwind_distinct_properties():
    """Test collect + UNWIND to get distinct property values"""
    query = """
    MATCH (u:User)
    WITH u, collect(u) as users
    UNWIND users as user
    RETURN DISTINCT user.city
    ORDER BY user.city
    """
    result = execute_query(query)
    
    assert "results" in result
    # Check that we get distinct cities
    cities = [row["user.city"] for row in result["results"]]
    assert len(cities) == len(set(cities)), "Cities should be distinct"
    # Check alphabetical ordering
    assert cities == sorted(cities), "Cities should be ordered"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
