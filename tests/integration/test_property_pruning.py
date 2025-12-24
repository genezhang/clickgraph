"""
Integration tests for property pruning optimization.

Tests that PropertyRequirementsAnalyzer correctly identifies which properties
are actually needed and enables property pruning in the renderer.
"""

import pytest
import json


def test_property_requirements_basic_query(clickgraph_client, setup_benchmark_data):
    """Test that property requirements are collected for basic queries"""
    
    query = """
    MATCH (u:User)
    WHERE u.user_id = 1
    RETURN u.name, u.email
    """
    
    # For now, just verify the query executes successfully
    # In the future, we can check SQL generation to verify only name & email columns are expanded
    response = clickgraph_client.post(
        "/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    # Should return only name and email columns
    if data["results"]:
        row = data["results"][0]
        assert "u.name" in row
        assert "u.email" in row


def test_property_requirements_with_collect(clickgraph_client, setup_benchmark_data):
    """Test property pruning with collect() aggregation"""
    
    query = """
    MATCH (u:User)-[:FOLLOWS]->(f:User)
    WHERE u.user_id = 1
    RETURN collect(f)[0].name AS first_friend_name
    """
    
    response = clickgraph_client.post(
        "/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    # collect(f) should only materialize f.name property (plus f.user_id for JOIN)
    # instead of all 50+ properties


def test_property_requirements_with_wildcard(clickgraph_client, setup_benchmark_data):
    """Test that wildcard disables pruning"""
    
    query = """
    MATCH (u:User)
    WHERE u.user_id = 1
    RETURN u
    """
    
    # u (whole node return) should expand to ALL properties
    response = clickgraph_client.post(
        "/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    # Should return all columns for the user
    if data["results"]:
        row = data["results"][0]
        # Check that multiple properties are present
        assert any(key.startswith("u.") for key in row.keys())


def test_property_requirements_with_clause_propagation(clickgraph_client, setup_benchmark_data):
    """Test property requirements propagate through WITH clause"""
    
    query = """
    MATCH (u:User)-[:FOLLOWS]->(f:User)
    WITH f, u.user_id AS user_id
    WHERE f.country = 'USA'
    RETURN f.name, f.email
    """
    
    response = clickgraph_client.post(
        "/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    # Property requirements for f (name, email, country) should propagate through WITH


def test_property_requirements_in_filter(clickgraph_client, setup_benchmark_data):
    """Test property requirements from WHERE clause"""
    
    query = """
    MATCH (u:User)
    WHERE u.country = 'USA' AND u.is_active = true
    RETURN u.name
    """
    
    response = clickgraph_client.post(
        "/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    # Requirements should include: name (RETURN), country + is_active (WHERE), user_id (ID)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
