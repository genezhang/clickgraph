#!/usr/bin/env python3
"""Test basic MATCH pattern without OPTIONAL to verify fundamental JOIN generation."""

import requests
import json

BASE_URL = "http://localhost:8080"

def query(cypher):
    """Execute a Cypher query and return the result."""
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher, "schema": "test_graph_schema"},
        headers={"Content-Type": "application/json"}
    )
    return response

def test_basic_match():
    """Test: MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name"""
    cypher = """
    MATCH (a:User)-[:FOLLOWS]->(b:User)
    RETURN a.name, b.name
    ORDER BY a.name, b.name
    """
    
    print("=" * 80)
    print("TEST: Basic MATCH with relationship traversal")
    print("=" * 80)
    print(f"Query: {cypher.strip()}")
    print()
    
    response = query(cypher)
    
    print(f"Status Code: {response.status_code}")
    print()
    
    if response.status_code == 200:
        result = response.json()
        print("SUCCESS!")
        print(f"Result: {json.dumps(result, indent=2)}")
    else:
        print("FAILED!")
        print(f"Error: {response.text}")
    
    print()

def test_basic_match_single_node():
    """Test: MATCH (a:User) RETURN a.name (simplest possible)"""
    cypher = """
    MATCH (a:User)
    RETURN a.name
    ORDER BY a.name
    """
    
    print("=" * 80)
    print("TEST: Basic MATCH with single node (no relationship)")
    print("=" * 80)
    print(f"Query: {cypher.strip()}")
    print()
    
    response = query(cypher)
    
    print(f"Status Code: {response.status_code}")
    print()
    
    if response.status_code == 200:
        result = response.json()
        print("SUCCESS!")
        print(f"Result: {json.dumps(result, indent=2)}")
    else:
        print("FAILED!")
        print(f"Error: {response.text}")
    
    print()

if __name__ == "__main__":
    # Test simplest case first
    test_basic_match_single_node()
    
    # Then test with relationship
    test_basic_match()
