#!/usr/bin/env python3
"""
Test mixed property expressions with denormalized nodes

Issue: Expressions like s.x + t.y where s and t are both denormalized
need edge context to resolve properties correctly.
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

BASE_URL = f"{CLICKGRAPH_URL}"

def test_denormalized_where_mixed_expression():
    """Test WHERE clause with mixed FROM/TO properties in expression"""
    query = """
    MATCH (s:Airport)-[f:FLIGHT]->(t:Airport)
    WHERE s.x + t.y < 100
    RETURN s.code, t.code
    LIMIT 10
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    # Should not error - even if columns don't exist in schema
    # The resolver should at least attempt to map them
    assert response.status_code in [200, 400], f"Unexpected status: {response.status_code}"

def test_denormalized_return_mixed_expression():
    """Test RETURN with computed expression using FROM/TO properties"""
    query = """
    MATCH (s:Airport)-[f:FLIGHT]->(t:Airport)
    WHERE s.city = 'Los Angeles'
    RETURN s.code, t.code, s.x + t.y AS computed
    LIMIT 10
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

def test_sql_generation_only():
    """Test SQL generation without execution"""
    query = """
    MATCH (s:Airport)-[f:FLIGHT]->(t:Airport)
    WHERE s.x + t.y < 100
    RETURN s.code, t.code, s.a + t.b AS sum
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "sql_only": True},
        headers={"Content-Type": "application/json"}
    )
    
    print(f"\nSQL Generation Test:")
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Generated SQL:\n{data.get('sql', 'N/A')}")
    else:
        print(f"Error: {response.text}")

if __name__ == "__main__":
    print("=" * 60)
    print("Testing Denormalized Mixed Expressions")
    print("=" * 60)
    
    print("\n1. WHERE clause with mixed properties:")
    print("-" * 60)
    test_denormalized_where_mixed_expression()
    
    print("\n2. RETURN with computed expression:")
    print("-" * 60)
    test_denormalized_return_mixed_expression()
    
    print("\n3. SQL generation only:")
    print("-" * 60)
    test_sql_generation_only()
