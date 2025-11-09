#!/usr/bin/env python3
"""
Test script to reproduce the property mapping issue in multi-variable queries.
"""

import requests
import json

def test_property_mapping():
    """Test property mapping in multi-variable queries."""

    # Start the server if not running
    print("Testing property mapping in multi-variable queries...")
    print("=" * 60)

    # Test query that was failing
    query = """
    MATCH (b:User), (a:User)
    WHERE a.name = "Alice Johnson" AND b.name = "Bob Smith"
    RETURN a.name, b.name
    """

    print(f"Query: {query.strip()}")

    try:
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": query},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            print("[OK] Query executed successfully")
            print(f"Results: {json.dumps(result, indent=2)}")
        else:
            print(f"✗ Query failed with status {response.status_code}")
            print(f"Error: {response.text}")

    except Exception as e:
        print(f"✗ Request failed: {e}")

    print()

    # Test simpler queries to isolate the issue
    print("Testing individual variable queries...")

    # Test just 'a' variable
    query_a = """
    MATCH (a:User)
    WHERE a.name = "Alice Johnson"
    RETURN a.name
    """

    print(f"Query A: {query_a.strip()}")

    try:
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": query_a},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            print("[OK] Query A executed successfully")
            print(f"Results: {json.dumps(result, indent=2)}")
        else:
            print(f"✗ Query A failed with status {response.status_code}")
            print(f"Error: {response.text}")

    except Exception as e:
        print(f"✗ Query A failed: {e}")

    print()

    # Test just 'b' variable
    query_b = """
    MATCH (b:User)
    WHERE b.name = "Bob Smith"
    RETURN b.name
    """

    print(f"Query B: {query_b.strip()}")

    try:
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": query_b},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            print("[OK] Query B executed successfully")
            print(f"Results: {json.dumps(result, indent=2)}")
        else:
            print(f"✗ Query B failed with status {response.status_code}")
            print(f"Error: {response.text}")

    except Exception as e:
        print(f"✗ Query B failed: {e}")

if __name__ == "__main__":
    test_property_mapping()