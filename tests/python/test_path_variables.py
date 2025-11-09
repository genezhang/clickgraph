#!/usr/bin/env python3
"""
Test script for Path Variables feature
Tests MATCH p = (a)-[r*]->(b) RETURN p, length(p), nodes(p), relationships(p)
"""

import requests
import json
import sys

def test_path_variables():
    """Test path variables with variable-length patterns"""

    # Test query with path variables and functions
    query = """
    MATCH p = (u1)-[r:FOLLOWS*1..3]->(u2)
    RETURN p, length(p) as path_length, nodes(p) as path_nodes, relationships(p) as path_relationships
    """

    payload = {
        "query": query
    }

    try:
        # Make request to ClickGraph server
        response = requests.post(
            "http://localhost:8080/query",
            json=payload,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            print("[OK] Path Variables test PASSED")
            print(f"Response: {json.dumps(result, indent=2)}")
            return True
        else:
            print(f"[FAIL] Path Variables test FAILED - Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False

    except Exception as e:
        print(f"[FAIL] Path Variables test FAILED - Exception: {e}")
        return False

if __name__ == "__main__":
    print("Testing Path Variables feature...")
    success = test_path_variables()
    sys.exit(0 if success else 1)