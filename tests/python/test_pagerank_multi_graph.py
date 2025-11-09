#!/usr/bin/env python3
"""
Test script for PageRank multi-graph support
"""

import requests
import json
import time

def test_pagerank_multi_graph():
    """Test PageRank with different graph specifications"""

    base_url = "http://localhost:8080"

    # Test 1: PageRank without graph parameter (should default to User)
    print("Test 1: PageRank without graph parameter")
    query1 = {
        "query": "CALL pagerank(maxIterations: 5, dampingFactor: 0.85)"
    }

    try:
        response = requests.post(f"{base_url}/query", json=query1)
        if response.status_code == 200:
            result = response.json()
            print("[OK] Default graph PageRank successful")
            print(f"  Result keys: {list(result.keys())}")
        else:
            print(f"✗ Default graph PageRank failed: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"✗ Default graph PageRank error: {e}")

    # Test 2: PageRank with explicit graph parameter
    print("\nTest 2: PageRank with explicit graph parameter")
    query2 = {
        "query": "CALL pagerank(graph: 'User', maxIterations: 5, dampingFactor: 0.85)"
    }

    try:
        response = requests.post(f"{base_url}/query", json=query2)
        if response.status_code == 200:
            result = response.json()
            print("[OK] Explicit graph PageRank successful")
            print(f"  Result keys: {list(result.keys())}")
        else:
            print(f"✗ Explicit graph PageRank failed: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"✗ Explicit graph PageRank error: {e}")

    # Test 3: PageRank with node labels and relationship types
    print("\nTest 3: PageRank with node labels and relationship types")
    query3 = {
        "query": "CALL pagerank(nodeLabels: 'User', relationshipTypes: 'FOLLOWS,FRIENDS_WITH', maxIterations: 5, dampingFactor: 0.85)"
    }

    try:
        response = requests.post(f"{base_url}/query", json=query3)
        if response.status_code == 200:
            result = response.json()
            print("[OK] Filtered PageRank successful")
            print(f"  Result keys: {list(result.keys())}")
        else:
            print(f"✗ Filtered PageRank failed: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"✗ Filtered PageRank error: {e}")

    # Test 4: PageRank with non-existent graph (should fail gracefully)
    print("\nTest 4: PageRank with non-existent graph")
    query4 = {
        "query": "CALL pagerank(graph: 'NonExistentGraph', maxIterations: 5, dampingFactor: 0.85)"
    }

    try:
        response = requests.post(f"{base_url}/query", json=query4)
        if response.status_code == 200:
            result = response.json()
            print("[OK] Non-existent graph handled gracefully")
            print(f"  Result keys: {list(result.keys())}")
        else:
            print(f"[OK] Non-existent graph failed as expected: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"[OK] Non-existent graph error handled: {e}")

    # Test 5: PageRank with legacy parameter names (backward compatibility)
    print("\nTest 5: PageRank with legacy parameter names")
    query5 = {
        "query": "CALL pagerank(graph: 'User', iterations: 5, damping: 0.85)"
    }

    try:
        response = requests.post(f"{base_url}/query", json=query5)
        if response.status_code == 200:
            result = response.json()
            print("[OK] Legacy parameter names work")
            print(f"  Result keys: {list(result.keys())}")
        else:
            print(f"✗ Legacy parameter names failed: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"✗ Legacy parameter names error: {e}")

if __name__ == "__main__":
    print("Testing PageRank multi-graph support...")
    test_pagerank_multi_graph()
    print("\nTest completed!")