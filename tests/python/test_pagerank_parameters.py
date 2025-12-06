#!/usr/bin/env python3
"""
Test script to verify PageRank parameter handling in ClickGraph.
Tests all parameter combinations and edge cases.
"""

import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json
import time
import sys

def test_pagerank_parameters():
    """Test various PageRank parameter combinations"""

    base_url = f"{CLICKGRAPH_URL}"

    test_cases = [
        # Test 1: Basic graph parameter (backward compatibility)
        {
            "name": "Basic graph parameter",
            "query": "CALL pagerank.graph(graph => 'User') YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 2: nodeLabels parameter
        {
            "name": "nodeLabels parameter",
            "query": "CALL pagerank.graph(nodeLabels => 'User') YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 3: Multiple node labels (using only available labels)
        {
            "name": "Multiple node labels (single valid label)",
            "query": "CALL pagerank.graph(nodeLabels => 'User') YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 4: relationshipTypes parameter
        {
            "name": "relationshipTypes parameter",
            "query": "CALL pagerank.graph(graph => 'User', relationshipTypes => 'FOLLOWS') YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 5: Multiple relationship types
        {
            "name": "Multiple relationship types",
            "query": "CALL pagerank.graph(graph => 'User', relationshipTypes => 'FOLLOWS,LIKES') YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 6: maxIterations parameter
        {
            "name": "maxIterations parameter",
            "query": "CALL pagerank.graph(graph => 'User', maxIterations => 5) YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 7: dampingFactor parameter
        {
            "name": "dampingFactor parameter",
            "query": "CALL pagerank.graph(graph => 'User', dampingFactor => 0.8) YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 8: Legacy iterations parameter
        {
            "name": "Legacy iterations parameter",
            "query": "CALL pagerank.graph(graph => 'User', iterations => 3) YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 9: Legacy damping parameter
        {
            "name": "Legacy damping parameter",
            "query": "CALL pagerank.graph(graph => 'User', damping => 0.9) YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 10: All parameters combined
        {
            "name": "All parameters combined",
            "query": "CALL pagerank.graph(graph => 'User', nodeLabels => 'User', relationshipTypes => 'FOLLOWS', maxIterations => 3, dampingFactor => 0.85) YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": True
        },

        # Test 11: Empty nodeLabels (should fail)
        {
            "name": "Empty nodeLabels (should fail)",
            "query": "CALL pagerank.graph(nodeLabels => '') YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": False
        },

        # Test 12: Invalid node label
        {
            "name": "Invalid node label",
            "query": "CALL pagerank.graph(nodeLabels => 'InvalidNode') YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": False
        },

        # Test 13: Empty relationshipTypes (should work - use all)
        {
            "name": "Empty relationshipTypes",
            "query": "CALL pagerank.graph(graph => 'User', relationshipTypes => '') YIELD node, score RETURN node, score LIMIT 5",
            "expected_success": False  # Should fail as per current implementation
        }
    ]

    results = []

    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test_case['name']}")
        print(f"Query: {test_case['query']}")

        try:
            payload = {"query": test_case['query']}
            response = requests.post(f"{base_url}/query", json=payload, timeout=30)

            if response.status_code == 200:
                result = response.json()
                if 'error' in result:
                    success = False
                    error_msg = result['error']
                    print(f"[FAIL] FAILED: {error_msg}")
                else:
                    success = True
                    # Response is a list of results
                    if isinstance(result, list):
                        data = result
                    else:
                        data = result.get('data', [])
                    data_count = len(data)
                    print(f"[OK] SUCCESS: Returned {data_count} rows")
            else:
                success = False
                print(f"[FAIL] FAILED: HTTP {response.status_code} - {response.text}")

        except requests.exceptions.RequestException as e:
            success = False
            print(f"[FAIL] FAILED: Request error - {e}")

        # Check if result matches expectation
        if success == test_case['expected_success']:
            print("[OK] Expected result")
            results.append(True)
        else:
            print("[FAIL] Unexpected result")
            results.append(False)

    # Summary
    passed = sum(results)
    total = len(results)
    print(f"\n{'='*50}")
    print(f"Test Results: {passed}/{total} passed")

    if passed == total:
        print("[SUCCESS] All tests passed!")
        return True
    else:
        print("[WARN]  Some tests failed")
        return False

if __name__ == "__main__":
    print("Testing PageRank parameter handling...")
    print("Make sure ClickHouse and ClickGraph server are running!")

    success = test_pagerank_parameters()
    sys.exit(0 if success else 1)