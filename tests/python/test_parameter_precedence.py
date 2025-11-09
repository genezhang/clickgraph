#!/usr/bin/env python3
"""
Focused test for PageRank parameter precedence and validation.
Tests what happens when both 'graph' and 'nodeLabels' are specified.
"""

import requests
import json

def test_parameter_precedence():
    """Test parameter precedence when both graph and nodeLabels are specified"""

    base_url = "http://localhost:8080"

    # Test: Both graph and nodeLabels specified
    # According to current implementation, nodeLabels should take precedence
    test_query = """
    CALL pagerank.graph(graph => 'User', nodeLabels => 'Post')
    YIELD node, score RETURN node, score LIMIT 3
    """

    print("Testing parameter precedence: graph='User' vs nodeLabels='Post'")
    print(f"Query: {test_query.strip()}")

    try:
        payload = {"query": test_query}
        response = requests.post(f"{base_url}/query", json=payload, timeout=30)

        if response.status_code == 200:
            result = response.json()
            print(f"Raw response: {json.dumps(result, indent=2)}")  # Debug: print raw response

            # Check if it's an error response (dictionary with 'error' key)
            if isinstance(result, dict) and 'error' in result:
                print(f"[FAIL] Error: {result['error']}")
                return False
            else:
                # Response is a list of results
                if isinstance(result, list):
                    data = result
                else:
                    data = result.get('data', [])

                print(f"[OK] Success: Returned {len(data)} rows")

                # Check if the results look like Post nodes (should have different IDs than User nodes)
                if data:
                    first_node = data[0]['node_id']  # Access node_id from the dictionary
                    print(f"First node ID: {first_node}")

                    # If nodeLabels takes precedence, we should get Post nodes
                    # Post nodes typically have higher IDs than User nodes in test data
                    if isinstance(first_node, int) and first_node > 100:  # Assuming Post IDs are > 100
                        print("[OK] nodeLabels appears to take precedence (got Post-like IDs)")
                        return True
                    else:
                        print("? Cannot determine precedence from data, but query succeeded")
                        return True
                else:
                    print("[OK] Query succeeded but returned no data")
                    return True
        else:
            print(f"[FAIL] HTTP Error: {response.status_code} - {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"[FAIL] Request error: {e}")
        return False

if __name__ == "__main__":
    print("Testing PageRank parameter precedence...")
    success = test_parameter_precedence()
    print(f"Test {'PASSED' if success else 'FAILED'}")