"""
Test shortest path with real ClickHouse data.

Test cases:
1. Direct connection (Alice -> Bob): 1 hop
2. Indirect connection with shortcut (Alice -> Carol): Should find 1-hop path, not 2-hop
3. Multi-hop path (Alice -> Eve): Should find shortest (2 or 4 hops depending on path)
4. Disconnected nodes (Alice -> Frank): Should return empty
"""

import requests
import json

BASE_URL = "http://localhost:8080"

def test_query(description, query, expect_results=True):
    """Run a query and show results"""
    print(f"\n{'='*80}")
    print(f"TEST: {description}")
    print(f"{'='*80}")
    print(f"Query: {query}")
    print()
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code == 200:
        result = response.json()
        
        # Handle both dict and list responses
        if isinstance(result, dict):
            # Show generated SQL
            if "generated_sql" in result:
                print("Generated SQL:")
                print(result["generated_sql"])
                print()
            results = result.get("results", [])
        else:
            # Direct list response
            results = result if isinstance(result, list) else []
        
        # Show results
        print(f"Results ({len(results)} rows):")
        if results:
            for i, row in enumerate(results, 1):
                print(f"  {i}. {row}")
        else:
            print("  (empty)")
        
        # Verify expectation
        if expect_results and not results:
            print("❌ UNEXPECTED: Expected results but got empty!")
        elif not expect_results and results:
            print("❌ UNEXPECTED: Expected empty but got results!")
        else:
            print("✅ Result matches expectation")
    else:
        print(f"❌ Request failed: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    print("Testing Shortest Path with Real ClickHouse Data")
    print("="*80)
    
    # Test 1: Direct connection (1 hop)
    test_query(
        "Direct connection (Alice -> Bob): 1 hop",
        """
        MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User))
        WHERE a.name = 'Alice Johnson' AND b.name = 'Bob Smith'
        RETURN a.name, b.name
        """,
        expect_results=True
    )
    
    # Test 2: Shortcut path (Alice -> Carol: direct vs via Bob)
    test_query(
        "Shortcut path (Alice -> Carol): Should prefer direct 1-hop over 2-hop via Bob",
        """
        MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User))
        WHERE a.name = 'Alice Johnson' AND b.name = 'Carol Brown'
        RETURN a.name, b.name
        """,
        expect_results=True
    )
    
    # Test 3: Multi-hop path (Alice -> Eve via Bob -> Carol -> David -> Eve)
    test_query(
        "Multi-hop path (Alice -> Eve): 4 hops via Bob -> Carol -> David -> Eve",
        """
        MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User))
        WHERE a.name = 'Alice Johnson' AND b.name = 'Eve Martinez'
        RETURN a.name, b.name
        """,
        expect_results=True
    )
    
    # Test 4: Disconnected nodes (Alice -> Frank)
    test_query(
        "Disconnected nodes (Alice -> Frank): Should return empty",
        """
        MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User))
        WHERE a.name = 'Alice Johnson' AND b.name = 'Frank Wilson'
        RETURN a.name, b.name
        """,
        expect_results=False
    )
    
    # Test 5: allShortestPaths (Alice -> Carol might have multiple paths)
    test_query(
        "All shortest paths (Alice -> Eve): Show all shortest paths",
        """
        MATCH allShortestPaths((a:User)-[:FOLLOWS*]-(b:User))
        WHERE a.name = 'Alice Johnson' AND b.name = 'Eve Martinez'
        RETURN a.name, b.name
        """,
        expect_results=True
    )
    
    print(f"\n{'='*80}")
    print("All tests completed!")
    print(f"{'='*80}")
