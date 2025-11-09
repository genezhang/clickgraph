#!/usr/bin/env python3
"""
Test path variable SQL generation
"""
import requests
import json
import sys

# Test queries
test_queries = [
    {
        "name": "Basic path variable return",
        "query": """
MATCH p = shortestPath((a:User)-[:FOLLOWS*]-(b:User))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN p
"""
    },
    {
        "name": "Path with properties",
        "query": """
MATCH p = shortestPath((a:User)-[:FOLLOWS*]-(b:User))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN p, a.name AS start_name, b.name AS end_name
"""
    },
    {
        "name": "Variable-length path with range",
        "query": """
MATCH p = (a:User)-[:FOLLOWS*1..3]-(b:User)
WHERE a.name = 'Alice'
RETURN p
"""
    }
]

def test_query(query_obj):
    """Send a query and print the SQL generated"""
    print(f"\n{'='*80}")
    print(f"Test: {query_obj['name']}")
    print(f"{'='*80}")
    print(f"Cypher Query:")
    print(query_obj['query'])
    print(f"\n{'-'*80}")
    
    try:
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": query_obj['query']},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Check if SQL is in the response (debug mode)
            if 'sql' in result:
                print(f"Generated SQL:")
                print(result['sql'])
            else:
                print(f"Response:")
                print(json.dumps(result, indent=2))
                
            # Show first few results
            if 'results' in result and result['results']:
                print(f"\nFirst result:")
                print(json.dumps(result['results'][0], indent=2))
        else:
            print(f"Error: HTTP {response.status_code}")
            print(response.text)
            
    except requests.exceptions.ConnectionError:
        print("[FAIL] Error: Could not connect to server on http://localhost:8080")
        print("   Make sure the ClickGraph server is running:")
        print("   cargo run --bin brahmand")
        sys.exit(1)
    except Exception as e:
        print(f"[FAIL] Error: {e}")

def main():
    print("Testing Path Variable SQL Generation")
    print("=" * 80)
    
    for query_obj in test_queries:
        test_query(query_obj)
    
    print(f"\n{'='*80}")
    print("Tests Complete!")
    print("=" * 80)

if __name__ == "__main__":
    main()
