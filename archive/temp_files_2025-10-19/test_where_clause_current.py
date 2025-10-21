"""Quick test to verify WHERE clause behavior in shortest path queries"""
import requests
import json

def test_query(desc, query):
    print(f"\n{'='*80}")
    print(f"TEST: {desc}")
    print(f"{'='*80}")
    print(f"Query: {query}\n")
    
    try:
        resp = requests.post('http://localhost:8080/query', 
                            json={'query': query},
                            timeout=5)
        
        if resp.status_code == 200:
            result = resp.json()
            print(f"[OK] Status: {resp.status_code}")
            print(f"Results: {json.dumps(result, indent=2)}")
        else:
            print(f"[ERROR] Status: {resp.status_code}")
            print(f"Error: {resp.text}")
    except Exception as e:
        print(f"[ERROR] Exception: {e}")

if __name__ == "__main__":
    # Test 1: Query with WHERE clause on both nodes
    test_query(
        "Shortest path with WHERE clause (Alice -> Bob)",
        "MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User)) WHERE a.name = 'Alice Johnson' AND b.name = 'Bob Smith' RETURN a.name, b.name"
    )
    
    # Test 2: Query without WHERE clause (should find ANY shortest path)
    test_query(
        "Shortest path WITHOUT WHERE clause",
        "MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User)) RETURN a.name, b.name"
    )
    
    # Test 3: Query with disconnected nodes (should return empty)
    test_query(
        "Shortest path to disconnected node (Alice -> Frank)",
        "MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User)) WHERE a.name = 'Alice Johnson' AND b.name = 'Frank Wilson' RETURN a.name, b.name"
    )
