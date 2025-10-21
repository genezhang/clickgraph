"""Test if WHERE filters are properly applied for disconnected nodes"""
import requests

def test_disconnected_nodes():
    """Test shortest path with WHERE clause for disconnected nodes"""
    query = """
    MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User))
    WHERE a.name = 'Alice Johnson' AND b.name = 'Frank Wilson'
    RETURN a.name AS start_name, b.name AS end_name
    """
    
    print("Testing disconnected nodes:")
    print(query)
    print()
    
    response = requests.post(
        "http://localhost:8080/query",
        json={"query": query}
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        results = result.get("results", []) if isinstance(result, dict) else result
        
        print(f"\nResults: {len(results)} rows")
        if results:
            print("❌ FAIL: Got results for disconnected nodes (WHERE clause ignored)")
            for row in results:
                print(f"  {row}")
        else:
            print("✅ PASS: No results (WHERE clause properly applied)")
    else:
        print(f"Error: {response.text}")

def test_without_where():
    """Test same query without WHERE clause"""
    query = """
    MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User))
    RETURN a.name AS start_name, b.name AS end_name
    LIMIT 5
    """
    
    print("\n" + "="*80)
    print("Testing without WHERE clause (should return some results):")
    print(query)
    print()
    
    response = requests.post(
        "http://localhost:8080/query",
        json={"query": query}
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        results = result.get("results", []) if isinstance(result, dict) else result
        
        print(f"\nResults: {len(results)} rows")
        for i, row in enumerate(results, 1):
            print(f"  {i}. {row}")

if __name__ == "__main__":
    test_disconnected_nodes()
    test_without_where()
