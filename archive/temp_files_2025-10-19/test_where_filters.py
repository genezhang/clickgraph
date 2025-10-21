"""Test if WHERE filters are applied in shortest path queries"""
import requests
import json

def test_shortest_path_filter():
    """Test shortest path with WHERE clause"""
    query = """
    MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User))
    WHERE a.name = 'Alice Johnson' AND b.name = 'Bob Smith'
    RETURN a.name AS start_name, b.name AS end_name
    """
    
    print("Testing query:")
    print(query)
    print()
    
    response = requests.post(
        "http://localhost:8080/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        
        # Show generated SQL if available
        if isinstance(result, dict) and "generated_sql" in result:
            print("\nGenerated SQL:")
            print(result["generated_sql"])
            print()
        
        # Get results
        results = result.get("results", []) if isinstance(result, dict) else result
        
        print(f"\nResults: {len(results)} rows")
        for i, row in enumerate(results, 1):
            print(f"  {i}. {row}")
    else:
        print(f"Error: {response.text}")

if __name__ == "__main__":
    test_shortest_path_filter()
