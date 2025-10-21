"""Debug test to see generated SQL"""
import requests

def test_with_debug():
    query = """
    MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User))
    WHERE a.name = 'Alice Johnson' AND b.name = 'Frank Wilson'
    RETURN a.name AS start_name, b.name AS end_name
    """
    
    print("Testing query:")
    print(query)
    print()
    
    # Try to get debug info if available
    response = requests.post(
        "http://localhost:8080/query",
        json={"query": query, "debug": True},  # Try debug flag
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print("\nFull response:")
        import json
        print(json.dumps(result, indent=2))
    else:
        print(f"Error: {response.text}")

if __name__ == "__main__":
    test_with_debug()
