"""
Test script for shortest path SQL generation.
Verifies that shortestPath() and allShortestPaths() generate correct SQL.
"""

import requests
import json

BASE_URL = "http://localhost:8080"

def test_shortest_path_sql_generation():
    """Test that shortestPath() generates correct SQL with ORDER BY and LIMIT"""
    query = """
    MATCH shortestPath((a:Person)-[*]-(b:Person))
    WHERE a.name = 'Alice' AND b.name = 'Bob'
    RETURN a.name, b.name
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "sql_only": True},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code == 200:
        result = response.json()
        sql = result.get("generated_sql", "")
        print("=== shortestPath() SQL ===")
        print(sql)
        print()
        
        # Verify SQL contains shortest path filtering
        assert "ORDER BY hop_count ASC LIMIT 1" in sql, "SQL should contain ORDER BY hop_count ASC LIMIT 1"
        assert "_inner" in sql, "SQL should use nested CTE pattern (_inner)"
        print("✅ shortestPath() SQL generation looks correct")
    else:
        print(f"❌ Request failed: {response.status_code}")
        print(response.text)

def test_all_shortest_paths_sql_generation():
    """Test that allShortestPaths() generates correct SQL with MIN filtering"""
    query = """
    MATCH allShortestPaths((a:Person)-[*]-(b:Person))
    WHERE a.name = 'Alice' AND b.name = 'Bob'
    RETURN a.name, b.name
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "sql_only": True},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code == 200:
        result = response.json()
        sql = result.get("generated_sql", "")
        print("=== allShortestPaths() SQL ===")
        print(sql)
        print()
        
        # Verify SQL contains all shortest paths filtering
        assert "MIN(hop_count)" in sql, "SQL should contain MIN(hop_count)"
        assert "WHERE hop_count =" in sql, "SQL should filter by hop_count"
        assert "_inner" in sql, "SQL should use nested CTE pattern (_inner)"
        print("✅ allShortestPaths() SQL generation looks correct")
    else:
        print(f"❌ Request failed: {response.status_code}")
        print(response.text)

def test_regular_variable_length():
    """Test that regular variable-length paths don't add shortest path filtering"""
    query = """
    MATCH (a:Person)-[*1..3]-(b:Person)
    WHERE a.name = 'Alice'
    RETURN b.name
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "sql_only": True},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code == 200:
        result = response.json()
        sql = result.get("generated_sql", "")
        print("=== Regular variable-length path SQL ===")
        print(sql)
        print()
        
        # Verify SQL does NOT contain shortest path filtering
        assert "ORDER BY hop_count ASC LIMIT 1" not in sql, "Regular paths should not have LIMIT 1"
        assert "MIN(hop_count)" not in sql, "Regular paths should not filter by MIN"
        print("✅ Regular variable-length path SQL generation looks correct")
    else:
        print(f"❌ Request failed: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    print("Testing Shortest Path SQL Generation")
    print("=" * 50)
    print()
    
    try:
        test_shortest_path_sql_generation()
        print()
        test_all_shortest_paths_sql_generation()
        print()
        test_regular_variable_length()
        print()
        print("=" * 50)
        print("✅ All SQL generation tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
    except requests.exceptions.ConnectionError:
        print("\n❌ Could not connect to server. Make sure it's running on port 8080")
        print("   Start with: cargo run --bin brahmand")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
