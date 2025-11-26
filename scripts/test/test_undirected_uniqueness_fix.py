"""
Test undirected pattern node uniqueness fix.

OpenCypher spec requirement:
"Looking for a user's friends of friends should not return said user"
"""

import requests
import json

BASE_URL = "http://localhost:8080"

def test_undirected_single_hop():
    """Test undirected single-hop pattern."""
    print("\n=== Test 1: Undirected Single Hop ===")
    cypher = """
    MATCH (a:User)-[:FOLLOWS]-(b:User)
    WHERE a.user_id = 1
    RETURN a.user_id, b.user_id
    LIMIT 5
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher, "sql_only": True}
    )
    result = response.json()
    
    print(f"Query: {cypher.strip()}")
    print(f"\nGenerated SQL WHERE clause:")
    sql = result['generated_sql']
    where_idx = sql.find('WHERE')
    if where_idx > 0:
        where_clause = sql[where_idx:where_idx+150]
        print(where_clause)
        
        if "a.user_id <> b.user_id" in sql:
            print("\n✅ Node uniqueness filter present!")
        else:
            print("\n⚠️  NO node uniqueness filter (allows a = b)")
    else:
        print("No WHERE clause found")

def test_undirected_two_hop():
    """Test undirected friends-of-friends pattern (the classic example)."""
    print("\n=== Test 2: Undirected Two-Hop (Friends-of-Friends) ===")
    cypher = """
    MATCH (user:User)-[r1:FOLLOWS]-(friend)-[r2:FOLLOWS]-(fof:User)
    WHERE user.user_id = 1
    RETURN DISTINCT fof.user_id
    ORDER BY fof.user_id
    LIMIT 10
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher, "sql_only": True}
    )
    result = response.json()
    
    print(f"Query: {cypher.strip()}")
    print(f"\nGenerated SQL WHERE clause:")
    sql = result['generated_sql']
    where_idx = sql.find('WHERE')
    if where_idx > 0:
        where_clause = sql[where_idx:where_idx+200]
        print(where_clause)
        
        # Check for node uniqueness filters
        filters_found = []
        if "user.user_id <> fof.user_id" in sql or "user.user_id <> friend.user_id" in sql:
            filters_found.append("✅ user != fof (or user != friend)")
        else:
            print("\n⚠️  NO user != fof filter")
            
        if filters_found:
            print(f"\nFilters found:")
            for f in filters_found:
                print(f"  {f}")
        else:
            print("\n❌ No node uniqueness filters - BUG!")
    else:
        print("No WHERE clause found")
    
    print("\n📖 OpenCypher Spec Requirement:")
    print("   'Looking for a user's friends of friends should not return said user'")
    print("   Expected: user.user_id != fof.user_id filter")

def test_directed_pattern_no_filter():
    """Test that directed patterns do NOT get unnecessary filters."""
    print("\n=== Test 3: Directed Pattern (Should NOT add filter) ===")
    cypher = """
    MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
    WHERE a.user_id = 1
    RETURN a.user_id, c.user_id
    LIMIT 5
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher, "sql_only": True}
    )
    result = response.json()
    
    print(f"Query: {cypher.strip()}")
    print(f"\nGenerated SQL WHERE clause:")
    sql = result['generated_sql']
    where_idx = sql.find('WHERE')
    if where_idx > 0:
        where_clause = sql[where_idx:where_idx+150]
        print(where_clause)
        
        if "a.user_id <> c.user_id" in sql:
            print("\n⚠️  Has a != c filter (unexpected for explicit directed)")
        else:
            print("\n✅ No unnecessary filters (directed pattern)")
    else:
        print("No WHERE clause found (only user filter expected)")
    
    print("\nNote: Directed patterns may or may not need cycle prevention")
    print("      (depends on Neo4j semantics - need to test)")

def test_variable_length_undirected():
    """Test undirected variable-length pattern."""
    print("\n=== Test 4: Undirected Variable-Length (*2) ===")
    cypher = """
    MATCH (a:User)-[:FOLLOWS*2]-(c:User)
    WHERE a.user_id = 1
    RETURN a.user_id, c.user_id
    LIMIT 5
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher, "sql_only": True}
    )
    result = response.json()
    
    print(f"Query: {cypher.strip()}")
    print(f"\nGenerated SQL WHERE clause:")
    sql = result['generated_sql']
    where_idx = sql.find('WHERE')
    if where_idx > 0:
        where_clause = sql[where_idx:where_idx+200]
        print(where_clause)
        
        filters = []
        if "a.user_id <> c.user_id" in sql:
            filters.append("✅ a != c (node uniqueness)")
        if "r1.followed_id <> r2.follower_id" in sql:
            filters.append("✅ No backtracking (cycle prevention)")
        
        if filters:
            print(f"\nFilters found:")
            for f in filters:
                print(f"  {f}")
        else:
            print("\n⚠️  Expected both node uniqueness AND cycle prevention")

if __name__ == "__main__":
    print("=" * 70)
    print("Testing Undirected Pattern Node Uniqueness Fix")
    print("=" * 70)
    
    try:
        test_undirected_single_hop()
        test_undirected_two_hop()
        test_directed_pattern_no_filter()
        test_variable_length_undirected()
        
        print("\n" + "=" * 70)
        print("Summary")
        print("=" * 70)
        print("\n✅ Undirected patterns should exclude start node from results")
        print("   Example: (user)-[:FRIEND]-(fof) must ensure user != fof")
        print("\n📝 This fix implements OpenCypher spec requirement:")
        print("   'Looking for a user's friends of friends should not return said user'")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
