"""
Integration test for multi-hop query planner bug fix.

Bug: Multi-hop patterns like (u1)-[]->()-[]->(u2) generate SQL with missing JOINs,
causing ClickHouse error: "Unknown expression or function identifier `u1.user_id`"

Expected: Complete JOIN chain with all intermediate nodes
"""
import requests
import json

BASE_URL = "http://localhost:8080"

def test_multi_hop_anonymous_intermediate():
    """Test: (u1)-[:FOLLOWS]->()-[:FOLLOWS]->(u2) with anonymous intermediate node"""
    query = """
    MATCH (u1:User)-[:FOLLOWS]->()-[:FOLLOWS]->(u2:User) 
    WHERE u1.user_id = 1 
    RETURN DISTINCT u2.name, u2.user_id 
    LIMIT 10
    """
    
    response = requests.post(f"{BASE_URL}/query", json={
        "query": query,
        "sql_only": True  # Get SQL for debugging
    })
    
    print(f"\n{'='*60}")
    print("Test: Anonymous Intermediate Node")
    print(f"{'='*60}")
    print(f"Query: {query.strip()}")
    print(f"\nHTTP Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        sql = result.get("generated_sql", "")
        print(f"\nGenerated SQL:\n{sql}")
        
        # Check for expected JOIN chain
        if "INNER JOIN" in sql:
            join_count = sql.count("INNER JOIN")
            print(f"\n✅ Found {join_count} INNER JOINs")
            
            # Should have: u1 JOIN rel1 JOIN (intermediate) JOIN rel2 JOIN u2
            # That's 4 JOINs total (or 3 if u1 is in FROM)
            if join_count >= 3:
                print("✅ JOIN count looks good for 2-hop pattern")
            else:
                print(f"❌ Expected at least 3 JOINs, got {join_count}")
        else:
            print("❌ No INNER JOINs found!")
    else:
        print(f"\n❌ Query failed: {response.text}")
    
    return response

def test_multi_hop_named_intermediate():
    """Test: (u)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof) with named intermediate node"""
    query = """
    MATCH (u:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof:User) 
    WHERE u.user_id = 1 
    RETURN DISTINCT fof.name, fof.user_id 
    LIMIT 10
    """
    
    response = requests.post(f"{BASE_URL}/query", json={
        "query": query,
        "sql_only": True
    })
    
    print(f"\n{'='*60}")
    print("Test: Named Intermediate Node (friend)")
    print(f"{'='*60}")
    print(f"Query: {query.strip()}")
    print(f"\nHTTP Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        sql = result.get("generated_sql", "")
        print(f"\nGenerated SQL:\n{sql}")
        
        # Check if 'friend' appears in the SQL
        if "friend" in sql.lower() or "AS friend" in sql:
            print("✅ Intermediate node 'friend' appears in SQL")
        else:
            print("⚠️  Intermediate node 'friend' not found in SQL")
    else:
        print(f"\n❌ Query failed: {response.text}")
    
    return response

def test_bidirectional_pattern():
    """Test: (u1)-[:FOLLOWS]->(u2)-[:FOLLOWS]->(u1) bidirectional mutual follows"""
    query = """
    MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u1) 
    RETURN u1.name, u2.name, u1.user_id, u2.user_id 
    LIMIT 10
    """
    
    response = requests.post(f"{BASE_URL}/query", json={
        "query": query,
        "sql_only": True
    })
    
    print(f"\n{'='*60}")
    print("Test: Bidirectional Pattern (mutual follows)")
    print(f"{'='*60}")
    print(f"Query: {query.strip()}")
    print(f"\nHTTP Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        sql = result.get("generated_sql", "")
        print(f"\nGenerated SQL:\n{sql}")
        
        # Should reference u1 twice (start and end of cycle)
        u1_count = sql.count("u1")
        print(f"\n'u1' appears {u1_count} times in SQL")
        
        if u1_count >= 2:
            print("✅ Cyclic reference detected")
        else:
            print("⚠️  Expected u1 to appear at least twice")
    else:
        print(f"\n❌ Query failed: {response.text}")
    
    return response

if __name__ == "__main__":
    print("\n" + "="*60)
    print("MULTI-HOP QUERY PLANNER BUG - REPRODUCTION TESTS")
    print("="*60)
    
    # Test all three failing patterns
    r1 = test_multi_hop_anonymous_intermediate()
    r2 = test_multi_hop_named_intermediate()
    r3 = test_bidirectional_pattern()
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    results = [
        ("Anonymous intermediate", r1.status_code == 200),
        ("Named intermediate", r2.status_code == 200),
        ("Bidirectional", r3.status_code == 200)
    ]
    
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {name}")
    
    passed = sum(1 for _, p in results if p)
    print(f"\nTotal: {passed}/{len(results)} passing")
