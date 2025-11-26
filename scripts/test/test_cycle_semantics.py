"""
Test cycle prevention behavior to verify Neo4j semantics.

Questions to answer:
1. Does Neo4j prevent cycles in directed variable-length paths?
2. Does Neo4j prevent cycles in undirected variable-length paths?
3. What about explicit multi-hop patterns?

We'll test our current implementation and document expected behavior.
"""

import requests

BASE_URL = "http://localhost:8080"

def query(cypher, sql_only=True):
    """Execute a Cypher query."""
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher, "sql_only": sql_only}
    )
    return response.json()

def test_directed_variable_length_cycle():
    """Test if directed variable-length paths allow cycles."""
    print("\n=== Test 1: Directed Variable-Length (*2) ===")
    cypher = """
    MATCH (a:User)-[:FOLLOWS*2]->(c:User)
    WHERE a.user_id = 1
    RETURN a.user_id, c.user_id
    LIMIT 5
    """
    result = query(cypher)
    print(f"Query: {cypher.strip()}")
    print(f"\nGenerated SQL snippet:")
    sql = result['generated_sql']
    # Extract WHERE clause
    where_idx = sql.find('WHERE')
    if where_idx > 0:
        print(sql[where_idx:where_idx+200])
    
    print("\nCurrent filters:")
    if "a.user_id <> c.user_id" in sql:
        print("  ✅ Start != End (prevents a->b->a)")
    if "r1.followed_id <> r2.follower_id" in sql:
        print("  ✅ No backtracking (prevents consecutive cycle)")
    
    print("\nQuestion: Should Neo4j prevent (a)-[:FOLLOWS*2]->(a)?")
    print("Answer: Need to test in Neo4j, but typically YES for relationship uniqueness")

def test_undirected_pattern():
    """Test undirected pattern (friends-of-friends case)."""
    print("\n=== Test 2: Undirected Pattern (FRIEND example) ===")
    cypher = """
    MATCH (user:User)-[r1:FOLLOWS]-(friend)-[r2:FOLLOWS]-(fof:User)
    WHERE user.user_id = 1
    RETURN DISTINCT fof.user_id
    ORDER BY fof.user_id
    LIMIT 10
    """
    result = query(cypher, sql_only=False)
    print(f"Query: {cypher.strip()}")
    
    if 'results' in result:
        fof_ids = [r['fof.user_id'] for r in result['results']]
        print(f"\nResults: {fof_ids}")
        
        if 1 in fof_ids:
            print("  ❌ User_id 1 appears in own friends-of-friends (BUG)")
            print("  Expected: Should NOT appear (Neo4j excludes start node)")
        else:
            print("  ✅ User_id 1 correctly excluded")
    
    print("\nNeo4j Behavior: Excludes start node from undirected patterns")
    print("OpenCypher Spec: 'Looking for a user's friends of friends should not return said user'")

def test_explicit_two_hop_directed():
    """Test explicit 2-hop directed pattern."""
    print("\n=== Test 3: Explicit 2-Hop Directed ===")
    cypher = """
    MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
    WHERE a.user_id = 1
    RETURN a.user_id, b.user_id, c.user_id
    LIMIT 5
    """
    result = query(cypher)
    print(f"Query: {cypher.strip()}")
    print(f"\nGenerated SQL snippet:")
    sql = result['generated_sql']
    where_idx = sql.find('WHERE')
    if where_idx > 0:
        print(sql[where_idx:where_idx+200])
    
    print("\nCurrent filters:")
    if "a.user_id <> c.user_id" in sql:
        print("  ✅ a != c")
    else:
        print("  ❌ NO cycle prevention (allows a->b->a)")
    
    if "a.user_id <> b.user_id" in sql:
        print("  ✅ a != b")
    else:
        print("  ⚠️  NO a != b filter")
    
    print("\nQuestion: Should Neo4j allow (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(a)?")
    print("Answer: NO - relationship uniqueness implies different paths")

def test_unbounded_variable_length():
    """Test unbounded variable-length (*1..)."""
    print("\n=== Test 4: Unbounded Variable-Length (*1..) ===")
    cypher = """
    MATCH (a:User)-[:FOLLOWS*1..]->(c:User)
    WHERE a.user_id = 1
    RETURN c.user_id
    LIMIT 5
    """
    result = query(cypher)
    print(f"Query: {cypher.strip()}")
    
    sql = result['generated_sql']
    if "WITH RECURSIVE" in sql:
        print("  ✅ Uses recursive CTE")
        
        # Check for cycle prevention
        if "start_id <> end_id" in sql or "start_node_id <> end_node_id" in sql:
            print("  ✅ Has start != end prevention")
        
        # Check for depth limit
        if "depth <=" in sql or "depth <" in sql:
            print("  ✅ Has recursion depth limit")
        else:
            print("  ⚠️  NO recursion depth limit (potential infinite loop!)")
    
    print("\nCritical: Unbounded paths MUST have depth limit to prevent infinite recursion")
    print("User should be able to configure: --max-recursion-depth 1000")

def test_recursion_depth_config():
    """Check if recursion depth is configurable."""
    print("\n=== Test 5: Recursion Depth Configuration ===")
    
    # TODO: Check server config for recursion depth settings
    print("Current status: Need to check if MAX_RECURSION_DEPTH is configurable")
    print("\nRecommendations:")
    print("  1. CLI flag: --max-recursion-depth 1000")
    print("  2. ENV var: CLICKGRAPH_MAX_RECURSION_DEPTH=1000")
    print("  3. Schema config: max_recursion_depth: 1000")
    print("  4. Per-query hint: OPTION (MAX_RECURSION 1000)")

if __name__ == "__main__":
    print("=" * 70)
    print("Testing Cycle Prevention Semantics")
    print("=" * 70)
    
    try:
        test_directed_variable_length_cycle()
        test_undirected_pattern()
        test_explicit_two_hop_directed()
        test_unbounded_variable_length()
        test_recursion_depth_config()
        
        print("\n" + "=" * 70)
        print("Summary of Findings")
        print("=" * 70)
        print("\n1. Variable-Length Directed (*2):")
        print("   - Current: Has cycle prevention (start != end, no backtracking)")
        print("   - Question: Does Neo4j actually prevent cycles here?")
        
        print("\n2. Undirected Pattern (FRIEND):")
        print("   - Current: BUG - allows start node in results")
        print("   - Expected: Should exclude start node (easy fix!)")
        
        print("\n3. Explicit Multi-Hop:")
        print("   - Current: NO cycle prevention")
        print("   - Question: Does Neo4j prevent cycles in explicit patterns?")
        
        print("\n4. Unbounded Variable-Length (*1..):")
        print("   - Current: Uses recursive CTE")
        print("   - TODO: Verify recursion depth limit exists and is configurable")
        
        print("\n" + "=" * 70)
        print("Next Steps")
        print("=" * 70)
        print("1. ✅ Add start != end filter for undirected patterns (EASY)")
        print("2. ⚠️  Test Neo4j: Does it prevent cycles in directed *2 patterns?")
        print("3. ⚠️  Test Neo4j: Does it prevent cycles in explicit 2-hop patterns?")
        print("4. ✅ Make recursion depth configurable (CLI/ENV)")
        print("5. 📝 Document actual Neo4j semantics (not assumptions)")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
