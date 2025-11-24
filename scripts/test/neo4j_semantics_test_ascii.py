"""
Neo4j Semantics Verification Script (ASCII-only for Windows)

This script tests actual Neo4j behavior for cycle prevention and node uniqueness
to ensure ClickGraph compatibility.

Prerequisites:
1. Neo4j running: docker run -d --name neo4j-test -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/testpassword neo4j:latest
2. Python neo4j driver: pip install neo4j

Usage:
    python scripts/test/neo4j_semantics_test_ascii.py
"""

from neo4j import GraphDatabase
import sys

# Neo4j connection
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORDS = ["testpassword", "neo4j", "password", "test"]

class Neo4jTester:
    def __init__(self, uri, user, passwords):
        connected = False
        last_error = None
        
        for password in passwords:
            try:
                self.driver = GraphDatabase.driver(uri, auth=(user, password))
                self.driver.verify_connectivity()
                print(f"[OK] Connected to Neo4j (password: {password[:4]}***)")
                connected = True
                break
            except Exception as e:
                last_error = e
                continue
        
        if not connected:
            print(f"[ERROR] Failed to connect to Neo4j with any password: {last_error}")
            print("\nTried passwords:", passwords)
            print("\nTo start Neo4j with known password:")
            print("docker run -d --name neo4j-test -p 7474:7474 -p 7687:7687 \\")
            print("  -e NEO4J_AUTH=neo4j/testpassword neo4j:latest")
            print("\nOr reset existing Neo4j password via browser: http://localhost:7474")
            sys.exit(1)

    def close(self):
        self.driver.close()

    def run_query(self, query):
        """Execute a Cypher query and return results"""
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

    def setup_test_data(self):
        """Create test data with cycle topology"""
        with self.driver.session() as session:
            # Clear existing data
            session.run("MATCH (n) DETACH DELETE n")
            
            # Create users with FOLLOWS forming a cycle: 1->2->3->4->1
            session.run("""
                CREATE (u1:User {user_id: 1, name: 'Alice'})
                CREATE (u2:User {user_id: 2, name: 'Bob'})
                CREATE (u3:User {user_id: 3, name: 'Charlie'})
                CREATE (u4:User {user_id: 4, name: 'David'})
                CREATE (u1)-[:FOLLOWS]->(u2)
                CREATE (u2)-[:FOLLOWS]->(u3)
                CREATE (u3)-[:FOLLOWS]->(u4)
                CREATE (u4)-[:FOLLOWS]->(u1)
            """)
        print("[OK] Test data created")

def print_separator():
    print("\n" + "="*80 + "\n")

def test_directed_var_length_cycles(tester):
    """Test 1: Directed variable-length paths with *2"""
    print_separator()
    print("TEST 1: Directed Variable-Length Paths (*2)")
    print("-" * 80)
    
    query = """
    MATCH (a:User)-[:FOLLOWS*2]->(c:User)
    WHERE a.user_id = 1
    RETURN a.name AS start, c.name AS end
    ORDER BY end
    """
    
    print("Query:", query)
    results = tester.run_query(query)
    
    print("\nResults:")
    for r in results:
        print(f"  {r['start']} -> {r['end']}")
    
    print("\nAnalysis:")
    print("- Total results:", len(results))
    if any(r['start'] == r['end'] for r in results):
        print("  [!!] CYCLES ALLOWED: start == end found!")
    else:
        print("  [OK] NO CYCLES: start != end for all results")
    
    print("\nQuestion: Does Neo4j prevent (a)-[:FOLLOWS*2]->(a) cycles?")
    print("Expected: Should NOT return Alice->Alice (cycle)")
    return results

def test_explicit_2hop_cycles(tester):
    """Test 2: Explicit 2-hop patterns"""
    print_separator()
    print("TEST 2: Explicit 2-Hop Patterns")
    print("-" * 80)
    
    query = """
    MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
    WHERE a.user_id = 1
    RETURN a.name AS start, b.name AS intermediate, c.name AS end
    ORDER BY end
    """
    
    print("Query:", query)
    results = tester.run_query(query)
    
    print("\nResults:")
    for r in results:
        print(f"  {r['start']} -> {r['intermediate']} -> {r['end']}")
    
    print("\nAnalysis:")
    print("- Total results:", len(results))
    if any(r['start'] == r['end'] for r in results):
        print("  [!!] CYCLES ALLOWED: start == end found!")
    else:
        print("  [OK] NO CYCLES: start != end for all results")
    
    print("\nQuestion: Does explicit 2-hop allow cycles?")
    print("Expected: Should NOT return Alice->Bob->Alice (cycle)")
    return results

def test_undirected_1hop(tester):
    """Test 3: Undirected 1-hop - must a != b?"""
    print_separator()
    print("TEST 3: Undirected 1-Hop Relationships")
    print("-" * 80)
    
    query = """
    MATCH (a:User)-[:FOLLOWS]-(b:User)
    WHERE a.user_id = 1
    RETURN a.name AS node_a, b.name AS node_b
    ORDER BY node_b
    """
    
    print("Query:", query)
    results = tester.run_query(query)
    
    print("\nResults:")
    for r in results:
        print(f"  {r['node_a']} - {r['node_b']}")
    
    print("\nAnalysis:")
    print("- Total results:", len(results))
    if any(r['node_a'] == r['node_b'] for r in results):
        print("  [!!] SELF-MATCHES ALLOWED: a == b found!")
    else:
        print("  [OK] NO SELF-MATCHES: a != b for all results")
    
    print("\nQuestion: Does undirected pattern enforce a != b?")
    print("Expected: Should NOT return Alice-Alice (self-match)")
    return results

def test_friends_of_friends(tester):
    """Test 4: Friends-of-friends - OpenCypher spec example"""
    print_separator()
    print("TEST 4: Friends-of-Friends Pattern")
    print("-" * 80)
    
    query = """
    MATCH (user:User)-[:FOLLOWS]-(friend:User)-[:FOLLOWS]-(fof:User)
    WHERE user.user_id = 1
    RETURN user.name AS user, friend.name AS friend, fof.name AS friend_of_friend
    ORDER BY fof
    """
    
    print("Query:", query)
    results = tester.run_query(query)
    
    print("\nResults:")
    for r in results:
        print(f"  {r['user']} - {r['friend']} - {r['friend_of_friend']}")
    
    print("\nAnalysis:")
    print("- Total results:", len(results))
    
    # Check if user appears as friend_of_friend
    user_as_fof = [r for r in results if r['user'] == r['friend_of_friend']]
    if user_as_fof:
        print(f"  [!!] START NODE APPEARS IN RESULTS: {len(user_as_fof)} times")
    else:
        print("  [OK] START NODE EXCLUDED: user != fof for all results")
    
    print("\nQuestion: Does Neo4j exclude start node from friends-of-friends results?")
    print("OpenCypher spec: 'Looking for a user's friends of friends should not return said user'")
    print("Expected: Should NOT return Alice-Bob-Alice (start node as result)")
    return results

def test_undirected_var_length(tester):
    """Test 5: Undirected variable-length paths"""
    print_separator()
    print("TEST 5: Undirected Variable-Length Paths (*2)")
    print("-" * 80)
    
    query = """
    MATCH (a:User)-[:FOLLOWS*2]-(c:User)
    WHERE a.user_id = 1
    RETURN a.name AS start, c.name AS end
    ORDER BY end
    """
    
    print("Query:", query)
    results = tester.run_query(query)
    
    print("\nResults:")
    for r in results:
        print(f"  {r['start']} - {r['end']}")
    
    print("\nAnalysis:")
    print("- Total results:", len(results))
    if any(r['start'] == r['end'] for r in results):
        print("  [!!] CYCLES ALLOWED: start == end found!")
    else:
        print("  [OK] NO CYCLES: start != end for all results")
    
    print("\nQuestion: Does undirected *2 prevent cycles?")
    print("Expected: Should NOT return Alice-Alice (cycle)")
    return results

def test_mixed_direction(tester):
    """Test 6: Mixed direction patterns"""
    print_separator()
    print("TEST 6: Mixed Direction Patterns")
    print("-" * 80)
    
    query = """
    MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]-(c:User)
    WHERE a.user_id = 1
    RETURN a.name AS start, b.name AS intermediate, c.name AS end
    ORDER BY end
    """
    
    print("Query:", query)
    results = tester.run_query(query)
    
    print("\nResults:")
    for r in results:
        print(f"  {r['start']} -> {r['intermediate']} - {r['end']}")
    
    print("\nAnalysis:")
    print("- Total results:", len(results))
    if any(r['start'] == r['end'] for r in results):
        print("  [!!] CYCLES ALLOWED: start == end found!")
    else:
        print("  [OK] NO CYCLES: start != end for all results")
    
    print("\nQuestion: What uniqueness filters apply to mixed direction?")
    return results

def test_named_intermediates(tester):
    """Test 7: Named intermediate nodes"""
    print_separator()
    print("TEST 7: Named Intermediate Nodes")
    print("-" * 80)
    
    query = """
    MATCH (a:User)-[:FOLLOWS]-(b:User)-[:FOLLOWS]-(c:User)
    WHERE a.user_id = 1
    RETURN a.name AS node_a, b.name AS node_b, c.name AS node_c
    ORDER BY node_c
    """
    
    print("Query:", query)
    results = tester.run_query(query)
    
    print("\nResults:")
    for r in results:
        print(f"  {r['node_a']} - {r['node_b']} - {r['node_c']}")
    
    print("\nAnalysis:")
    print("- Total results:", len(results))
    
    # Check for pairwise uniqueness
    ab_unique = all(r['node_a'] != r['node_b'] for r in results)
    bc_unique = all(r['node_b'] != r['node_c'] for r in results)
    ac_unique = all(r['node_a'] != r['node_c'] for r in results)
    
    print(f"  a != b: {'[OK]' if ab_unique else '[!!]'}")
    print(f"  b != c: {'[OK]' if bc_unique else '[!!]'}")
    print(f"  a != c: {'[OK]' if ac_unique else '[!!]'}")
    
    print("\nQuestion: Do named nodes enforce full pairwise uniqueness?")
    print("Expected: All three conditions should be true")
    return results

def test_multi_match(tester):
    """Test 8: Multiple MATCH clauses"""
    print_separator()
    print("TEST 8: Multiple MATCH Clauses")
    print("-" * 80)
    
    query = """
    MATCH (a:User)-[:FOLLOWS]->(b:User)
    MATCH (c:User)-[:FOLLOWS]->(d:User)
    WHERE a.user_id = 1 AND c.user_id = 2
    RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d
    """
    
    print("Query:", query)
    results = tester.run_query(query)
    
    print("\nResults:")
    for r in results:
        print(f"  a={r['a']}, b={r['b']}, c={r['c']}, d={r['d']}")
    
    print("\nAnalysis:")
    print("- Total results:", len(results))
    print("\nQuestion: Does uniqueness apply across MATCH clauses?")
    print("Expected: b and c can be the same node")
    return results

def test_unbounded_depth(tester):
    """Test 9: Unbounded variable-length paths"""
    print_separator()
    print("TEST 9: Unbounded Variable-Length Paths (*1..)")
    print("-" * 80)
    
    query = """
    MATCH (a:User)-[:FOLLOWS*1..]->(c:User)
    WHERE a.user_id = 1
    RETURN a.name AS start, c.name AS end, length(path) AS depth
    ORDER BY end
    LIMIT 20
    """
    
    print("Query:", query)
    try:
        # Note: This might take a while or hit recursion limits
        results = tester.run_query(query)
        
        print("\nResults (first 20):")
        for r in results:
            print(f"  {r['start']} -> {r['end']} (depth: {r.get('depth', 'N/A')})")
        
        print("\nAnalysis:")
        print("- Total results:", len(results))
        max_depth = max((r.get('depth', 0) for r in results), default=0)
        print(f"- Max depth reached: {max_depth}")
        
        print("\nQuestion: What is Neo4j's default max depth for unbounded paths?")
    except Exception as e:
        print(f"\n[ERROR] Query failed: {e}")
        print("This might indicate Neo4j has recursion depth limits")
    
    return results if 'results' in locals() else []

def test_relationship_uniqueness(tester):
    """Test 10: Relationship uniqueness"""
    print_separator()
    print("TEST 10: Relationship Uniqueness")
    print("-" * 80)
    
    query = """
    MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User)
    WHERE a.user_id = 1 AND id(r1) = id(r2)
    RETURN a.name AS start, b.name AS intermediate, c.name AS end
    """
    
    print("Query:", query)
    results = tester.run_query(query)
    
    print("\nResults:")
    if not results:
        print("  [EMPTY] - No results")
    for r in results:
        print(f"  {r['start']} -> {r['intermediate']} -> {r['end']}")
    
    print("\nAnalysis:")
    print("- Total results:", len(results))
    if len(results) == 0:
        print("  [OK] RELATIONSHIP UNIQUENESS ENFORCED: Same relationship cannot be used twice")
    else:
        print("  [!!] RELATIONSHIP UNIQUENESS NOT ENFORCED")
    
    print("\nQuestion: Does Neo4j enforce relationship uniqueness in patterns?")
    print("Expected: Empty results (relationship uniqueness is always enforced)")
    return results

def print_summary(all_results):
    """Print summary of all tests"""
    print_separator()
    print("SUMMARY: Neo4j Behavior Analysis")
    print("-" * 80)
    
    print("\n[KEY FINDINGS]")
    print("\n1. Cycle Prevention:")
    print("   - Directed *2:", "Has cycles" if any(r['start'] == r['end'] for r in all_results['test1']) else "No cycles")
    print("   - Explicit 2-hop:", "Has cycles" if any(r['start'] == r['end'] for r in all_results['test2']) else "No cycles")
    print("   - Undirected *2:", "Has cycles" if any(r['start'] == r['end'] for r in all_results['test5']) else "No cycles")
    
    print("\n2. Node Uniqueness:")
    print("   - Undirected 1-hop:", "Has self-matches" if any(r['node_a'] == r['node_b'] for r in all_results['test3']) else "No self-matches")
    print("   - Friends-of-friends:", "Start in results" if any(r['user'] == r['friend_of_friend'] for r in all_results['test4']) else "Start excluded")
    
    if all_results['test7']:
        ab_unique = all(r['node_a'] != r['node_b'] for r in all_results['test7'])
        bc_unique = all(r['node_b'] != r['node_c'] for r in all_results['test7'])
        ac_unique = all(r['node_a'] != r['node_c'] for r in all_results['test7'])
        print(f"   - Named 3-node chain: a!=b={ab_unique}, b!=c={bc_unique}, a!=c={ac_unique}")
    
    print("\n3. Relationship Uniqueness:")
    print("   - Same rel twice:", "Allowed" if all_results['test10'] else "Prevented")
    
    print("\n[RECOMMENDATIONS FOR CLICKGRAPH]")
    print("\nBased on Neo4j's actual behavior, ClickGraph should:")
    
    # Analyze test1 and test2 to determine cycle behavior
    test1_has_cycles = any(r['start'] == r['end'] for r in all_results['test1'])
    test2_has_cycles = any(r['start'] == r['end'] for r in all_results['test2'])
    
    if not test1_has_cycles and not test2_has_cycles:
        print("1. [KEEP] Cycle prevention for all patterns (directed and undirected)")
    else:
        print("1. [REVIEW] Neo4j allows some cycles - verify semantics")
    
    # Analyze test3 for undirected behavior
    test3_has_self = any(r['node_a'] == r['node_b'] for r in all_results['test3'])
    if not test3_has_self:
        print("2. [KEEP] Node uniqueness for undirected patterns (a != b)")
    else:
        print("2. [REMOVE] Neo4j allows self-matches in undirected patterns")
    
    # Analyze test4 for FOF behavior
    test4_has_start = any(r['user'] == r['friend_of_friend'] for r in all_results['test4'])
    if not test4_has_start:
        print("3. [ADD] Overall start != end filter for multi-hop undirected chains")
    else:
        print("3. [REVIEW] Neo4j allows start node in FOF results")
    
    # Analyze test7 for full uniqueness
    if all_results['test7']:
        ac_unique = all(r['node_a'] != r['node_c'] for r in all_results['test7'])
        if ac_unique:
            print("4. [ADD] Full pairwise uniqueness for named intermediate nodes")
        else:
            print("4. [CURRENT] Only adjacent node uniqueness (matches Neo4j)")
    
    print("\n[NEXT STEPS]")
    print("1. Compare these results with ClickGraph's current SQL generation")
    print("2. Adjust filters to match Neo4j's exact behavior")
    print("3. Add integration tests using this data as expected results")
    print("4. Document any intentional deviations from Neo4j")

def main():
    print("="*80)
    print("Neo4j Semantics Verification - Cycle Prevention & Node Uniqueness")
    print("="*80)
    
    print("\n[SETUP]")
    tester = Neo4jTester(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORDS)
    tester.setup_test_data()
    
    # Run all tests
    all_results = {}
    all_results['test1'] = test_directed_var_length_cycles(tester)
    all_results['test2'] = test_explicit_2hop_cycles(tester)
    all_results['test3'] = test_undirected_1hop(tester)
    all_results['test4'] = test_friends_of_friends(tester)
    all_results['test5'] = test_undirected_var_length(tester)
    all_results['test6'] = test_mixed_direction(tester)
    all_results['test7'] = test_named_intermediates(tester)
    all_results['test8'] = test_multi_match(tester)
    all_results['test9'] = test_unbounded_depth(tester)
    all_results['test10'] = test_relationship_uniqueness(tester)
    
    # Print summary
    print_summary(all_results)
    
    tester.close()
    print("\n[COMPLETE] All tests finished")

if __name__ == "__main__":
    main()
