"""
Neo4j Semantics Verification Script

This script tests actual Neo4j behavior for cycle prevention and node uniqueness
to ensure ClickGraph compatibility.

Prerequisites:
1. Neo4j running: docker run -d --name neo4j-test -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/testpassword neo4j:latest
2. Python neo4j driver: pip install neo4j

Usage:
    python scripts/test/neo4j_semantics_verification.py
"""

from neo4j import GraphDatabase
import sys

# Neo4j connection
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORDS = ["testpassword", "neo4j", "password", "test"]  # Try multiple passwords

class Neo4jTester:
    def __init__(self, uri, user, passwords):
        connected = False
        last_error = None
        
        # Try each password
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

    def setup_test_data(self):
        """Create test data matching ClickGraph benchmark schema."""
        print("\n" + "="*70)
        print("Setting Up Test Data")
        print("="*70)
        
        with self.driver.session() as session:
            # Clear existing data
            session.run("MATCH (n) DETACH DELETE n")
            print("✅ Cleared existing data")
            
            # Create users
            session.run("""
                CREATE (:User {user_id: 1, full_name: 'Alice'})
                CREATE (:User {user_id: 2, full_name: 'Bob'})
                CREATE (:User {user_id: 3, full_name: 'Charlie'})
                CREATE (:User {user_id: 4, full_name: 'David'})
            """)
            print("✅ Created 4 users")
            
            # Create FOLLOWS relationships (directed)
            # Creates cycle: 1 -> 2 -> 3 -> 1
            # Plus direct: 1 -> 3
            session.run("""
                MATCH (a:User {user_id: 1}), (b:User {user_id: 2})
                CREATE (a)-[:FOLLOWS]->(b)
            """)
            session.run("""
                MATCH (a:User {user_id: 2}), (b:User {user_id: 3})
                CREATE (a)-[:FOLLOWS]->(b)
            """)
            session.run("""
                MATCH (a:User {user_id: 3}), (b:User {user_id: 1})
                CREATE (a)-[:FOLLOWS]->(b)
            """)
            session.run("""
                MATCH (a:User {user_id: 1}), (b:User {user_id: 3})
                CREATE (a)-[:FOLLOWS]->(b)
            """)
            print("✅ Created FOLLOWS relationships")
            print("   Topology: 1 -> 2 -> 3 -> 1 (cycle)")
            print("             1 -> 3 (direct)")
            
            # Verify
            result = session.run("MATCH (u:User) RETURN count(u) as count")
            user_count = result.single()["count"]
            result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as count")
            rel_count = result.single()["count"]
            print(f"   Total: {user_count} users, {rel_count} relationships")

    def run_test(self, test_num, query, description, question, expected):
        """Run a single test case."""
        print(f"\n{'='*70}")
        print(f"Test {test_num}: {description}")
        print(f"{'='*70}")
        print(f"❓ Question: {question}")
        print(f"📖 Expected: {expected}")
        print(f"\n🔍 Query:\n{query}")
        
        try:
            with self.driver.session() as session:
                result = session.run(query)
                records = [dict(record) for record in result]
                
                print(f"\n📊 Results ({len(records)} rows):")
                if len(records) == 0:
                    print("   (empty)")
                else:
                    for i, record in enumerate(records[:10], 1):
                        print(f"   {i}. {record}")
                    if len(records) > 10:
                        print(f"   ... and {len(records) - 10} more rows")
                
                return records
        except Exception as e:
            print(f"\n❌ Error: {e}")
            return None

def main():
    print("="*70)
    print("Neo4j Semantics Verification")
    print("Testing cycle prevention and node uniqueness behavior")
    print("="*70)
    
    tester = Neo4jTester(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORDS)
    
    try:
        # Setup test data
        tester.setup_test_data()
        
        # Test 1: Directed Variable-Length (*2) - Cycle Behavior
        results_1 = tester.run_test(
            1,
            """
            MATCH (a:User)-[:FOLLOWS*2]->(c:User)
            WHERE a.user_id = 1
            RETURN a.user_id, c.user_id
            ORDER BY c.user_id
            """,
            "Directed Variable-Length (*2)",
            "Does Neo4j allow (a)-[:FOLLOWS*2]->(a) (returning to start)?",
            "If prevents cycles: No (1,1). If allows: May have (1,1) or (1,3)"
        )
        
        # Test 2: Directed Explicit 2-Hop - Cycle Behavior
        results_2 = tester.run_test(
            2,
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
            WHERE a.user_id = 1
            RETURN a.user_id, b.user_id, c.user_id
            ORDER BY b.user_id, c.user_id
            """,
            "Directed Explicit 2-Hop",
            "Does Neo4j allow (a)->(b)->(a) in explicit patterns?",
            "Check if (1,2,1) or (1,3,1) appear in results"
        )
        
        # Test 3: Undirected Single-Hop - Node Uniqueness
        results_3 = tester.run_test(
            3,
            """
            MATCH (a:User)-[:FOLLOWS]-(b:User)
            WHERE a.user_id = 1
            RETURN a.user_id, b.user_id
            ORDER BY b.user_id
            """,
            "Undirected Single-Hop",
            "Does Neo4j return (1,1) if user 1 follows itself?",
            "Should NOT return (1,1) - undirected requires a != b"
        )
        
        # Test 4: Undirected Two-Hop - Friends-of-Friends
        results_4 = tester.run_test(
            4,
            """
            MATCH (user:User)-[:FOLLOWS]-(friend)-[:FOLLOWS]-(fof:User)
            WHERE user.user_id = 1
            RETURN DISTINCT fof.user_id
            ORDER BY fof.user_id
            """,
            "Undirected Two-Hop (Friends-of-Friends)",
            "Does user_id=1 appear in the results?",
            "Should NOT return user_id=1 (OpenCypher spec)"
        )
        
        # Test 5: Undirected Variable-Length (*2)
        results_5 = tester.run_test(
            5,
            """
            MATCH (a:User)-[:FOLLOWS*2]-(c:User)
            WHERE a.user_id = 1
            RETURN a.user_id, c.user_id
            ORDER BY c.user_id
            """,
            "Undirected Variable-Length (*2)",
            "Does Neo4j return (1,1)?",
            "Should NOT return (1,1) for undirected"
        )
        
        # Test 6: Mixed Directed/Undirected
        results_6 = tester.run_test(
            6,
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]-(c:User)
            WHERE a.user_id = 1
            RETURN a.user_id, b.user_id, c.user_id
            ORDER BY b.user_id, c.user_id
            """,
            "Mixed Directed/Undirected",
            "What filters apply when mixing directions?",
            "Check if (1,2,2) appears (b != c for undirected part?)"
        )
        
        # Test 7: Named Intermediate Nodes
        results_7 = tester.run_test(
            7,
            """
            MATCH (a:User)-[:FOLLOWS]-(b:User)-[:FOLLOWS]-(c:User)
            WHERE a.user_id = 1
            RETURN a.user_id, b.user_id, c.user_id
            ORDER BY b.user_id, c.user_id
            """,
            "Named Intermediate Nodes (Undirected)",
            "Must a, b, c all be different nodes?",
            "Check if a==c, a==b, or b==c ever occur"
        )
        
        # Test 8: Multiple MATCH Clauses - No Uniqueness
        results_8 = tester.run_test(
            8,
            """
            MATCH (a:User)-[:FOLLOWS]-(b:User)
            MATCH (b)-[:FOLLOWS]-(c:User)
            WHERE a.user_id = 1
            RETURN DISTINCT c.user_id
            ORDER BY c.user_id
            """,
            "Multiple MATCH Clauses",
            "Can a == c? (should be YES - no cross-MATCH uniqueness)",
            "Should allow user_id=1 in results"
        )
        
        # Test 9: Unbounded Variable-Length (*1..)
        results_9 = tester.run_test(
            9,
            """
            MATCH (a:User)-[:FOLLOWS*1..]->(c:User)
            WHERE a.user_id = 1
            RETURN a.user_id, c.user_id, length([(a)-[r:FOLLOWS*]->(c) | r]) as hops
            ORDER BY c.user_id, hops
            LIMIT 10
            """,
            "Unbounded Variable-Length (*1..)",
            "Does Neo4j prevent infinite loops? What's default max depth?",
            "Check max hops returned and if cycle appears"
        )
        
        # Test 10: Relationship Uniqueness
        results_10 = tester.run_test(
            10,
            """
            MATCH (a:User)-[r1:FOLLOWS]-(b:User)-[r2:FOLLOWS]-(c:User)
            WHERE a.user_id = 1 AND id(r1) = id(r2)
            RETURN a.user_id, b.user_id, c.user_id
            """,
            "Relationship Uniqueness",
            "Should this return empty? (r1 and r2 must be different)",
            "Empty results - relationship uniqueness guaranteed"
        )
        
        # Summary
        print("\n" + "="*70)
        print("SUMMARY OF FINDINGS")
        print("="*70)
        
        print("\n📊 Results Analysis:\n")
        
        # Analyze Test 1
        if results_1:
            has_cycle = any(r['a.user_id'] == r['c.user_id'] for r in results_1)
            print(f"1. Directed *2: {'❌ ALLOWS cycles' if has_cycle else '✅ PREVENTS cycles'}")
            print(f"   {len(results_1)} results total")
        
        # Analyze Test 2
        if results_2:
            has_cycle = any(r['a.user_id'] == r['c.user_id'] for r in results_2)
            print(f"2. Explicit 2-hop: {'❌ ALLOWS cycles' if has_cycle else '✅ PREVENTS cycles'}")
            print(f"   {len(results_2)} results total")
        
        # Analyze Test 3
        if results_3:
            has_self = any(r['a.user_id'] == r['b.user_id'] for r in results_3)
            print(f"3. Undirected 1-hop: {'❌ ALLOWS a==b' if has_self else '✅ ENFORCES a!=b'}")
        
        # Analyze Test 4
        if results_4:
            has_self = any(r['fof.user_id'] == 1 for r in results_4)
            print(f"4. Friends-of-Friends: {'❌ Returns self (BUG)' if has_self else '✅ Excludes self'}")
        
        # Analyze Test 5
        if results_5:
            has_self = any(r['a.user_id'] == r['c.user_id'] for r in results_5)
            print(f"5. Undirected *2: {'❌ ALLOWS cycles' if has_self else '✅ PREVENTS cycles'}")
        
        # Analyze Test 6
        if results_6:
            print(f"6. Mixed direction: {len(results_6)} results")
            has_b_eq_c = any(r['b.user_id'] == r['c.user_id'] for r in results_6)
            print(f"   {'❌ Allows b==c' if has_b_eq_c else '✅ Enforces b!=c for undirected part'}")
        
        # Analyze Test 7
        if results_7:
            print(f"7. Named intermediates: {len(results_7)} results")
            has_a_eq_c = any(r['a.user_id'] == r['c.user_id'] for r in results_7)
            has_a_eq_b = any(r['a.user_id'] == r['b.user_id'] for r in results_7)
            has_b_eq_c = any(r['b.user_id'] == r['c.user_id'] for r in results_7)
            print(f"   a==c: {'Yes' if has_a_eq_c else 'No'}, a==b: {'Yes' if has_a_eq_b else 'No'}, b==c: {'Yes' if has_b_eq_c else 'No'}")
        
        # Analyze Test 8
        if results_8:
            has_self = any(r['c.user_id'] == 1 for r in results_8)
            print(f"8. Multi-MATCH: {'✅ Allows a==c' if has_self else '❌ Prevents a==c (unexpected)'}")
        
        # Analyze Test 9
        if results_9:
            max_hops = max(r.get('hops', 0) for r in results_9) if results_9 else 0
            print(f"9. Unbounded *1..: Max hops = {max_hops}")
            has_self = any(r['a.user_id'] == r['c.user_id'] for r in results_9)
            print(f"   {'Allows cycles' if has_self else 'Prevents cycles'}")
        
        # Analyze Test 10
        if results_10 is not None:
            print(f"10. Relationship uniqueness: {'✅ ENFORCED' if len(results_10) == 0 else '❌ NOT ENFORCED'}")
        
        print("\n" + "="*70)
        print("CLICKGRAPH COMPATIBILITY RECOMMENDATIONS")
        print("="*70)
        
        print("\n🎯 Based on these findings, ClickGraph should:\n")
        
        if results_1 and not any(r['a.user_id'] == r['c.user_id'] for r in results_1):
            print("✅ KEEP cycle prevention for directed variable-length (*2)")
        else:
            print("❌ REMOVE cycle prevention for directed variable-length (*2)")
        
        if results_2 and not any(r['a.user_id'] == r['c.user_id'] for r in results_2):
            print("✅ ADD cycle prevention for explicit directed patterns")
        else:
            print("❌ NO cycle prevention needed for explicit directed patterns")
        
        if results_4 and not any(r['fof.user_id'] == 1 for r in results_4):
            print("✅ FIX friends-of-friends to exclude start node (OpenCypher spec)")
        
        print("\n📝 See notes/neo4j-semantics-testing-plan.md for implementation details")
        
    finally:
        tester.close()
        print("\n✅ Neo4j connection closed")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

