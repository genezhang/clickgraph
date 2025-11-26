"""
Critical Test: Distinguish Relationship Uniqueness vs Node Uniqueness

The key insight: We need a graph where a node CAN be reached in 2 hops
if we DON'T enforce node uniqueness (only relationship uniqueness).
"""

from neo4j import GraphDatabase

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "testpassword"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Create a test topology where node uniqueness matters
with driver.session() as session:
    # Clear data
    session.run("MATCH (n) DETACH DELETE n")
    
    # Create a graph where Alice can reach herself in 2 hops via DIFFERENT relationships:
    # Alice -> Bob -> Alice  (two different FOLLOWS relationships)
    # Alice -> Charlie (one relationship)
    #
    # If only relationship uniqueness: Alice-Bob-Alice IS allowed (different rel instances)
    # If node uniqueness too: Alice-Bob-Alice NOT allowed (Alice appears twice)
    
    session.run("""
        CREATE (alice:User {user_id: 1, name: 'Alice'})
        CREATE (bob:User {user_id: 2, name: 'Bob'})
        CREATE (charlie:User {user_id: 3, name: 'Charlie'})
        CREATE (alice)-[:FOLLOWS]->(bob)
        CREATE (bob)-[:FOLLOWS]->(alice)
        CREATE (alice)-[:FOLLOWS]->(charlie)
    """)
    
    print("="*80)
    print("Test Setup: Can Alice reach herself in 2 directed hops?")
    print("="*80)
    print("Graph: Alice -> Bob -> Alice (cycle!)")
    print("       Alice -> Charlie")
    print()
    
    # Test 1: Directed 2-hop - does Neo4j allow cycles?
    print("Test 1: Directed 2-Hop Pattern")
    print("-"*80)
    query1 = """
        MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
        WHERE a.user_id = 1
        RETURN a.name AS start, b.name AS intermediate, c.name AS end
    """
    print("Query:", query1)
    result1 = list(session.run(query1))
    print("Results:")
    for r in result1:
        print(f"  {r['start']} -> {r['intermediate']} -> {r['end']}")
    
    print("\nAnalysis:")
    has_cycle = any(r['start'] == r['end'] for r in result1)
    if has_cycle:
        print("  [!!] CYCLE ALLOWED: Alice->Bob->Alice appears!")
        print("  => Neo4j only enforces RELATIONSHIP uniqueness")
    else:
        print("  [OK] NO CYCLE: Alice->Bob->Alice does NOT appear")
        print("  => Neo4j enforces NODE uniqueness too!")
    
    # Test 2: Undirected 2-hop
    print("\n" + "="*80)
    print("Test 2: Undirected 2-Hop Pattern")
    print("-"*80)
    query2 = """
        MATCH (a:User)-[:FOLLOWS]-(b:User)-[:FOLLOWS]-(c:User)
        WHERE a.user_id = 1
        RETURN a.name AS start, b.name AS intermediate, c.name AS end
    """
    print("Query:", query2)
    result2 = list(session.run(query2))
    print("Results:")
    for r in result2:
        print(f"  {r['start']} - {r['intermediate']} - {r['end']}")
    
    print("\nAnalysis:")
    has_cycle = any(r['start'] == r['end'] for r in result2)
    if has_cycle:
        print("  [!!] CYCLE ALLOWED: Alice-Bob-Alice appears!")
    else:
        print("  [OK] NO CYCLE: Alice-Bob-Alice does NOT appear")
    
    # Test 3: With named relationships - check if same rel is used
    print("\n" + "="*80)
    print("Test 3: Named Relationships - Can r1 == r2?")
    print("-"*80)
    query3 = """
        MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User)
        WHERE a.user_id = 1
        RETURN a.name AS start, b.name AS intermediate, c.name AS end,
               elementId(r1) AS r1_id, elementId(r2) AS r2_id,
               elementId(r1) = elementId(r2) AS same_rel
    """
    print("Query:", query3)
    result3 = list(session.run(query3))
    print("Results:")
    for r in result3:
        print(f"  {r['start']} -> {r['intermediate']} -> {r['end']}")
        print(f"    r1_id: {r['r1_id']}, r2_id: {r['r2_id']}, same_rel: {r['same_rel']}")
    
    print("\nConclusion:")
    print("="*80)
    if has_cycle:
        print("Neo4j allows cycles when relationship instances are different")
        print("=> Only RELATIONSHIP uniqueness enforced")
    else:
        print("Neo4j prevents cycles even with different relationship instances")
        print("=> BOTH relationship AND node uniqueness enforced")

driver.close()
