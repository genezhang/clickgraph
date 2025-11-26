"""
Test: Does Neo4j enforce relationship uniqueness for UNDIRECTED patterns?

Critical Question: Can the same relationship edge be traversed twice in opposite directions?
Example: If (Alice)-[:FOLLOWS]->(Bob) exists, can we match:
  (Alice)--(Bob)--(Alice) using the SAME edge twice (once in each direction)?
"""

from neo4j import GraphDatabase
import os

# Neo4j connection
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "testpassword"))

def setup_simple_graph(tx):
    """Create minimal graph: Alice -> Bob (one directed relationship)"""
    tx.run("MATCH (n) DETACH DELETE n")  # Clean slate
    tx.run("""
        CREATE (alice:User {user_id: 1, name: 'Alice'})
        CREATE (bob:User {user_id: 2, name: 'Bob'})
        CREATE (alice)-[:FOLLOWS {created_at: '2024-01-01'}]->(bob)
    """)
    
def test_undirected_2hop(tx):
    """Test: Can we traverse (Alice)--(Bob)--(Alice) with only one edge?"""
    result = tx.run("""
        MATCH (a:User)-[r1]-(b:User)-[r2]-(c:User)
        WHERE a.user_id = 1
        RETURN a.name AS a_name, b.name AS b_name, c.name AS c_name,
               id(r1) AS r1_id, id(r2) AS r2_id, id(r1) = id(r2) AS same_rel
        ORDER BY a_name, b_name, c_name
    """)
    return [dict(record) for record in result]

def test_directed_2hop(tx):
    """Control: Directed pattern shouldn't match (only one edge, one direction)"""
    result = tx.run("""
        MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User)
        WHERE a.user_id = 1
        RETURN a.name AS a_name, b.name AS b_name, c.name AS c_name
    """)
    return [dict(record) for record in result]

print("="*80)
print("UNDIRECTED RELATIONSHIP UNIQUENESS TEST")
print("="*80)
print()
print("Graph: Alice -[:FOLLOWS]-> Bob (single directed edge)")
print()

with driver.session() as session:
    # Setup
    session.execute_write(setup_simple_graph)
    print("✓ Graph created")
    print()
    
    # Test 1: Directed (control)
    print("Test 1: Directed Pattern (Control)")
    print("Query: MATCH (a)-[r1:FOLLOWS]->(b)-[r2:FOLLOWS]->(c) WHERE a.user_id = 1")
    results = session.execute_read(test_directed_2hop)
    print(f"Results: {len(results)} matches")
    for r in results:
        print(f"  {r['a_name']} -> {r['b_name']} -> {r['c_name']}")
    print()
    print("Expected: 0 matches (need 2 edges, only have 1)")
    print("Actual: ✓ CORRECT" if len(results) == 0 else "✗ UNEXPECTED")
    print()
    
    # Test 2: Undirected (THE CRITICAL TEST)
    print("Test 2: Undirected Pattern (CRITICAL)")
    print("Query: MATCH (a)-[r1]-(b)-[r2]-(c) WHERE a.user_id = 1")
    print()
    print("Question: Can we match Alice-Bob-Alice using the SAME edge twice?")
    print("  - r1: Alice-Bob (using FOLLOWS in forward direction)")
    print("  - r2: Bob-Alice (using SAME FOLLOWS in reverse direction)")
    print()
    results = session.execute_read(test_undirected_2hop)
    print(f"Results: {len(results)} matches")
    for r in results:
        print(f"  {r['a_name']} - {r['b_name']} - {r['c_name']}")
        print(f"    r1_id: {r['r1_id']}, r2_id: {r['r2_id']}, same: {r['same_rel']}")
    print()
    
    if len(results) == 0:
        print("✓ Neo4j PREVENTS same edge reuse even in undirected patterns!")
        print("  => Relationship uniqueness enforced for undirected patterns too")
    else:
        same_edge = any(r['same_rel'] for r in results)
        if same_edge:
            print("✗ Neo4j ALLOWS same edge reuse in undirected patterns!")
            print("  => We need to add explicit relationship uniqueness filters!")
        else:
            print("✓ All matches use different edges (checked via id(r1) != id(r2))")

driver.close()

print()
print("="*80)
print("IMPLICATIONS FOR CLICKGRAPH")
print("="*80)
print()
print("If Neo4j prevents same edge reuse in undirected patterns:")
print("  → SQL structure alone is NOT sufficient")
print("  → We need explicit filters to prevent relationship reuse")
print()
print("For undirected patterns with UNION:")
print("  SELECT ... FROM follows r1 WHERE r1.src = a AND r1.dst = b")
print("  UNION ALL")
print("  SELECT ... FROM follows r1 WHERE r1.dst = a AND r1.src = b")
print()
print("  The SAME physical row could be used by r1 and r2 differently!")
print("  Example: row (1, 2) could be:")
print("    - r1: src=1, dst=2 (forward)")
print("    - r2: src=2, dst=1 (reverse) -- SAME ROW, interpreted backwards!")
print()
print("Without relationship IDs in ClickHouse, we need composite key filters:")
print("  WHERE NOT (r1.src = r2.src AND r1.dst = r2.dst)")
print("  AND NOT (r1.src = r2.dst AND r1.dst = r2.src)  -- prevent reverse match")
