"""
Compare ClickGraph vs Neo4j behavior for friend-of-friend duplicate results.

This script verifies whether Neo4j returns duplicates for the same query pattern
that users reported seeing duplicates in ClickGraph.

Test Query:
    MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
    WHERE a.name = "Alice" AND b.name = "Bob" 
    RETURN mutual.name

Expected behavior investigation:
    - Does Neo4j return duplicates without DISTINCT?
    - Is this a bug in ClickGraph or expected Cypher semantics?
"""

import sys
import time
from neo4j import GraphDatabase


# Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "testpassword"


def setup_neo4j_data(driver):
    """Create the same test data structure as our integration tests."""
    with driver.session() as session:
        # Clear existing data
        session.run("MATCH (n) DETACH DELETE n")
        
        # Create users
        session.run("""
            CREATE (alice:User {user_id: 1, name: 'Alice', age: 30})
            CREATE (bob:User {user_id: 2, name: 'Bob', age: 25})
            CREATE (charlie:User {user_id: 3, name: 'Charlie', age: 35})
            CREATE (diana:User {user_id: 4, name: 'Diana', age: 28})
            CREATE (eve:User {user_id: 5, name: 'Eve', age: 32})
        """)
        
        # Create relationships matching test_integration data
        session.run("""
            MATCH (alice:User {name: 'Alice'})
            MATCH (bob:User {name: 'Bob'})
            MATCH (charlie:User {name: 'Charlie'})
            MATCH (diana:User {name: 'Diana'})
            MATCH (eve:User {name: 'Eve'})
            
            CREATE (alice)-[:FOLLOWS {since: '2023-01-01'}]->(bob)
            CREATE (alice)-[:FOLLOWS {since: '2023-01-15'}]->(charlie)
            CREATE (bob)-[:FOLLOWS {since: '2023-02-01'}]->(charlie)
            CREATE (charlie)-[:FOLLOWS {since: '2023-02-15'}]->(diana)
            CREATE (diana)-[:FOLLOWS {since: '2023-03-01'}]->(eve)
            CREATE (bob)-[:FOLLOWS {since: '2023-03-15'}]->(diana)
        """)
        
        print("✓ Test data loaded into Neo4j")
        
        # Verify data
        result = session.run("MATCH (u:User) RETURN count(u) as count")
        user_count = result.single()["count"]
        
        result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as count")
        rel_count = result.single()["count"]
        
        print(f"  - {user_count} users")
        print(f"  - {rel_count} FOLLOWS relationships")


def test_mutual_friends_query(driver):
    """
    Test the exact query from user bug report.
    
    Query: MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
           WHERE a.name = "Alice" AND b.name = "Bob" 
           RETURN mutual.name
    
    Graph structure:
        Alice --FOLLOWS--> Bob
        Alice --FOLLOWS--> Charlie
        Bob --FOLLOWS--> Charlie
    
    Pattern analysis:
        - Alice and Bob both follow Charlie
        - This creates one path: Alice->Charlie<-Bob
        - Expected: 1 result (Charlie)
        - Question: Does Neo4j return it once or multiple times?
    """
    print("\n" + "="*70)
    print("TEST 1: Mutual Friends (User Bug Report)")
    print("="*70)
    
    query = """
        MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
        WHERE a.name = 'Alice' AND b.name = 'Bob' 
        RETURN mutual.name
    """
    
    with driver.session() as session:
        print("\nQuery WITHOUT DISTINCT:")
        print(query)
        
        result = session.run(query)
        records = list(result)
        
        print(f"\nResults count: {len(records)}")
        print("Results:")
        for i, record in enumerate(records, 1):
            print(f"  {i}. {record['mutual.name']}")
        
        if len(records) > 1:
            print("\n❌ DUPLICATES FOUND - Multiple rows returned for same result")
        else:
            print("\n✅ NO DUPLICATES - Single result as expected")
        
        # Test with DISTINCT
        query_distinct = query.replace("RETURN mutual.name", "RETURN DISTINCT mutual.name")
        
        print("\nQuery WITH DISTINCT:")
        print(query_distinct)
        
        result = session.run(query_distinct)
        records_distinct = list(result)
        
        print(f"\nResults count: {len(records_distinct)}")
        print("Results:")
        for i, record in enumerate(records_distinct, 1):
            print(f"  {i}. {record['mutual.name']}")
        
        return len(records), len(records_distinct)


def test_bidirectional_pattern(driver):
    """
    Test bidirectional relationship pattern.
    
    Query: MATCH (a:User)-[:FOLLOWS]-(mutual:User)-[:FOLLOWS]-(b:User)
           WHERE a.name = 'Alice' AND b.name = 'Charlie'
           RETURN mutual.name
    
    This tests undirected relationships (both directions).
    """
    print("\n" + "="*70)
    print("TEST 2: Bidirectional Pattern")
    print("="*70)
    
    query = """
        MATCH (a:User)-[:FOLLOWS]-(mutual:User)-[:FOLLOWS]-(b:User)
        WHERE a.name = 'Alice' AND b.name = 'Charlie'
        RETURN mutual.name
    """
    
    with driver.session() as session:
        print("\nQuery WITHOUT DISTINCT:")
        print(query)
        
        result = session.run(query)
        records = list(result)
        
        print(f"\nResults count: {len(records)}")
        print("Results:")
        for i, record in enumerate(records, 1):
            print(f"  {i}. {record['mutual.name']}")
        
        # Test with DISTINCT
        query_distinct = query.replace("RETURN mutual.name", "RETURN DISTINCT mutual.name")
        
        print("\nQuery WITH DISTINCT:")
        print(query_distinct)
        
        result = session.run(query_distinct)
        records_distinct = list(result)
        
        print(f"\nResults count: {len(records_distinct)}")
        print("Results:")
        for i, record in enumerate(records_distinct, 1):
            print(f"  {i}. {record['mutual.name']}")
        
        return len(records), len(records_distinct)


def test_friend_of_friend(driver):
    """
    Test classic friend-of-friend query.
    
    Query: MATCH (me:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof:User)
           WHERE me.name = 'Alice'
           RETURN fof.name
    
    This should show all people that Alice's friends follow.
    """
    print("\n" + "="*70)
    print("TEST 3: Friend-of-Friend (Classic Pattern)")
    print("="*70)
    
    query = """
        MATCH (me:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof:User)
        WHERE me.name = 'Alice'
        RETURN fof.name
    """
    
    with driver.session() as session:
        print("\nQuery WITHOUT DISTINCT:")
        print(query)
        
        result = session.run(query)
        records = list(result)
        
        print(f"\nResults count: {len(records)}")
        print("Results:")
        for i, record in enumerate(records, 1):
            print(f"  {i}. {record['fof.name']}")
        
        # Test with DISTINCT
        query_distinct = query.replace("RETURN fof.name", "RETURN DISTINCT fof.name")
        
        print("\nQuery WITH DISTINCT:")
        print(query_distinct)
        
        result = session.run(query_distinct)
        records_distinct = list(result)
        
        print(f"\nResults count: {len(records_distinct)}")
        print("Results:")
        for i, record in enumerate(records_distinct, 1):
            print(f"  {i}. {record['fof.name']}")
        
        return len(records), len(records_distinct)


def main():
    print("="*70)
    print("Neo4j vs ClickGraph: Duplicate Results Comparison")
    print("="*70)
    print(f"\nConnecting to Neo4j at {NEO4J_URI}...")
    
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Test connection
        driver.verify_connectivity()
        print("✓ Connected to Neo4j\n")
        
        # Setup test data
        setup_neo4j_data(driver)
        
        # Run tests
        results = {}
        results['test1'] = test_mutual_friends_query(driver)
        results['test2'] = test_bidirectional_pattern(driver)
        results['test3'] = test_friend_of_friend(driver)
        
        # Summary
        print("\n" + "="*70)
        print("SUMMARY: Neo4j Behavior")
        print("="*70)
        
        print("\nTest 1 - Mutual Friends (User Bug Report):")
        print(f"  Without DISTINCT: {results['test1'][0]} results")
        print(f"  With DISTINCT: {results['test1'][1]} results")
        if results['test1'][0] == results['test1'][1]:
            print("  ✅ Neo4j does NOT return duplicates")
        else:
            print("  ❌ Neo4j DOES return duplicates")
        
        print("\nTest 2 - Bidirectional Pattern:")
        print(f"  Without DISTINCT: {results['test2'][0]} results")
        print(f"  With DISTINCT: {results['test2'][1]} results")
        if results['test2'][0] == results['test2'][1]:
            print("  ✅ Neo4j does NOT return duplicates")
        else:
            print("  ❌ Neo4j DOES return duplicates")
        
        print("\nTest 3 - Friend-of-Friend:")
        print(f"  Without DISTINCT: {results['test3'][0]} results")
        print(f"  With DISTINCT: {results['test3'][1]} results")
        if results['test3'][0] == results['test3'][1]:
            print("  ✅ Neo4j does NOT return duplicates")
        else:
            print("  ❌ Neo4j DOES return duplicates")
        
        print("\n" + "="*70)
        print("CONCLUSION")
        print("="*70)
        
        any_duplicates = any(
            results[test][0] != results[test][1] 
            for test in ['test1', 'test2', 'test3']
        )
        
        if any_duplicates:
            print("\n📊 Neo4j CAN return duplicate results without DISTINCT")
            print("✅ This means ClickGraph behavior is CORRECT and Neo4j-compatible")
            print("📝 Users MUST use RETURN DISTINCT to avoid duplicates")
        else:
            print("\n📊 Neo4j does NOT return duplicates (de-duplicates automatically)")
            print("❌ This means ClickGraph behavior is DIFFERENT from Neo4j")
            print("🔧 ClickGraph needs to implement automatic de-duplication")
        
        driver.close()
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nTroubleshooting:")
        print("1. Start Neo4j: docker run -d -p 7687:7687 -p 7474:7474")
        print("                 -e NEO4J_AUTH=neo4j/testpassword neo4j:latest")
        print("2. Wait 30 seconds for Neo4j to start")
        print("3. Install driver: pip install neo4j")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
