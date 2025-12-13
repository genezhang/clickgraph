#!/usr/bin/env python3
"""
Test Neo4j's actual WITH scope behavior to verify semantics.

Tests:
1. Can we access variables from before WITH that weren't exported?
2. Does WITH create a scope barrier or just a projection?
"""

from neo4j import GraphDatabase
import sys

# Neo4j connection details
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")  # Update with your credentials

def test_with_scope_barrier(driver):
    """Test if WITH shields variables not in its export list"""
    
    print("=" * 70)
    print("Test 1: Accessing variable from before WITH (not exported)")
    print("=" * 70)
    
    query1 = """
    MATCH (a:Person)-[:KNOWS]->(b:Person)
    WITH a
    RETURN a.name, b.name
    LIMIT 1
    """
    
    print(f"\nQuery:\n{query1}\n")
    
    try:
        with driver.session() as session:
            result = session.run(query1)
            records = list(result)
            print(f"✗ UNEXPECTED: Query succeeded! Results: {records}")
            print("This means WITH does NOT create a scope barrier!")
    except Exception as e:
        print(f"✓ EXPECTED: Query failed with error:")
        print(f"   {e}")
        print("This confirms WITH creates a scope barrier - 'b' is not accessible")
    
    print("\n" + "=" * 70)
    print("Test 2: Using same variable name after WITH (should be NEW)")
    print("=" * 70)
    
    query2 = """
    MATCH (a:Person)-[:KNOWS]->(b:Person)
    WITH a, b.name as b_name
    MATCH (a)-[:KNOWS]->(b:Person)
    RETURN a.name, b.name, b_name
    LIMIT 3
    """
    
    print(f"\nQuery:\n{query2}\n")
    
    try:
        with driver.session() as session:
            result = session.run(query2)
            records = list(result)
            print(f"Results:")
            for i, record in enumerate(records[:3]):
                print(f"  Row {i+1}: a={record['a.name']}, b={record['b.name']}, b_name={record['b_name']}")
            
            # Check if b and b_name are different
            if records:
                if records[0]['b.name'] != records[0]['b_name']:
                    print("\n✓ CONFIRMED: Second 'b' is DIFFERENT from first 'b'")
                    print("   WITH creates scope barrier - variables can be reused with new meaning")
                else:
                    print("\n? UNCLEAR: Need more data to determine if same or different")
    except Exception as e:
        print(f"✗ Query failed: {e}")
    
    print("\n" + "=" * 70)
    print("Test 3: Multiple variables before WITH, only one exported")
    print("=" * 70)
    
    query3 = """
    MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
    WITH a, c
    RETURN a.name, b.name, c.name
    LIMIT 1
    """
    
    print(f"\nQuery:\n{query3}\n")
    
    try:
        with driver.session() as session:
            result = session.run(query3)
            records = list(result)
            print(f"✗ UNEXPECTED: Query succeeded! Results: {records}")
            print("This means WITH does NOT shield 'b' - it's still accessible!")
    except Exception as e:
        print(f"✓ EXPECTED: Query failed with error:")
        print(f"   {e}")
        print("This confirms 'b' is shielded by WITH - only a and c are accessible")

def test_with_aggregation_scope(driver):
    """Test WITH with aggregation - does it affect scope?"""
    
    print("\n" + "=" * 70)
    print("Test 4: WITH aggregation - accessing aggregated variable")
    print("=" * 70)
    
    query4 = """
    MATCH (a:Person)-[:KNOWS]->(b:Person)
    WITH a, COUNT(b) as friend_count
    RETURN a.name, friend_count, b.name
    LIMIT 1
    """
    
    print(f"\nQuery:\n{query4}\n")
    
    try:
        with driver.session() as session:
            result = session.run(query4)
            records = list(result)
            print(f"✗ UNEXPECTED: Query succeeded! Results: {records}")
    except Exception as e:
        print(f"✓ EXPECTED: Query failed with error:")
        print(f"   {e}")
        print("Confirms 'b' is not accessible after aggregation in WITH")

def main():
    print("\n" + "=" * 70)
    print("Neo4j WITH Clause Scope Semantics Test")
    print("=" * 70)
    print("\nThis script tests Neo4j's actual behavior to verify:")
    print("1. Does WITH create a scope barrier?")
    print("2. Are non-exported variables inaccessible after WITH?")
    print("3. Can variable names be reused with new meaning after WITH?")
    print()
    
    try:
        driver = GraphDatabase.driver(URI, auth=AUTH)
        driver.verify_connectivity()
        print("✓ Connected to Neo4j\n")
        
        test_with_scope_barrier(driver)
        test_with_aggregation_scope(driver)
        
        driver.close()
        
    except Exception as e:
        print(f"\n✗ Failed to connect to Neo4j: {e}")
        print("\nPlease ensure:")
        print("1. Neo4j is running (docker-compose up neo4j)")
        print("2. Update URI and AUTH in the script if needed")
        print("3. Load some test data (CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}))")
        return 1
    
    print("\n" + "=" * 70)
    print("Test Complete")
    print("=" * 70)
    return 0

if __name__ == "__main__":
    sys.exit(main())
