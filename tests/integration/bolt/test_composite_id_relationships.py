#!/usr/bin/env python3
"""
Test Bug #3 Fix: Composite ID Relationships

Tests that relationships correctly generate elementIds when connecting
to nodes with composite IDs.
"""

from neo4j import GraphDatabase
import os

# Configuration
BOLT_URI = "bolt://localhost:7688"
AUTH = ("neo4j", "password")

def test_composite_id_relationships():
    """Test relationship returns with composite IDs"""
    driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
    
    try:
        with driver.session() as session:
            print("=" * 60)
            print("Test: Composite ID Relationships")
            print("=" * 60)
            
            # Test 1: Return relationship connecting composite ID nodes
            print("\n1. Testing relationship with composite IDs...")
            query = """
            USE social_benchmark
            MATCH (u:User)-[r:FOLLOWS]->(u2:User) 
            RETURN r 
            LIMIT 1
            """
            
            result = session.run(query)
            record = result.single()
            
            if record:
                rel = record['r']
                print(f"✅ Relationship returned: {type(rel)}")
                print(f"   Type: {rel.type}")
                print(f"   Element ID: {rel.element_id}")
                print(f"   Start Element ID: {rel.start_node.element_id}")
                print(f"   End Element ID: {rel.end_node.element_id}")
                
                # Verify format
                if '->' in rel.element_id:
                    print(f"✅ Relationship elementId has correct format")
                else:
                    print(f"❌ Relationship elementId missing '->' separator")
                    
                # Check for composite ID support (if IDs have '|')
                if '|' in rel.start_node.element_id or '|' in rel.end_node.element_id:
                    print(f"✅ Composite ID detected in node elementIds")
                else:
                    print(f"ℹ️  Single column IDs (composite test needs different schema)")
            else:
                print("❌ No relationship returned")
            
            # Test 2: Mixed node and relationship return
            print("\n2. Testing mixed return with relationships...")
            query = """
            USE social_benchmark
            MATCH (u:User)-[r:FOLLOWS]->(u2:User)
            RETURN u, r, u2
            LIMIT 1
            """
            
            result = session.run(query)
            record = result.single()
            
            if record:
                print(f"✅ Mixed return successful")
                print(f"   From node: {record['u'].element_id}")
                print(f"   Relationship: {record['r'].element_id}")
                print(f"   To node: {record['u2'].element_id}")
                
                # Verify consistency
                rel = record['r']
                from_node = record['u']
                to_node = record['u2']
                
                if rel.start_node.element_id == from_node.element_id:
                    print(f"✅ Start node elementId matches")
                else:
                    print(f"❌ Start node elementId mismatch!")
                    print(f"   Rel start: {rel.start_node.element_id}")
                    print(f"   From node: {from_node.element_id}")
                    
                if rel.end_node.element_id == to_node.element_id:
                    print(f"✅ End node elementId matches")
                else:
                    print(f"❌ End node elementId mismatch!")
                    print(f"   Rel end: {rel.end_node.element_id}")
                    print(f"   To node: {to_node.element_id}")
            else:
                print("❌ No results returned")
            
            print("\n" + "=" * 60)
            print("Test Complete")
            print("=" * 60)
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.close()

if __name__ == "__main__":
    test_composite_id_relationships()
