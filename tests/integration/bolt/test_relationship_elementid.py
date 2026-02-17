#!/usr/bin/env python3
"""
Test Bug #3 Fix: TRUE Composite ID Test

Creates a mock scenario to verify composite ID handling
"""

from neo4j import GraphDatabase

BOLT_URI = "bolt://localhost:7688"
AUTH = ("neo4j", "password")

def test_relationship_structure():
    """Test the internal structure to verify composite ID support"""
    driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
    
    try:
        with driver.session() as session:
            print("=" * 60)
            print("Relationship ElementID Structure Test")
            print("=" * 60)
            
            # Test with a known relationship
            query = """
            USE social_integration
            MATCH (u1:User {user_id: 1})-[r:FOLLOWS]->(u2:User {user_id: 2})
            RETURN u1, r, u2
            """
            
            result = session.run(query)
            record = result.single()
            
            if record:
                u1 = record['u1']
                r = record['r']
                u2 = record['u2']
                
                print(f"\n✅ Query successful!")
                print(f"\nFrom Node (u1):")
                print(f"   ElementID: {u1.element_id}")
                print(f"   Properties: {dict(u1)}")
                
                print(f"\nRelationship (r):")
                print(f"   Type: {r.type}")
                print(f"   ElementID: {r.element_id}")
                print(f"   Start ElementID: {r.start_node.element_id}")
                print(f"   End ElementID: {r.end_node.element_id}")
                print(f"   Properties: {dict(r)}")
                
                print(f"\nTo Node (u2):")
                print(f"   ElementID: {u2.element_id}")
                print(f"   Properties: {dict(u2)}")
                
                # Verify consistency
                print(f"\n{'=' * 60}")
                print("Consistency Checks:")
                print(f"{'=' * 60}")
                
                checks = [
                    (r.start_node.element_id == u1.element_id, 
                     "Start elementId matches from node",
                     f"Expected: {u1.element_id}, Got: {r.start_node.element_id}"),
                    (r.end_node.element_id == u2.element_id,
                     "End elementId matches to node",
                     f"Expected: {u2.element_id}, Got: {r.end_node.element_id}"),
                    ('->' in r.element_id,
                     "Relationship elementId has arrow separator",
                     f"ElementID: {r.element_id}"),
                    (r.element_id.startswith(f"{r.type}:"),
                     "Relationship elementId starts with type",
                     f"ElementID: {r.element_id}"),
                ]
                
                all_passed = True
                for passed, description, details in checks:
                    if passed:
                        print(f"✅ {description}")
                    else:
                        print(f"❌ {description}")
                        print(f"   {details}")
                        all_passed = False
                
                print(f"\n{'=' * 60}")
                if all_passed:
                    print("✅ ALL CHECKS PASSED - Bug #3 FIX VERIFIED!")
                else:
                    print("❌ SOME CHECKS FAILED")
                print(f"{'=' * 60}")
            else:
                print("❌ No results returned")
                
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.close()

if __name__ == "__main__":
    test_relationship_structure()
