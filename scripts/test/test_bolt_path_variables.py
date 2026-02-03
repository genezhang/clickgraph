#!/usr/bin/env python3
"""
Test path variables via Bolt protocol.
Tests that UNION path variable queries work through Neo4j Browser connection.
"""

from neo4j import GraphDatabase
import sys

def test_bolt_path_variables():
    """Test path variable queries via Bolt protocol."""
    
    # Connect to ClickGraph via Bolt protocol
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("", ""))  # ClickGraph doesn't require auth by default
    
    try:
        with driver.session() as session:
            print("✓ Connected to ClickGraph via Bolt protocol")
            
            # Test 1: UNION path variable (the feature we just fixed!)
            print("\n📍 Test 1: MATCH p=()-->() RETURN p LIMIT 3")
            result = session.run("MATCH p=()-->() RETURN p LIMIT 3")
            records = list(result)
            print(f"   Results: {len(records)} records")
            for i, record in enumerate(records):
                path = record["p"]
                print(f"   [{i}] p = {path}")
            
            if len(records) > 0:
                print("   ✅ UNION path variables working via Bolt!")
            else:
                print("   ❌ No results returned")
                return False
            
            # Test 2: Typed path variable (should still work)
            print("\n📍 Test 2: MATCH p=(a:User)-[:FOLLOWS]->(b:User) RETURN p LIMIT 2")
            result = session.run("MATCH p=(a:User)-[:FOLLOWS]->(b:User) RETURN p LIMIT 2")
            records = list(result)
            print(f"   Results: {len(records)} records")
            for i, record in enumerate(records):
                path = record["p"]
                print(f"   [{i}] p = {path}")
            
            if len(records) > 0:
                print("   ✅ Typed path variables working via Bolt!")
            else:
                print("   ❌ No results returned")
                return False
            
            # Test 3: Count query (sanity check)
            print("\n📍 Test 3: MATCH ()-->() RETURN count(*) AS total")
            result = session.run("MATCH ()-->() RETURN count(*) AS total")
            record = result.single()
            total = record["total"]
            print(f"   Total relationships: {total}")
            
            if total > 0:
                print("   ✅ Count query working!")
            else:
                print("   ❌ Count returned 0")
                return False
            
            print("\n🎉 All Bolt protocol tests passed!")
            print("\n📝 Neo4j Browser Connection:")
            print("   URL: bolt://localhost:7687")
            print("   Auth: No authentication required")
            print("   Try: MATCH p=()-->() RETURN p LIMIT 10")
            
            return True
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        driver.close()

if __name__ == "__main__":
    success = test_bolt_path_variables()
    sys.exit(0 if success else 1)
