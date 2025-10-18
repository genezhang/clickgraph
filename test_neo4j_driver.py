#!/usr/bin/env python3
"""
Test ClickGraph using the official neo4j Python driver.
"""

try:
    from neo4j import GraphDatabase
    
    def test_connection():
        """Test basic connection to ClickGraph via Bolt."""
        uri = "bolt://localhost:7687"
        
        # Try without auth first
        try:
            driver = GraphDatabase.driver(uri, auth=None)
            with driver.session() as session:
                result = session.run("RETURN 1 AS number")
                record = result.single()
                print(f"✅ Connection successful! Result: {record['number']}")
            driver.close()
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def test_cypher_query():
        """Test a simple Cypher query."""
        uri = "bolt://localhost:7687"
        
        try:
            driver = GraphDatabase.driver(uri, auth=None)
            with driver.session() as session:
                # Test simple node query
                query = "MATCH (u:User) RETURN u.user_id LIMIT 1"
                print(f"\n📝 Testing query: {query}")
                
                result = session.run(query)
                records = list(result)
                
                if records:
                    print(f"✅ Query successful! Got {len(records)} results")
                    for record in records:
                        print(f"   Result: {dict(record)}")
                    return True
                else:
                    print("⚠️ Query returned no results")
                    return True  # Still successful, just no data
                    
        except Exception as e:
            print(f"❌ Query failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            try:
                driver.close()
            except:
                pass
    
    if __name__ == "__main__":
        print("Testing ClickGraph with Neo4j Python Driver")
        print("=" * 50)
        
        print("\n1. Testing basic connection...")
        if test_connection():
            print("\n2. Testing Cypher query...")
            test_cypher_query()
        else:
            print("\n❌ Basic connection failed, skipping query test")
            
except ImportError:
    print("❌ neo4j Python package not installed")
    print("Install with: pip install neo4j")
    exit(1)
