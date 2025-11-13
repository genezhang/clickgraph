#!/usr/bin/env python3
"""
Test Bolt Protocol Integration with ClickGraph

This script tests the basic Bolt protocol flow:
1. Connect via Bolt driver
2. Send HELLO message
3. Send RUN message with Cypher query
4. Send PULL message to fetch results
5. Verify RECORD messages received

Requirements:
    pip install neo4j

Usage:
    python test_bolt_integration.py
"""

from neo4j import GraphDatabase
import sys

def test_basic_connection():
    """Test 1: Verify Bolt connection and handshake"""
    print("=" * 60)
    print("TEST 1: Basic Connection")
    print("=" * 60)
    
    try:
        driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password")  # ClickGraph may not enforce auth yet
        )
        driver.verify_connectivity()
        print("✅ Connection established successfully!")
        driver.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def test_simple_query():
    """Test 2: Execute simple Cypher query"""
    print("\n" + "=" * 60)
    print("TEST 2: Simple Query Execution")
    print("=" * 60)
    
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        with driver.session() as session:
            # Simple query: RETURN constant
            result = session.run("RETURN 42 AS answer")
            records = list(result)
            
            if len(records) == 1 and records[0]["answer"] == 42:
                print("✅ Simple query works!")
                print(f"   Result: {records[0]}")
                return True
            else:
                print(f"❌ Unexpected result: {records}")
                return False
                
    except Exception as e:
        print(f"❌ Query failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        driver.close()

def test_graph_query():
    """Test 3: Query graph data (assumes demo data is loaded)"""
    print("\n" + "=" * 60)
    print("TEST 3: Graph Query (User nodes)")
    print("=" * 60)
    
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        with driver.session() as session:
            # Query for User nodes
            result = session.run("MATCH (u:User) RETURN u.name AS name LIMIT 5")
            records = list(result)
            
            print(f"✅ Found {len(records)} users:")
            for record in records:
                print(f"   - {record['name']}")
            
            return len(records) > 0
                
    except Exception as e:
        print(f"❌ Graph query failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        driver.close()

def test_parameterized_query():
    """Test 4: Query with parameters"""
    print("\n" + "=" * 60)
    print("TEST 4: Parameterized Query")
    print("=" * 60)
    
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        with driver.session() as session:
            # Parameterized query
            result = session.run(
                "RETURN $name AS name, $age AS age",
                name="Alice",
                age=30
            )
            records = list(result)
            
            if (len(records) == 1 and 
                records[0]["name"] == "Alice" and 
                records[0]["age"] == 30):
                print("✅ Parameterized query works!")
                print(f"   Result: {records[0]}")
                return True
            else:
                print(f"❌ Unexpected result: {records}")
                return False
                
    except Exception as e:
        print(f"❌ Parameterized query failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        driver.close()

def test_error_handling():
    """Test 5: Verify error messages for invalid queries"""
    print("\n" + "=" * 60)
    print("TEST 5: Error Handling")
    print("=" * 60)
    
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        with driver.session() as session:
            # Invalid Cypher syntax
            result = session.run("INVALID SYNTAX HERE")
            list(result)
            print("❌ Should have thrown error for invalid syntax")
            return False
                
    except Exception as e:
        print(f"✅ Error correctly caught: {type(e).__name__}")
        print(f"   Message: {str(e)[:100]}")
        return True
    finally:
        driver.close()

def main():
    print("\n" + "=" * 60)
    print("ClickGraph Bolt Protocol Integration Tests")
    print("=" * 60)
    print("\nEnsure ClickGraph is running:")
    print("  cargo run --release --bin clickgraph")
    print("\nEnsure demo data is loaded:")
    print("  docker exec -i clickgraph-clickhouse clickhouse-client \\")
    print("    --user test_user --password test_pass < setup_demo_data.sql")
    print("")
    
    tests = [
        ("Basic Connection", test_basic_connection),
        ("Simple Query", test_simple_query),
        ("Graph Query", test_graph_query),
        ("Parameterized Query", test_parameterized_query),
        ("Error Handling", test_error_handling),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            passed = test_func()
            results.append((name, passed))
        except Exception as e:
            print(f"\n❌ Test '{name}' crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Bolt protocol integration working.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check logs above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
