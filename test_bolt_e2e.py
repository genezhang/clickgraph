#!/usr/bin/env python3
"""
ClickGraph Bolt Protocol Integration Tests
Tests Neo4j Python driver connecting to ClickGraph via Bolt protocol
"""

from neo4j import GraphDatabase
import sys

def print_section(title):
    print("\n" + "="*60)
    print(title)
    print("="*60)

def test_connection():
    """Test 1: Basic connection and authentication"""
    print_section("TEST 1: Basic Connection")
    try:
        driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password"),
            database="ecommerce_demo"  # Specify the schema to use
        )
        driver.verify_connectivity()
        print("✅ Connection established successfully with ecommerce_demo schema!")
        driver.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def test_simple_query(driver):
    """Test 2: Simple query execution with schema"""
    print_section("TEST 2: Simple Customer Query")
    try:
        with driver.session(database="ecommerce_demo") as session:
            # Use the loaded schema
            result = session.run("MATCH (c:Customer) RETURN c.first_name AS name LIMIT 3")
            records = list(result)
            
            if records:
                print(f"✅ Query successful! Retrieved {len(records)} customers:")
                for record in records:
                    print(f"   - {record['name']}")
                return True
            else:
                print("⚠️  Query returned no results")
                return False
    except Exception as e:
        print(f"❌ Query failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_graph_query(driver):
    """Test 3: Graph traversal query"""
    print_section("TEST 3: Graph Traversal (Customer -> Product)")
    try:
        with driver.session(database="ecommerce_demo") as session:
            result = session.run("""
                MATCH (c:Customer)-[p:PURCHASED]->(prod:Product)
                RETURN c.first_name AS customer, prod.name AS product, p.quantity AS qty
                LIMIT 5
            """)
            records = list(result)
            
            if records:
                print(f"✅ Graph query successful! Retrieved {len(records)} purchases:")
                for record in records:
                    print(f"   - {record['customer']} bought {record['qty']}x {record['product']}")
                return True
            else:
                print("⚠️  Query returned no results")
                return False
    except Exception as e:
        print(f"❌ Graph query failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_aggregation(driver):
    """Test 4: Aggregation query"""
    print_section("TEST 4: Aggregation Query")
    try:
        with driver.session(database="ecommerce_demo") as session:
            result = session.run("""
                MATCH (c:Customer)-[p:PURCHASED]->(prod:Product)
                RETURN c.first_name AS customer, 
                       COUNT(prod) AS products_purchased,
                       SUM(p.total_amount) AS total_spent
                ORDER BY total_spent DESC
                LIMIT 5
            """)
            records = list(result)
            
            if records:
                print(f"✅ Aggregation successful! Top customers:")
                for record in records:
                    print(f"   - {record['customer']}: {record['products_purchased']} products, ${record['total_spent']:.2f}")
                return True
            else:
                print("⚠️  Query returned no results")
                return False
    except Exception as e:
        print(f"❌ Aggregation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_error_handling(driver):
    """Test 5: Error handling"""
    print_section("TEST 5: Error Handling")
    try:
        with driver.session(database="ecommerce_demo") as session:
            # Try an invalid query
            result = session.run("MATCH (x:NonExistentLabel) RETURN x")
            list(result)
        print("⚠️  Expected an error but query succeeded")
        return False
    except Exception as e:
        print(f"✅ Error correctly caught: {type(e).__name__}")
        print(f"   Message: {str(e)[:150]}")
        return True

def main():
    print("="*60)
    print("ClickGraph Bolt Protocol Integration Tests")
    print("="*60)
    print("\nPrerequisites:")
    print("  1. ClickGraph server running: cargo run --release --bin clickgraph")
    print("  2. Demo data loaded: python load_schema.py")
    print("  3. ClickHouse with demo data: docker exec -i clickgraph-clickhouse ...")
    
    # Test connection first
    if not test_connection():
        print("\n❌ Connection test failed. Exiting.")
        return 1
    
    # Create driver for subsequent tests
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "password"),
        database="ecommerce_demo"  # Use the loaded schema
    )
    
    try:
        tests = [
            ("Simple Query", test_simple_query),
            ("Graph Query", test_graph_query),
            ("Aggregation", test_aggregation),
            ("Error Handling", test_error_handling),
        ]
        
        results = []
        for test_name, test_func in tests:
            try:
                result = test_func(driver)
                results.append((test_name, result))
            except Exception as e:
                print(f"\n❌ Test '{test_name}' crashed: {e}")
                results.append((test_name, False))
        
        # Print summary
        print_section("TEST SUMMARY")
        passed = sum(1 for _, result in results if result)
        total = len(results) + 1  # +1 for connection test
        
        print("✅ PASS: Basic Connection")
        for test_name, result in results:
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status}: {test_name}")
        
        print(f"\nTotal: {passed + 1}/{total} tests passed")
        
        if passed + 1 < total:
            print("\n⚠️  Some test(s) failed. Check logs above.")
            return 1
        else:
            print("\n🎉 All tests passed!")
            return 0
            
    finally:
        driver.close()

if __name__ == "__main__":
    sys.exit(main())
