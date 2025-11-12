#!/usr/bin/env python3
"""Simple Bolt E2E tests without Unicode characters"""

from neo4j import GraphDatabase

def test_connection():
    """Test basic Bolt connection"""
    try:
        driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'))
        driver.verify_connectivity()
        driver.close()
        print("[PASS] Connection test")
        return True
    except Exception as e:
        print(f"[FAIL] Connection test: {e}")
        return False

def test_simple_query():
    """Test simple MATCH query"""
    try:
        driver = GraphDatabase.driver('bolt://localhost:7687')
        session = driver.session()
        result = session.run("MATCH (c:Customer) RETURN c.first_name AS name LIMIT 3")
        records = list(result)
        session.close()
        driver.close()
        
        if len(records) > 0:
            print(f"[PASS] Simple query - Got {len(records)} customers")
            return True
        else:
            print("[FAIL] Simple query - No results")
            return False
    except Exception as e:
        print(f"[FAIL] Simple query: {e}")
        return False

def test_graph_traversal():
    """Test graph pattern matching"""
    try:
        driver = GraphDatabase.driver('bolt://localhost:7687')
        session = driver.session()
        result = session.run("""
            MATCH (c:Customer)-[p:PURCHASED]->(prod:Product)
            RETURN c.first_name AS customer, prod.name AS product
            LIMIT 5
        """)
        records = list(result)
        session.close()
        driver.close()
        
        if len(records) > 0:
            print(f"[PASS] Graph traversal - Got {len(records)} purchases")
            return True
        else:
            print("[FAIL] Graph traversal - No results")
            return False
    except Exception as e:
        print(f"[FAIL] Graph traversal: {e}")
        return False

def test_aggregation():
    """Test aggregation query"""
    try:
        driver = GraphDatabase.driver('bolt://localhost:7687')
        session = driver.session()
        result = session.run("""
            MATCH (c:Customer)-[p:PURCHASED]->(prod:Product)
            RETURN c.first_name AS customer, 
                   COUNT(prod) AS product_count,
                   SUM(p.quantity) AS total_qty
            ORDER BY total_qty DESC
            LIMIT 5
        """)
        records = list(result)
        session.close()
        driver.close()
        
        if len(records) > 0:
            print(f"[PASS] Aggregation - Got {len(records)} grouped results")
            return True
        else:
            print("[FAIL] Aggregation - No results")
            return False
    except Exception as e:
        print(f"[FAIL] Aggregation: {e}")
        return False

def main():
    print("=" * 60)
    print("ClickGraph Bolt 5.8 Protocol Tests")
    print("=" * 60)
    
    tests = [
        test_connection,
        test_simple_query,
        test_graph_traversal,
        test_aggregation
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
    
    print("=" * 60)
    print(f"RESULTS: {passed}/{len(tests)} tests passed")
    print("=" * 60)
    
    return 0 if passed == len(tests) else 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
