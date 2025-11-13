"""
Test script for Bolt protocol query execution.

Tests the newly implemented query execution pipeline through Neo4j Python driver.
Verifies basic queries, parameters, aggregations, and relationships work correctly.

Requirements:
    pip install neo4j

Usage:
    1. Start ClickGraph server: cargo run --bin clickgraph
    2. Ensure ClickHouse is running with demo data: docker-compose up -d
    3. Run tests: python test_bolt_protocol.py
"""

from neo4j import GraphDatabase
import sys
import traceback

# Configuration
BOLT_URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")  # Default auth from bolt_protocol/auth.rs
DATABASE = "social_network"  # Loaded schema name

def test_basic_query(session):
    """Test 1: Basic node query without parameters"""
    print("\n=== Test 1: Basic Query ===")
    query = "MATCH (u:User) RETURN u.name AS name LIMIT 5"
    
    try:
        result = session.run(query)
        records = list(result)
        
        print(f"✅ Query executed successfully")
        print(f"   Returned {len(records)} records")
        for i, record in enumerate(records, 1):
            print(f"   {i}. {record['name']}")
        
        return len(records) > 0
    except Exception as e:
        print(f"❌ Failed: {e}")
        traceback.print_exc()
        return False

def test_parameterized_query(session):
    """Test 2: Query with parameters"""
    print("\n=== Test 2: Parameterized Query ===")
    query = "MATCH (u:User {id: $user_id}) RETURN u.name AS name, u.email AS email"
    
    try:
        result = session.run(query, user_id=1)
        records = list(result)
        
        print(f"✅ Query executed successfully")
        print(f"   Returned {len(records)} records")
        for record in records:
            print(f"   User: {record['name']}, Email: {record['email']}")
        
        return len(records) > 0
    except Exception as e:
        print(f"❌ Failed: {e}")
        traceback.print_exc()
        return False

def test_aggregation(session):
    """Test 3: Aggregation query"""
    print("\n=== Test 3: Aggregation ===")
    query = "MATCH (u:User) RETURN count(u) AS user_count"
    
    try:
        result = session.run(query)
        records = list(result)
        
        print(f"✅ Query executed successfully")
        if records:
            count = records[0]['user_count']
            print(f"   Total users: {count}")
        
        return len(records) > 0
    except Exception as e:
        print(f"❌ Failed: {e}")
        traceback.print_exc()
        return False

def test_relationship_query(session):
    """Test 4: Query with relationships"""
    print("\n=== Test 4: Relationship Traversal ===")
    query = """
        MATCH (u:User)-[f:FOLLOWS]->(friend:User)
        RETURN u.name AS user, friend.name AS follows
        LIMIT 5
    """
    
    try:
        result = session.run(query)
        records = list(result)
        
        print(f"✅ Query executed successfully")
        print(f"   Returned {len(records)} records")
        for i, record in enumerate(records, 1):
            print(f"   {i}. {record['user']} follows {record['follows']}")
        
        return len(records) > 0
    except Exception as e:
        print(f"❌ Failed: {e}")
        traceback.print_exc()
        return False

def test_where_clause(session):
    """Test 5: WHERE clause filtering"""
    print("\n=== Test 5: WHERE Clause ===")
    query = """
        MATCH (u:User)
        WHERE u.id > 5
        RETURN u.name AS name, u.id AS id
        LIMIT 3
    """
    
    try:
        result = session.run(query)
        records = list(result)
        
        print(f"✅ Query executed successfully")
        print(f"   Returned {len(records)} records")
        for record in records:
            print(f"   ID {record['id']}: {record['name']}")
        
        return len(records) > 0
    except Exception as e:
        print(f"❌ Failed: {e}")
        traceback.print_exc()
        return False

def test_order_by(session):
    """Test 6: ORDER BY clause"""
    print("\n=== Test 6: ORDER BY ===")
    query = """
        MATCH (u:User)
        RETURN u.name AS name, u.id AS id
        ORDER BY u.id DESC
        LIMIT 3
    """
    
    try:
        result = session.run(query)
        records = list(result)
        
        print(f"✅ Query executed successfully")
        print(f"   Returned {len(records)} records (ordered by ID DESC)")
        for record in records:
            print(f"   ID {record['id']}: {record['name']}")
        
        return len(records) > 0
    except Exception as e:
        print(f"❌ Failed: {e}")
        traceback.print_exc()
        return False

def main():
    print("=" * 60)
    print("Bolt Protocol Query Execution Test Suite")
    print("=" * 60)
    print(f"\nConnecting to: {BOLT_URI}")
    print(f"Database: {DATABASE}")
    
    try:
        # Create driver and session
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
        
        # Test connection
        driver.verify_connectivity()
        print("✅ Connection successful\n")
        
        # Run tests
        results = {}
        with driver.session(database=DATABASE) as session:
            results['basic'] = test_basic_query(session)
            results['parameterized'] = test_parameterized_query(session)
            results['aggregation'] = test_aggregation(session)
            results['relationship'] = test_relationship_query(session)
            results['where'] = test_where_clause(session)
            results['order_by'] = test_order_by(session)
        
        # Close driver
        driver.close()
        
        # Summary
        print("\n" + "=" * 60)
        print("Test Summary")
        print("=" * 60)
        
        passed = sum(1 for v in results.values() if v)
        total = len(results)
        
        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status} - {test_name}")
        
        print(f"\nResult: {passed}/{total} tests passed")
        
        if passed == total:
            print("\n🎉 All tests passed! Bolt protocol query execution working!")
            return 0
        else:
            print(f"\n⚠️  {total - passed} test(s) failed")
            return 1
            
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Is ClickGraph server running? (cargo run --bin clickgraph)")
        print("2. Is ClickHouse running? (docker-compose up -d)")
        print("3. Is demo data loaded? (see scripts/setup_demo_data.sql)")
        print("4. Is Bolt port correct? (default: 7687)")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
