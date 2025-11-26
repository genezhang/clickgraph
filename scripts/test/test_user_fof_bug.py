"""
Test the exact user bug report query against ClickGraph.

User's query:
    MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
    WHERE a.name = "Alice" AND b.name = "Bob" 
    RETURN mutual.name

Expected (Neo4j): 1 result (Charlie)
Question: Does ClickGraph return duplicates?
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'tests', 'integration'))

from conftest import execute_cypher
import pytest
import clickhouse_connect


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "test_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "test_pass")


def setup_test_data():
    """Create the exact same test data as Neo4j test."""
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )
    
    database = "test_integration"
    
    # Clean existing data
    tables = client.query(f"SELECT name FROM system.tables WHERE database = '{database}'").result_rows
    for (table_name,) in tables:
        client.command(f"DROP TABLE IF EXISTS {database}.{table_name}")
    
    # Create users table
    client.command(f"""
        CREATE TABLE {database}.users (
            user_id UInt32,
            name String,
            age UInt32
        ) ENGINE = Memory
    """)
    
    # Create follows table
    client.command(f"""
        CREATE TABLE {database}.follows (
            follower_id UInt32,
            followed_id UInt32,
            since String
        ) ENGINE = Memory
    """)
    
    # Insert users (same as Neo4j)
    client.command(f"""
        INSERT INTO {database}.users VALUES
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Charlie', 35),
            (4, 'Diana', 28),
            (5, 'Eve', 32)
    """)
    
    # Insert follows relationships (same as Neo4j)
    client.command(f"""
        INSERT INTO {database}.follows VALUES
            (1, 2, '2023-01-01'),
            (1, 3, '2023-01-15'),
            (2, 3, '2023-02-01'),
            (3, 4, '2023-02-15'),
            (4, 5, '2023-03-01'),
            (2, 4, '2023-03-15')
    """)
    
    print("✓ Test data loaded into ClickHouse")
    
    # Verify data
    user_count = client.command(f"SELECT count(*) FROM {database}.users")
    rel_count = client.command(f"SELECT count(*) FROM {database}.follows")
    
    print(f"  - {user_count} users")
    print(f"  - {rel_count} FOLLOWS relationships")
    
    client.close()


def test_user_bug_report():
    """Test the exact query from user bug report."""
    print("\n" + "="*70)
    print("USER BUG REPORT: Mutual Friends Query")
    print("="*70)
    
    query = """
        MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
        WHERE a.name = 'Alice' AND b.name = 'Bob' 
        RETURN mutual.name
    """
    
    print("\nQuery WITHOUT DISTINCT:")
    print(query)
    
    try:
        response = execute_cypher(query, schema_name="test_graph_schema", raise_on_error=False)
        
        # Handle both list and dict responses
        if isinstance(response, dict) and "error" in response:
            print(f"\n❌ ERROR: {response['error']}")
            return False
        
        results = response if isinstance(response, list) else response.get("results", [])
        
        print(f"\nClickGraph Results: {len(results)} rows")
        print("Results:")
        for i, record in enumerate(results, 1):
            print(f"  {i}. {record}")
        
        if len(results) > 1:
            # Check if all results are the same
            unique_values = set(str(r) for r in results)
            if len(unique_values) == 1:
                print(f"\n❌ DUPLICATES FOUND - Same result returned {len(results)} times")
            else:
                print(f"\n❌ DIFFERENT RESULTS - {len(results)} different rows")
        else:
            print("\n✅ NO DUPLICATES - Single result as expected")
        
        # Test with DISTINCT
        query_distinct = """
            MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
            WHERE a.name = 'Alice' AND b.name = 'Bob' 
            RETURN DISTINCT mutual.name
        """
        
        print("\nQuery WITH DISTINCT:")
        print(query_distinct)
        
        response_distinct = execute_cypher(query_distinct, schema_name="test_graph_schema", raise_on_error=False)
        
        if isinstance(response_distinct, dict) and "error" in response_distinct:
            print(f"\n❌ ERROR: {response_distinct['error']}")
            return False
        
        results_distinct = response_distinct if isinstance(response_distinct, list) else response_distinct.get("results", [])
        
        print(f"\nClickGraph Results: {len(results_distinct)} rows")
        print("Results:")
        for i, record in enumerate(results_distinct, 1):
            print(f"  {i}. {record}")
        
        # Compare
        print("\n" + "="*70)
        print("COMPARISON")
        print("="*70)
        print(f"Neo4j without DISTINCT:      1 result (Charlie)")
        print(f"ClickGraph without DISTINCT:  {len(results)} result(s)")
        print(f"ClickGraph with DISTINCT:     {len(results_distinct)} result(s)")
        
        if len(results) == 1 and len(results_distinct) == 1:
            print("\n✅ ClickGraph matches Neo4j - NO BUG")
        else:
            print(f"\n❌ ClickGraph differs from Neo4j - BUG CONFIRMED")
            print(f"   ClickGraph returns {len(results)} rows, Neo4j returns 1 row")
        
        return True
        
    except Exception as e:
        print(f"\n❌ EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_friend_of_friend():
    """Test the friend-of-friend pattern that showed duplicates in Neo4j."""
    print("\n" + "="*70)
    print("FRIEND-OF-FRIEND: Pattern That HAS Duplicates in Neo4j")
    print("="*70)
    
    query = """
        MATCH (me:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof:User)
        WHERE me.name = 'Alice'
        RETURN fof.name
    """
    
    print("\nQuery WITHOUT DISTINCT:")
    print(query)
    
    try:
        response = execute_cypher(query, schema_name="test_graph_schema", raise_on_error=False)
        
        if isinstance(response, dict) and "error" in response:
            print(f"\n❌ ERROR: {response['error']}")
            return False
        
        results = response if isinstance(response, list) else response.get("results", [])
        
        print(f"\nClickGraph Results: {len(results)} rows")
        print("Results:")
        for i, record in enumerate(results, 1):
            print(f"  {i}. {record}")
        
        # Compare to Neo4j
        print("\n" + "="*70)
        print("COMPARISON")
        print("="*70)
        print(f"Neo4j without DISTINCT:      3 results (Diana, Diana, Charlie)")
        print(f"ClickGraph without DISTINCT:  {len(results)} result(s)")
        
        if len(results) == 3:
            print("\n✅ ClickGraph matches Neo4j - Duplicates are expected here")
        else:
            print(f"\n⚠️  ClickGraph returns {len(results)} rows, Neo4j returns 3 rows")
        
        return True
        
    except Exception as e:
        print(f"\n❌ EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("="*70)
    print("ClickGraph User Bug Verification")
    print("="*70)
    print("\nLoading test data...")
    setup_test_data()
    
    print("\n")
    test_user_bug_report()
    
    print("\n")
    test_friend_of_friend()
    
    print("\n" + "="*70)
    print("DONE")
    print("="*70)
