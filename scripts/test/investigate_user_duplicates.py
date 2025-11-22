"""
Template for investigating user's duplicate results issue.

Once user provides their actual data, use this script to:
1. Load their exact data into ClickHouse
2. Run their exact query
3. Check for duplicate relationships or unexpected patterns
4. Compare against expected Neo4j behavior
"""

import clickhouse_connect
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'tests', 'integration'))
from conftest import execute_cypher


def analyze_user_data(database="test_integration"):
    """Analyze the user's actual data to understand why they see duplicates."""
    
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="test_user",
        password="test_pass"
    )
    
    print("="*70)
    print("ANALYZING USER DATA")
    print("="*70)
    
    # 1. Check table structure
    print("\n1. Table Structure:")
    print("-" * 70)
    
    tables = client.query(f"SHOW TABLES FROM {database}").result_rows
    print(f"Tables in {database}: {[t[0] for t in tables]}")
    
    # 2. Check for duplicate relationships
    print("\n2. Checking for Duplicate Relationships:")
    print("-" * 70)
    
    # Assuming follows table (adjust based on user's schema)
    follow_table = "follows"  # or user_follows, etc.
    
    try:
        # Check total relationships
        total = client.command(f"SELECT count(*) FROM {database}.{follow_table}")
        print(f"Total relationships: {total}")
        
        # Check for duplicates
        duplicates = client.query(f"""
            SELECT follower_id, followed_id, count(*) as cnt
            FROM {database}.{follow_table}
            GROUP BY follower_id, followed_id
            HAVING cnt > 1
        """).result_rows
        
        if duplicates:
            print(f"\n⚠️  FOUND {len(duplicates)} DUPLICATE RELATIONSHIP(S):")
            for follower, followed, count in duplicates:
                print(f"   {follower} -> {followed}: appears {count} times")
        else:
            print("✓ No duplicate relationships found")
            
    except Exception as e:
        print(f"Could not check {follow_table}: {e}")
    
    # 3. Check users named Alice and Bob
    print("\n3. Users in Query:")
    print("-" * 70)
    
    try:
        alice = client.query(f"""
            SELECT * FROM {database}.users 
            WHERE name = 'Alice'
        """).result_rows
        
        bob = client.query(f"""
            SELECT * FROM {database}.users 
            WHERE name = 'Bob'
        """).result_rows
        
        print(f"Alice: {alice}")
        print(f"Bob: {bob}")
        
        if not alice:
            print("⚠️  No user named 'Alice' found!")
        if not bob:
            print("⚠️  No user named 'Bob' found!")
            
    except Exception as e:
        print(f"Could not check users: {e}")
    
    # 4. Check what Alice and Bob follow
    print("\n4. Relationship Paths:")
    print("-" * 70)
    
    try:
        # What does Alice follow?
        alice_follows = client.query(f"""
            SELECT u2.user_id, u2.name
            FROM {database}.users u1
            JOIN {database}.{follow_table} f ON f.follower_id = u1.user_id
            JOIN {database}.users u2 ON u2.user_id = f.followed_id
            WHERE u1.name = 'Alice'
        """).result_rows
        
        print(f"Alice follows: {alice_follows}")
        
        # What does Bob follow?
        bob_follows = client.query(f"""
            SELECT u2.user_id, u2.name
            FROM {database}.users u1
            JOIN {database}.{follow_table} f ON f.follower_id = u1.user_id
            JOIN {database}.users u2 ON u2.user_id = f.followed_id
            WHERE u1.name = 'Bob'
        """).result_rows
        
        print(f"Bob follows: {bob_follows}")
        
        # Find mutual (people both follow)
        alice_ids = {row[0] for row in alice_follows}
        bob_ids = {row[0] for row in bob_follows}
        mutual_ids = alice_ids & bob_ids
        
        print(f"\nMutual follows (should be result): {mutual_ids}")
        
        if len(mutual_ids) > 1:
            print(f"⚠️  Found {len(mutual_ids)} mutual follows (expected 1 in test case)")
        
    except Exception as e:
        print(f"Could not check relationships: {e}")
    
    client.close()
    
    print("\n" + "="*70)


def test_user_query(query, schema_name="test_graph_schema"):
    """Test the exact query the user is running."""
    
    print("\n" + "="*70)
    print("TESTING USER QUERY")
    print("="*70)
    print(f"\nQuery:\n{query}")
    
    try:
        response = execute_cypher(query, schema_name=schema_name, raise_on_error=False)
        
        if isinstance(response, dict) and "error" in response:
            print(f"\n❌ ERROR: {response['error']}")
            return
        
        results = response if isinstance(response, list) else response.get("results", [])
        
        print(f"\nResults: {len(results)} row(s)")
        for i, record in enumerate(results, 1):
            print(f"  {i}. {record}")
        
        if len(results) > 1:
            # Check if they're actual duplicates
            unique = len(set(str(r) for r in results))
            if unique < len(results):
                print(f"\n❌ DUPLICATES: {len(results)} rows but only {unique} unique values")
            else:
                print(f"\n⚠️  {len(results)} different results (not duplicates)")
        else:
            print("\n✓ Single result (no duplicates)")
            
    except Exception as e:
        print(f"\n❌ EXCEPTION: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Wait for user to provide:
    # 1. Their actual table names
    # 2. Their actual data (or SQL dump)
    # 3. Their exact query
    
    print("Waiting for user data...")
    print("\nPlease provide:")
    print("1. Table names and structure")
    print("2. Data (SELECT * FROM users; SELECT * FROM follows;)")
    print("3. Exact Cypher query showing duplicates")
    
    # For now, analyze current test data
    analyze_user_data()
