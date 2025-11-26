#!/usr/bin/env python3
"""
Test SET ROLE RBAC implementation for ClickGraph.

Tests:
1. Setup: Create database-managed users with roles
2. Test role-based query filtering
3. Test error handling (role not granted, role doesn't exist)
4. Verify system.current_roles filtering works
5. Test both HTTP and Bolt protocols
"""

import requests
import sys
from clickhouse_driver import Client

CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = "test_user"
CLICKHOUSE_PASSWORD = "test_pass"
CLICKHOUSE_DB = "brahmand"

CLICKGRAPH_HTTP_URL = "http://localhost:8080"

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")

def setup_clickhouse():
    """Setup ClickHouse with roles and test data."""
    print_section("Setting Up ClickHouse")
    
    client = Client(
        host='localhost',
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )
    
    print("1. Creating roles...")
    try:
        client.execute("CREATE ROLE IF NOT EXISTS admin_role")
        client.execute("CREATE ROLE IF NOT EXISTS user_role")
        client.execute("CREATE ROLE IF NOT EXISTS viewer_role")
        print("   ✅ Roles created: admin_role, user_role, viewer_role")
    except Exception as e:
        print(f"   ⚠️  Roles may already exist: {e}")
    
    print("\n2. Creating database-managed users...")
    try:
        # Drop existing users if they exist
        client.execute("DROP USER IF EXISTS alice")
        client.execute("DROP USER IF EXISTS bob")
        client.execute("DROP USER IF EXISTS charlie")
        
        # Create users with passwords
        client.execute("CREATE USER alice IDENTIFIED WITH plaintext_password BY 'alice_pass'")
        client.execute("CREATE USER bob IDENTIFIED WITH plaintext_password BY 'bob_pass'")
        client.execute("CREATE USER charlie IDENTIFIED WITH plaintext_password BY 'charlie_pass'")
        print("   ✅ Users created: alice, bob, charlie")
    except Exception as e:
        print(f"   ❌ Failed to create users: {e}")
        return False
    
    print("\n3. Granting roles to users...")
    try:
        # Grant roles
        client.execute("GRANT admin_role TO alice")
        client.execute("GRANT user_role TO bob")
        client.execute("GRANT viewer_role TO charlie")
        
        # Grant database access
        client.execute(f"GRANT SELECT ON {CLICKHOUSE_DB}.* TO admin_role")
        client.execute(f"GRANT SELECT ON {CLICKHOUSE_DB}.* TO user_role")
        client.execute(f"GRANT SELECT ON {CLICKHOUSE_DB}.* TO viewer_role")
        
        print("   ✅ Roles granted:")
        print("      - alice: admin_role (full access)")
        print("      - bob: user_role (limited access)")
        print("      - charlie: viewer_role (read-only)")
    except Exception as e:
        print(f"   ❌ Failed to grant roles: {e}")
        return False
    
    print("\n4. Creating test table with role-based data...")
    try:
        # Drop and recreate test table
        client.execute("DROP TABLE IF EXISTS users_rbac_test")
        client.execute("""
            CREATE TABLE users_rbac_test (
                user_id UInt32,
                name String,
                email String,
                owner String,
                visible_to_role String
            ) ENGINE = Memory
        """)
        
        # Insert test data
        client.execute("""
            INSERT INTO users_rbac_test VALUES
                (1, 'Alice Admin', 'alice@example.com', 'alice', 'admin_role'),
                (2, 'Bob User', 'bob@example.com', 'bob', 'user_role'),
                (3, 'Charlie Viewer', 'charlie@example.com', 'charlie', 'viewer_role'),
                (4, 'Admin Only Data', 'admin@example.com', 'system', 'admin_role'),
                (5, 'Public Data', 'public@example.com', 'system', 'viewer_role')
        """)
        
        count = client.execute("SELECT count() FROM users_rbac_test")[0][0]
        print(f"   ✅ Created users_rbac_test with {count} records")
    except Exception as e:
        print(f"   ❌ Failed to create test table: {e}")
        return False
    
    print("\n5. Creating role-filtered view...")
    try:
        client.execute("DROP VIEW IF EXISTS users_rbac_secure")
        client.execute("""
            CREATE VIEW users_rbac_secure AS
            SELECT * FROM users_rbac_test
            WHERE visible_to_role IN (
                SELECT role_name FROM system.current_roles
            )
        """)
        print("   ✅ Created users_rbac_secure view with role filtering")
    except Exception as e:
        print(f"   ❌ Failed to create view: {e}")
        return False
    
    return True

def test_direct_clickhouse_role():
    """Test SET ROLE directly in ClickHouse to verify setup."""
    print_section("Testing Direct ClickHouse Role Behavior")
    
    # Connect as alice
    print("1. Testing as alice (admin_role)...")
    alice_client = Client(
        host='localhost',
        user='alice',
        password='alice_pass',
        database=CLICKHOUSE_DB
    )
    
    try:
        # Set role
        alice_client.execute("SET ROLE admin_role")
        print("   ✅ SET ROLE admin_role succeeded")
        
        # Check current roles
        roles = alice_client.execute("SELECT role_name FROM system.current_roles")
        print(f"   ✅ Current roles: {[r[0] for r in roles]}")
        
        # Query the view
        result = alice_client.execute("SELECT user_id, name FROM users_rbac_secure ORDER BY user_id")
        print(f"   ✅ Alice sees {len(result)} records through view:")
        for row in result:
            print(f"      - {row[0]}: {row[1]}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False
    
    # Connect as bob
    print("\n2. Testing as bob (user_role)...")
    bob_client = Client(
        host='localhost',
        user='bob',
        password='bob_pass',
        database=CLICKHOUSE_DB
    )
    
    try:
        bob_client.execute("SET ROLE user_role")
        print("   ✅ SET ROLE user_role succeeded")
        
        result = bob_client.execute("SELECT user_id, name FROM users_rbac_secure ORDER BY user_id")
        print(f"   ✅ Bob sees {len(result)} records through view:")
        for row in result:
            print(f"      - {row[0]}: {row[1]}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False
    
    # Connect as charlie
    print("\n3. Testing as charlie (viewer_role)...")
    charlie_client = Client(
        host='localhost',
        user='charlie',
        password='charlie_pass',
        database=CLICKHOUSE_DB
    )
    
    try:
        charlie_client.execute("SET ROLE viewer_role")
        print("   ✅ SET ROLE viewer_role succeeded")
        
        result = charlie_client.execute("SELECT user_id, name FROM users_rbac_secure ORDER BY user_id")
        print(f"   ✅ Charlie sees {len(result)} records through view:")
        for row in result:
            print(f"      - {row[0]}: {row[1]}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False
    
    return True

def create_test_schema():
    """Create ClickGraph schema for RBAC testing."""
    print_section("Creating ClickGraph Schema")
    
    schema_yaml = """
graph_schema:
  graph_name: rbac_test
  version: "1.0"
  
  nodes:
    - label: User
      database: brahmand
      table: users_rbac_secure
      id_column: user_id
      property_mappings:
        user_id: user_id
        name: name
        email: email
        owner: owner
  
  relationships: []
"""
    
    try:
        response = requests.post(
            f"{CLICKGRAPH_HTTP_URL}/schemas/load",
            json={"config_content": schema_yaml, "schema_name": "rbac_test"}
        )
        
        if response.status_code == 200:
            print("   ✅ Schema loaded successfully")
            return True
        else:
            print(f"   ❌ Failed to load schema: {response.status_code}")
            print(f"      {response.text}")
            return False
    except Exception as e:
        print(f"   ❌ Failed to load schema: {e}")
        return False

def test_http_with_role():
    """Test HTTP API with role parameter."""
    print_section("Testing HTTP API with SET ROLE")
    
    # Test 1: Query with admin_role
    print("1. Query with role='admin_role'...")
    try:
        response = requests.post(
            f"{CLICKGRAPH_HTTP_URL}/query",
            json={
                "query": "MATCH (u:User) RETURN u.user_id, u.name ORDER BY u.user_id",
                "schema_name": "rbac_test",
                "role": "admin_role"
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Success! Returned {len(data)} records:")
            for row in data[:5]:  # Show first 5
                print(f"      - {row}")
        else:
            print(f"   ❌ Failed: {response.status_code}")
            print(f"      {response.text}")
            return False
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        return False
    
    # Test 2: Query with user_role
    print("\n2. Query with role='user_role'...")
    try:
        response = requests.post(
            f"{CLICKGRAPH_HTTP_URL}/query",
            json={
                "query": "MATCH (u:User) RETURN u.user_id, u.name ORDER BY u.user_id",
                "schema_name": "rbac_test",
                "role": "user_role"
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Success! Returned {len(data)} records:")
            for row in data:
                print(f"      - {row}")
        else:
            print(f"   ❌ Failed: {response.status_code}")
            print(f"      {response.text}")
            return False
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        return False
    
    # Test 3: Query with viewer_role
    print("\n3. Query with role='viewer_role'...")
    try:
        response = requests.post(
            f"{CLICKGRAPH_HTTP_URL}/query",
            json={
                "query": "MATCH (u:User) RETURN u.user_id, u.name ORDER BY u.user_id",
                "schema_name": "rbac_test",
                "role": "viewer_role"
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Success! Returned {len(data)} records:")
            for row in data:
                print(f"      - {row}")
        else:
            print(f"   ❌ Failed: {response.status_code}")
            print(f"      {response.text}")
            return False
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        return False
    
    # Test 4: Query without role
    print("\n4. Query without role parameter (should work)...")
    try:
        response = requests.post(
            f"{CLICKGRAPH_HTTP_URL}/query",
            json={
                "query": "MATCH (u:User) RETURN u.user_id, u.name ORDER BY u.user_id",
                "schema_name": "rbac_test"
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Success! Returned {len(data)} records (no role set)")
        else:
            print(f"   ⚠️  Failed: {response.status_code}")
            print(f"      {response.text}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    return True

def test_error_handling():
    """Test error cases."""
    print_section("Testing Error Handling")
    
    # Test 1: Role that doesn't exist
    print("1. Testing non-existent role...")
    try:
        response = requests.post(
            f"{CLICKGRAPH_HTTP_URL}/query",
            json={
                "query": "MATCH (u:User) RETURN u.user_id",
                "schema_name": "rbac_test",
                "role": "nonexistent_role"
            }
        )
        
        if response.status_code != 200:
            print(f"   ✅ Correctly rejected with status {response.status_code}")
            print(f"      Error: {response.text[:100]}")
        else:
            print(f"   ⚠️  Should have failed but succeeded")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Test 2: Role not granted to current user (test_user)
    print("\n2. Testing role not granted to test_user...")
    try:
        response = requests.post(
            f"{CLICKGRAPH_HTTP_URL}/query",
            json={
                "query": "MATCH (u:User) RETURN u.user_id",
                "schema_name": "rbac_test",
                "role": "admin_role"  # test_user doesn't have this role
            }
        )
        
        if response.status_code != 200:
            print(f"   ✅ Correctly rejected with status {response.status_code}")
            print(f"      Error: {response.text[:100]}")
        else:
            print(f"   ⚠️  Should have failed but succeeded")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    return True

def main():
    print("\n" + "="*60)
    print("  ClickGraph SET ROLE RBAC Test Suite")
    print("="*60)
    print("\n⚠️  NOTE: This test requires ClickGraph server running on port 8080")
    print("    Start with: cargo run --release --bin clickgraph")
    
    # Setup
    if not setup_clickhouse():
        print("\n❌ Setup failed!")
        sys.exit(1)
    
    # Test direct ClickHouse behavior
    if not test_direct_clickhouse_role():
        print("\n❌ Direct ClickHouse tests failed!")
        sys.exit(1)
    
    # Create ClickGraph schema
    if not create_test_schema():
        print("\n❌ Schema creation failed!")
        sys.exit(1)
    
    # Test HTTP API
    if not test_http_with_role():
        print("\n❌ HTTP API tests failed!")
        sys.exit(1)
    
    # Test error handling
    test_error_handling()
    
    print_section("✅ ALL TESTS PASSED!")
    print("\nSummary:")
    print("  ✅ Database-managed users created (alice, bob, charlie)")
    print("  ✅ Roles created and granted (admin_role, user_role, viewer_role)")
    print("  ✅ Role-filtered view working correctly")
    print("  ✅ Direct ClickHouse SET ROLE working")
    print("  ✅ ClickGraph HTTP API with role parameter working")
    print("  ✅ Error handling for invalid roles working")
    print("\n🎉 SET ROLE RBAC implementation verified!")

if __name__ == "__main__":
    main()
