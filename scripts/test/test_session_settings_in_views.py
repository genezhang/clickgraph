#!/usr/bin/env python3
"""
Test script to verify ClickHouse session settings work within views.

Tests:
1. Session-level settings (SET user_id = 'alice')
2. Query-level settings (SELECT ... SETTINGS user_id = 'alice')
3. Settings accessible in views via getSetting()
"""

import requests
import json

CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = "test_user"
CLICKHOUSE_PASSWORD = "test_pass"
DATABASE = "brahmand"

def execute_clickhouse(sql, settings=None):
    """Execute SQL in ClickHouse and return response."""
    params = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "database": DATABASE,
    }
    if settings:
        params.update(settings)
    
    response = requests.post(
        CLICKHOUSE_URL,
        params=params,
        data=sql
    )
    
    if response.status_code != 200:
        print(f"❌ ERROR: {response.text}")
        return None
    
    return response.text.strip()


def test_session_settings():
    """Test session-level SET statements."""
    print("\n" + "="*80)
    print("TEST 1: Session-level SET statements")
    print("="*80)
    
    # Set session variable
    sql = "SET user_id = 'alice'"
    print(f"\n1. Setting session variable:")
    print(f"   SQL: {sql}")
    result = execute_clickhouse(sql)
    print(f"   Result: {result}")
    
    # Try to read it back
    sql = "SELECT getSetting('user_id') as user_id FORMAT JSONCompact"
    print(f"\n2. Reading session variable:")
    print(f"   SQL: {sql}")
    result = execute_clickhouse(sql)
    print(f"   Result: {result}")
    
    if result and '"alice"' in result:
        print("   ✅ SUCCESS: Session setting persisted!")
        return True
    else:
        print("   ❌ FAIL: Session setting NOT accessible")
        return False


def test_query_level_settings():
    """Test query-level SETTINGS clause."""
    print("\n" + "="*80)
    print("TEST 2: Query-level SETTINGS clause")
    print("="*80)
    
    sql = """
SELECT getSetting('user_id') as user_id, getSetting('user_role') as user_role 
FORMAT JSONCompact
SETTINGS user_id = 'bob', user_role = 'admin'
""".strip()
    
    print(f"\n1. Query with SETTINGS clause:")
    print(f"   SQL: {sql}")
    result = execute_clickhouse(sql)
    print(f"   Result: {result}")
    
    if result and '"bob"' in result and '"admin"' in result:
        print("   ✅ SUCCESS: Query-level settings work!")
        return True
    else:
        print("   ❌ FAIL: Query-level settings NOT working")
        return False


def test_settings_in_view():
    """Test getSetting() within a view definition."""
    print("\n" + "="*80)
    print("TEST 3: Settings accessible in views")
    print("="*80)
    
    # Create test table
    print("\n1. Creating test table...")
    sql = """
DROP TABLE IF EXISTS rbac_test_users;
CREATE TABLE rbac_test_users (
    user_id String,
    name String,
    role String
) ENGINE = Memory;
""".strip()
    execute_clickhouse(sql)
    
    # Insert test data
    print("\n2. Inserting test data...")
    sql = """
INSERT INTO rbac_test_users VALUES 
    ('alice', 'Alice Admin', 'admin'),
    ('bob', 'Bob User', 'user'),
    ('charlie', 'Charlie User', 'user');
""".strip()
    execute_clickhouse(sql)
    
    # Create view with getSetting() filter
    print("\n3. Creating view with getSetting() filter...")
    sql = """
DROP VIEW IF EXISTS rbac_test_filtered;
CREATE VIEW rbac_test_filtered AS
SELECT * FROM rbac_test_users
WHERE role = getSetting('user_role') 
   OR user_id = getSetting('user_id');
""".strip()
    print(f"   SQL: {sql}")
    execute_clickhouse(sql)
    
    # Query view with session settings
    print("\n4. Querying view with session-level SET...")
    execute_clickhouse("SET user_id = 'alice'")
    execute_clickhouse("SET user_role = 'nonexistent'")
    
    sql = "SELECT * FROM rbac_test_filtered FORMAT JSONCompact"
    print(f"   SQL: {sql}")
    result = execute_clickhouse(sql)
    print(f"   Result: {result}")
    
    session_works = result and '"alice"' in result and '"Alice Admin"' in result
    
    # Query view with query-level SETTINGS
    print("\n5. Querying view with query-level SETTINGS...")
    sql = """
SELECT * FROM rbac_test_filtered 
FORMAT JSONCompact
SETTINGS user_id = 'bob', user_role = 'nonexistent'
""".strip()
    print(f"   SQL: {sql}")
    result = execute_clickhouse(sql)
    print(f"   Result: {result}")
    
    query_works = result and '"bob"' in result and '"Bob User"' in result
    
    # Cleanup
    print("\n6. Cleaning up...")
    execute_clickhouse("DROP VIEW IF EXISTS rbac_test_filtered")
    execute_clickhouse("DROP TABLE IF EXISTS rbac_test_users")
    
    if session_works and query_works:
        print("   ✅ SUCCESS: Settings work in views (both methods)!")
        return True
    elif session_works:
        print("   ⚠️  PARTIAL: Session-level works, query-level doesn't")
        return False
    elif query_works:
        print("   ⚠️  PARTIAL: Query-level works, session-level doesn't")
        return False
    else:
        print("   ❌ FAIL: Settings NOT accessible in views")
        return False


def main():
    print("\n" + "="*80)
    print("ClickHouse Session Settings Test Suite")
    print("="*80)
    print("\nPurpose: Verify that session settings (user_id, user_role) are")
    print("accessible within views using getSetting() function.")
    print("\nThis validates our RBAC implementation approach.")
    
    results = {
        "session_settings": test_session_settings(),
        "query_settings": test_query_level_settings(),
        "settings_in_views": test_settings_in_view(),
    }
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    all_passed = all(results.values())
    
    print("\n" + "="*80)
    if all_passed:
        print("✅ ALL TESTS PASSED")
        print("\nConclusion: Both session-level SET and query-level SETTINGS")
        print("work correctly with getSetting() in views. Either approach is valid!")
        print("\nRecommendation: Use query-level SETTINGS for better performance")
        print("(eliminates extra round-trips and session state complexity).")
    else:
        print("❌ SOME TESTS FAILED")
        print("\nIMPORTANT: Review which approach works before committing!")
    print("="*80 + "\n")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())
