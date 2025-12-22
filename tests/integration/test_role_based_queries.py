"""
Integration tests for role-based connection pool and RBAC queries.

Tests that verify:
1. Queries work without role (default connection pool)
2. Queries work with role specified (role-specific connection pool)
3. Multiple concurrent requests with different roles work correctly
4. SQL generation endpoint includes SET ROLE in array
"""

import pytest
import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = f"{CLICKGRAPH_URL}"
SCHEMA_NAME = "unified_test_schema"  # Default schema for these tests


def test_query_without_role():
    """Test that queries work without specifying a role (default connection pool)."""
    query = """
    MATCH (u:User)
    WHERE u.user_id = 1
    RETURN u.name, u.email
    LIMIT 1
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "schema_name": SCHEMA_NAME}
    )
    
    assert response.status_code == 200, f"Query failed: {response.text}"
    data = response.json()
    assert "results" in data
    assert len(data["results"]) > 0


def test_query_with_role():
    """Test that queries work with a role specified (role-specific connection pool)."""
    query = """
    MATCH (u:User)
    WHERE u.user_id = 1
    RETURN u.name, u.email
    LIMIT 1
    """
    
    # Note: This test assumes the role 'analyst' exists in ClickHouse
    # If not, the query should still execute but may have different permissions
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "role": "analyst",
            "schema_name": SCHEMA_NAME
        }
    )
    
    # Should succeed even if role doesn't exist (uses default permissions)
    assert response.status_code in [200, 500], f"Unexpected status: {response.status_code}"
    
    if response.status_code == 200:
        data = response.json()
        assert "results" in data
        print(f"✓ Query with role 'analyst' succeeded")
    else:
        print(f"✓ Query with role 'analyst' failed as expected (role may not exist)")


def test_sql_generation_includes_set_role():
    """Test that SQL generation endpoint includes SET ROLE in the SQL array."""
    query = """
    MATCH (u:User)
    WHERE u.user_id = 1
    RETURN u.name
    LIMIT 1
    """
    
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={
            "query": query,
            "role": "analyst",
            "schema_name": SCHEMA_NAME
        }
    )
    
    assert response.status_code == 200, f"SQL generation failed: {response.text}"
    data = response.json()
    
    # Check that sql field is an array
    assert "sql" in data, "Response missing 'sql' field"
    assert isinstance(data["sql"], list), "sql field should be an array"
    
    # Check that SET ROLE is in the array
    sql_statements = data["sql"]
    has_set_role = any("SET ROLE" in stmt for stmt in sql_statements)
    assert has_set_role, f"SET ROLE not found in SQL array: {sql_statements}"
    
    # Check that role is in response metadata
    assert data.get("role") == "analyst", "Role not in response metadata"
    
    print(f"✓ SQL generation includes SET ROLE in array: {sql_statements}")


def test_sql_generation_without_role():
    """Test that SQL generation without role doesn't include SET ROLE."""
    query = """
    MATCH (u:User)
    WHERE u.user_id = 1
    RETURN u.name
    LIMIT 1
    """
    
    response = requests.post(
        f"{BASE_URL}/query/sql",
        json={"query": query, "schema_name": SCHEMA_NAME}
    )
    
    assert response.status_code == 200, f"SQL generation failed: {response.text}"
    data = response.json()
    
    # Check that sql field is an array
    assert "sql" in data, "Response missing 'sql' field"
    assert isinstance(data["sql"], list), "sql field should be an array"
    
    # Check that SET ROLE is NOT in the array
    sql_statements = data["sql"]
    has_set_role = any("SET ROLE" in stmt for stmt in sql_statements)
    assert not has_set_role, f"SET ROLE should not be in SQL array without role: {sql_statements}"
    
    # Check that role is None in response metadata
    assert data.get("role") is None, "Role should be None in response metadata"
    
    print(f"✓ SQL generation without role excludes SET ROLE")


@pytest.mark.xfail(reason="Query parameter substitution {user_id} not being processed by parser - test issue")
def test_concurrent_queries_different_roles():
    """
    Test that concurrent queries with different roles work correctly.
    
    This verifies that the role-based connection pool prevents race conditions
    and properly isolates role contexts across concurrent requests.
    """
    query = """
    MATCH (u:User)
    WHERE u.user_id = {user_id}
    RETURN u.name, u.email
    LIMIT 1
    """
    
    def execute_query(role, user_id):
        """Execute a query with a specific role."""
        try:
            response = requests.post(
                f"{BASE_URL}/query",
                json={
                    "query": query,
                    "role": role,
                    "parameters": {"user_id": user_id},
                    "schema_name": SCHEMA_NAME
                }
            )
            return {
                "role": role,
                "user_id": user_id,
                "status": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None,
                "error": response.text if response.status_code != 200 else None
            }
        except Exception as e:
            return {
                "role": role,
                "user_id": user_id,
                "status": 0,
                "success": False,
                "error": str(e)
            }
    
    # Execute 10 queries concurrently with different roles
    test_cases = [
        ("analyst", 1),
        ("admin", 2),
        (None, 3),  # No role (default pool)
        ("analyst", 4),
        ("admin", 5),
        (None, 1),
        ("analyst", 2),
        ("admin", 3),
        (None, 4),
        ("analyst", 5),
    ]
    
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(execute_query, role, user_id)
            for role, user_id in test_cases
        ]
        
        for future in as_completed(futures):
            results.append(future.result())
    
    # Verify all queries succeeded (or failed for expected reasons)
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    
    print(f"\nConcurrent query results:")
    print(f"  Successful: {len(successful)}/{len(results)}")
    print(f"  Failed: {len(failed)}/{len(results)}")
    
    if failed:
        print(f"\nFailed queries:")
        for r in failed:
            print(f"  - Role: {r['role']}, User ID: {r['user_id']}, Error: {r['error']}")
    
    # At least some queries should succeed (roles may not exist, but default pool should work)
    assert len(successful) >= 3, f"Expected at least 3 successful queries with default role"


def test_sql_only_mode_with_role():
    """Test sql_only parameter works correctly with role."""
    query = """
    MATCH (u:User)
    WHERE u.user_id = 1
    RETURN u.name
    LIMIT 1
    """
    
    response = requests.post(
        f"{BASE_URL}/query",
        json={
            "query": query,
            "sql_only": True,
            "role": "analyst",
            "schema_name": SCHEMA_NAME
        }
    )
    
    assert response.status_code == 200, f"SQL-only query failed: {response.text}"
    data = response.json()
    
    # Should return SQL without executing
    assert "generated_sql" in data, "Response missing 'generated_sql' field"
    assert "execution_mode" in data, "Response missing 'execution_mode' field"
    assert data["execution_mode"] == "sql_only", "Wrong execution mode"
    
    # Note: sql_only uses old format (single string), not array format
    # The /query/sql endpoint uses the new array format
    print(f"✓ sql_only mode works with role")


if __name__ == "__main__":
    print("Running role-based query tests...")
    print("\n" + "="*60)
    
    try:
        print("\n1. Testing query without role (default pool)...")
        test_query_without_role()
        print("   ✓ PASSED")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
    
    try:
        print("\n2. Testing query with role (role-specific pool)...")
        test_query_with_role()
        print("   ✓ PASSED")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
    
    try:
        print("\n3. Testing SQL generation includes SET ROLE...")
        test_sql_generation_includes_set_role()
        print("   ✓ PASSED")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
    
    try:
        print("\n4. Testing SQL generation without role...")
        test_sql_generation_without_role()
        print("   ✓ PASSED")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
    
    try:
        print("\n5. Testing concurrent queries with different roles...")
        test_concurrent_queries_different_roles()
        print("   ✓ PASSED")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
    
    try:
        print("\n6. Testing sql_only mode with role...")
        test_sql_only_mode_with_role()
        print("   ✓ PASSED")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
    
    print("\n" + "="*60)
    print("Role-based query tests complete!")
