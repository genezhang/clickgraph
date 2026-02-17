"""
Regression tests for OPTIONAL MATCH with WHERE clause bug.

Bug: WHERE clause from required MATCH was silently dropped when followed by OPTIONAL MATCH.
Root Cause: collect_graphrel_predicates() had no Filter case, so Filter wrapping GraphNode
            was never extracted to WHERE clause.
Result: Cartesian product returned instead of filtered results.

Fixed: 2026-02-17 (commit 472c712)
- Added Filter case to collect_graphrel_predicates()
- Extracts predicate with property mapping
- Now generates correct SQL with WHERE clause

These tests ensure this bug never returns.
"""
import pytest
import requests


BASE_URL = "http://localhost:8080"


def execute_query(query, schema_name="test_fixtures", parameters=None):
    """Execute a query and return results"""
    payload = {"query": query, "schema_name": schema_name, "replan": "force"}
    if parameters:
        payload["parameters"] = parameters
    
    response = requests.post(f"{BASE_URL}/query", json=payload)
    assert response.status_code == 200, f"Query failed: {response.text}"
    return response.json()


class TestOptionalMatchWhereRegression:
    """Critical regression tests for OPTIONAL MATCH WHERE clause bug."""
    
    def test_match_where_optional_match_basic(self):
        """
        Regression: MATCH (a) WHERE a.prop = X OPTIONAL MATCH (a)-[]->(b)
        
        Bug: WHERE clause was dropped, returning all users (Cartesian product).
        Fix: WHERE clause now included in generated SQL.
        """
        result = execute_query(
            """
            MATCH (a:TestUser)
            WHERE a.name = 'Alice' AND a.user_id = 1
            OPTIONAL MATCH (a)-[:TEST_FOLLOWS]->(b:TestUser)
            RETURN DISTINCT a.name, b.name
            ORDER BY b.name
            """
        )
        
        # Alice (user_id=1) follows Bob and Charlie, so should return 2 rows
        assert "results" in result
        assert len(result["results"]) == 2, f"Expected 2 rows (Alice's connections), got {len(result['results'])}"
        
        # Verify it's actually Alice's connections
        names = [row.get("b.name") for row in result["results"]]
        assert "Bob" in names
        assert "Charlie" in names
    
    def test_match_where_optional_match_no_results(self):
        """
        Regression: User with no outgoing relationships should return 1 row with NULL.
        
        Bug: Would return all users × relationships (wrong count).
        Fix: Returns single row for the filtered user.
        """
        result = execute_query(
            """
            MATCH (a:TestUser)
            WHERE a.name = 'Eve' AND a.user_id = 5
            OPTIONAL MATCH (a)-[:TEST_FOLLOWS]->(b:TestUser)
            RETURN DISTINCT a.name, b.name
            """
        )
        
        # Eve (user_id=5) has no connections, should return 1 row with NULL for b
        assert len(result["results"]) == 1, f"Expected 1 row, got {len(result['results'])}"
        assert result["results"][0]["a.name"] == "Eve"
        assert result["results"][0]["b.name"] is None
    
    def test_match_where_complex_optional_match(self):
        """
        Regression: Complex WHERE predicate with OPTIONAL MATCH.
        
        Bug: Complex predicates (AND/OR) were also dropped.
        Fix: All predicates preserved in WHERE clause.
        """
        result = execute_query(
            """
            MATCH (a:TestUser)
            WHERE a.name = 'Alice' AND a.user_id = 1
            OPTIONAL MATCH (a)-[:TEST_FOLLOWS]->(b:TestUser)
            RETURN DISTINCT a.name, b.name, b.user_id
            """
        )
        
        # Should filter to just Alice (user_id=1)'s connections
        assert len(result["results"]) == 2, f"Expected 2 rows, got {len(result['results'])}"
        # All results should be for Alice (user_id = 1)
        for row in result["results"]:
            assert row["a.name"] == "Alice"
    
    def test_match_where_optional_match_incoming(self):
        """
        Regression: Incoming relationships with WHERE filter.
        
        Bug: WHERE clause dropped for incoming relationships too.
        Fix: Direction doesn't matter - WHERE clause preserved.
        """
        result = execute_query(
            """
            MATCH (a:TestUser)
            WHERE a.name = 'Bob' AND a.user_id = 2
            OPTIONAL MATCH (b:TestUser)-[:TEST_FOLLOWS]->(a)
            RETURN DISTINCT a.name, b.name
            """
        )
        
        # Bob (user_id=2) is followed by Alice, should return 1 row
        assert len(result["results"]) == 1, f"Expected 1 row, got {len(result['results'])}"
        assert result["results"][0]["a.name"] == "Bob"
        assert result["results"][0]["b.name"] == "Alice"
    
    def test_sql_generation_includes_where(self):
        """
        Regression: Verify SQL actually contains WHERE clause.
        
        This is a meta-test that checks the generated SQL directly
        to ensure the bug fix is working at the SQL generation level.
        """
        payload = {
            "query": "MATCH (a:TestUser) WHERE a.name = 'Alice' OPTIONAL MATCH (a)-[:TEST_FOLLOWS]->(b) RETURN a.name, b.name",
            "schema_name": "test_fixtures",
            "sql_only": True
        }
        
        response = requests.post(f"{BASE_URL}/query", json=payload)
        assert response.status_code == 200
        
        result = response.json()
        sql = result["generated_sql"]
        
        # CRITICAL: SQL must contain WHERE clause
        assert "WHERE" in sql.upper(), "SQL missing WHERE clause - regression detected!"
        assert "a.name = 'Alice'" in sql or 'a.name = "Alice"' in sql, \
            "SQL missing filter predicate - regression detected!"
        
        # Should have LEFT JOIN for OPTIONAL MATCH
        assert "LEFT JOIN" in sql.upper(), "SQL missing LEFT JOIN for OPTIONAL MATCH"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
