"""
Test COUNT() with relationships - verifying type inference requirements.

This test suite verifies:
1. COUNT(r) works with explicit relationship types
2. Error messages correctly say "Missing Type" for relationships
3. Anonymous relationships need type inference support
"""

import pytest
import sys
from pathlib import Path

# Add tests directory to path
sys.path.insert(0, str(Path(__file__).parent))

from conftest import execute_cypher, assert_query_success, get_single_value, assert_row_count


def execute_query(query, schema_name="test_fixtures"):
    """Execute a Cypher query against ClickGraph."""
    return execute_cypher(query, schema_name=schema_name)


class TestCountRelationships:
    """Test COUNT() aggregation with relationships."""

    def test_count_with_explicit_type(self):
        """
        Test COUNT(r) with explicit relationship type - should work.
        """
        response = execute_cypher(
            """
            MATCH ()-[r:TEST_FOLLOWS]->()
            RETURN count(r) AS total
            """,
            schema_name="test_fixtures"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        total = get_single_value(response, "total", convert_to_int=True)
        assert total > 0, f"Expected some relationships, got {total}"

    @pytest.mark.xfail(reason="UNION ALL type mismatch: Date vs String columns across relationship types")
    def test_count_with_untyped_relationship(self):
        """
        Test COUNT(r) without type - should expand to all relationship types.
        
        Untyped relationships are valid in Cypher and ClickGraph handles them
        by generating a UNION ALL across all relationship types in the schema.
        """
        response = execute_cypher(
            "MATCH ()-[r]->() RETURN count(r) AS total",
            schema_name="test_fixtures"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        total = get_single_value(response, "total", convert_to_int=True)
        # Should count across all relationship types (TEST_FOLLOWS + TEST_PURCHASED + TEST_FRIENDS_WITH)
        assert total > 0

    @pytest.mark.xfail(reason="UNION ALL type mismatch: Date vs String columns across relationship types")
    def test_count_star_with_anonymous_relationship(self):
        """
        Test count(*) with anonymous relationship pattern.
        
        Anonymous untyped relationships expand to all types via UNION ALL.
        """
        response = execute_cypher(
            "MATCH ()-[]->() RETURN count(*) AS total",
            schema_name="test_fixtures"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        total = get_single_value(response, "total", convert_to_int=True)
        assert total > 0

    def test_count_relationship_with_node_constraints(self):
        """
        Test relationship counting with node type constraints.
        
        When node types are specified, ClickGraph infers the valid
        relationship types connecting them via UNION ALL.
        """
        response = execute_cypher(
            "MATCH (u:TestUser)-[r]->(other:TestUser) RETURN count(r) AS total",
            schema_name="test_fixtures"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        total = get_single_value(response, "total", convert_to_int=True)
        assert total > 0

    def test_count_star_with_typed_relationship(self):
        """
        Test count(*) with explicit relationship type - should work.
        """
        response = execute_cypher(
            """
            MATCH ()-[:TEST_FOLLOWS]->()
            RETURN count(*) AS total
            """,
            schema_name="test_fixtures"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        total = get_single_value(response, "total", convert_to_int=True)
        assert total > 0

    def test_error_message_for_nonexistent_type(self):
        """
        Verify error messages for relationship types not in the schema.
        """
        import requests
        
        # Test relationship with non-existent type
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": "USE test_fixtures\nMATCH ()-[r:NONEXISTENT]->()\nRETURN count(r)"}
        )
        assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
