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

    def test_count_with_missing_type_shows_correct_error(self):
        """
        Test COUNT(r) without type - should show 'Missing Type' error.
        
        This verifies the terminology fix: relationships use "Type" not "Label".
        """
        # Note: execute_cypher raises on error, so we need to catch it
        import requests
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": "USE test_fixtures\nMATCH ()-[r]->()\nRETURN count(r)"}
        )
        
        assert response.status_code == 500
        assert "Missing type for relationship" in response.text
        assert "Missing label" not in response.text.lower()  # Should NOT say "label"

    @pytest.mark.xfail(reason="Anonymous relationships require type inference - not yet implemented")
    def test_count_star_with_anonymous_relationship(self):
        """
        Test count(*) with anonymous relationship pattern.
        
        This should work because we're not selecting relationship properties,
        but currently fails with "Missing type for relationship `t11`".
        
        Expected behavior: ClickGraph should infer that ANY relationship type
        is acceptable when no variable name is given and only count(*) is used.
        """
        import requests
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": "USE test_fixtures\nMATCH ()-[]->() RETURN count(*) AS total"}
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        result = response.json()
        assert "results" in result
        assert result["results"][0]["total"] > 0

    @pytest.mark.xfail(reason="Relationship type inference not yet implemented")
    def test_count_relationship_with_node_constraints(self):
        """
        Test relationship counting with node type constraints.
        
        When node types are specified, ClickGraph could potentially infer
        the relationship types connecting them.
        """
        import requests
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": "USE test_fixtures\nMATCH (u:TestUser)-[r]->(other:TestUser)\nRETURN count(r) AS total"}
        )
        
        assert response.status_code == 200
        result = response.json()
        assert result["results"][0]["total"] > 0

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

    def test_error_message_node_vs_relationship(self):
        """
        Verify error messages use correct terminology:
        - Nodes: "Missing label" (when that occurs)
        - Relationships: "Missing type"
        """
        import requests
        
        # Test relationship without type - should say "Missing type"
        response_rel = requests.post(
            "http://localhost:8080/query",
            json={"query": "USE test_fixtures\nMATCH ()-[r]->()\nRETURN count(r)"}
        )
        assert response_rel.status_code == 500
        assert "Missing type for relationship" in response_rel.text


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
