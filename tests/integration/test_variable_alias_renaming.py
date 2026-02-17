#!/usr/bin/env python3
"""
Integration tests for variable alias renaming in WITH clauses.

Tests the fix for: MATCH (u:User) WITH u AS person RETURN person.name
Issue: Variable renaming should preserve label information through WITH clause boundaries.
"""

import pytest
import json
import subprocess
from pathlib import Path

# Test configuration
SCHEMA_PATH = "./schemas/test/social_integration.yaml"
CLICKGRAPH_PORT = 8080
BASE_URL = f"http://localhost:{CLICKGRAPH_PORT}"


def query_clickgraph(cypher_query: str) -> dict:
    """Execute a Cypher query and return the response."""
    cmd = [
        "curl", "-s", "-X", "POST",
        f"{BASE_URL}/query",
        "-H", "Content-Type: application/json",
        "-d", json.dumps({"query": cypher_query, "schema_name": "social_integration"})
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        pytest.fail(f"Invalid JSON response: {result.stdout}")


class TestVariableAliasRenaming:
    """Test that variable alias renaming preserves type information through WITH clauses."""

    def test_simple_node_renaming(self):
        """Test: WITH u AS person RETURN person.name"""
        response = query_clickgraph(
            'MATCH (u:User) WITH u AS person RETURN person.name LIMIT 1'
        )
        
        # Should not have an error about property not found
        assert "error" not in response or response.get("error") is None, \
            f"Query failed: {response.get('error', 'Unknown error')}"
        
        # Should have results
        assert "results" in response, f"No results in response: {response}"
        assert len(response["results"]) > 0, "Expected at least one result"
        print(f"✓ Simple node renaming works: {response['results'][0]}")

    def test_multiple_renames_in_with(self):
        """Test: WITH u AS person, f AS friend RETURN person.name, friend.name"""
        response = query_clickgraph(
            'MATCH (u:User) MATCH (f:User) WITH u AS person, f AS friend RETURN person.user_id, friend.user_id LIMIT 1'
        )
        
        assert "error" not in response or response.get("error") is None, \
            f"Query failed: {response.get('error', 'Unknown error')}"
        assert "results" in response
        print(f"✓ Multiple renames work: {response['results'][0]}")

    def test_mixed_rename_and_pass_through(self):
        """Test: WITH u, f AS friend - some renamed, some not"""
        response = query_clickgraph(
            'MATCH (u:User) MATCH (f:User) WITH u, f AS friend RETURN u.user_id, friend.user_id LIMIT 1'
        )
        
        assert "error" not in response or response.get("error") is None, \
            f"Query failed: {response.get('error', 'Unknown error')}"
        assert "results" in response
        print(f"✓ Mixed rename and pass-through works: {response['results'][0]}")

    def test_renamed_node_in_subsequent_match(self):
        """Test: WITH u AS person ... MATCH (person)-[:FOLLOWS]->(f)"""
        response = query_clickgraph(
            'MATCH (u:User) WITH u AS person MATCH (person)-[:FOLLOWS]->(f:User) RETURN person.user_id, f.user_id LIMIT 1'
        )
        
        assert "error" not in response or response.get("error") is None, \
            f"Query failed: {response.get('error', 'Unknown error')}"
        assert "results" in response
        print(f"✓ Renamed node in subsequent MATCH works: {response['results'][0]}")

    def test_renamed_node_with_property_expression(self):
        """Test: WITH u.name AS name - property renaming"""
        response = query_clickgraph(
            'MATCH (u:User) WITH u.name AS user_name RETURN user_name LIMIT 1'
        )
        
        # This is a different case - property access, not variable renaming
        # But should still work
        assert "error" not in response or response.get("error") is None, \
            f"Query failed: {response.get('error', 'Unknown error')}"
        assert "results" in response
        print(f"✓ Property expression renaming works: {response['results'][0]}")

    def test_renamed_node_in_where_filter(self):
        """Test: WITH u AS person WHERE person.user_id = 1"""
        response = query_clickgraph(
            'MATCH (u:User) WITH u AS person WHERE person.user_id = 1 RETURN person.name'
        )
        
        assert "error" not in response or response.get("error") is None, \
            f"Query failed: {response.get('error', 'Unknown error')}"
        print(f"✓ Renamed node in WHERE filter works: {response['results']}")

    def test_chained_renaming(self):
        """Test: WITH u AS a, WITH a AS b - chained renaming"""
        response = query_clickgraph(
            'MATCH (u:User) WITH u AS a WITH a AS b RETURN b.name LIMIT 1'
        )
        
        assert "error" not in response or response.get("error") is None, \
            f"Query failed: {response.get('error', 'Unknown error')}"
        assert "results" in response
        print(f"✓ Chained renaming works: {response['results'][0]}")


if __name__ == "__main__":
    # Run tests with: pytest tests/integration/test_variable_alias_renaming.py -v
    pytest.main([__file__, "-v", "-s"])
