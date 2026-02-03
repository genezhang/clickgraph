"""
Integration tests for Track C: Property-based UNION branch pruning

Tests that untyped patterns with WHERE property conditions only query
node/relationship types that have those properties.
"""

import pytest
import requests
import os

BASE_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")


class TestPropertyFilteringNodes:
    """Test property-based filtering for node patterns"""

    def test_single_property_bytes_sent(self):
        """
        Query: MATCH (n) WHERE n.bytes_sent > 100
        Expected: Only node types with bytes_sent property queried
        """
        query = """
        USE test_fixtures
        MATCH (n) WHERE n.bytes_sent > 100
        RETURN n.bytes_sent AS bytes
        LIMIT 5
        """
        
        response = requests.post(
            f"{BASE_URL}/query",
            json={"query": query}
        )
        
        assert response.status_code == 200, f"Query failed: {response.text}"
        result = response.json()
        
        # Should return results
        assert "results" in result
        
        # Verify results have bytes_sent field > 100
        if result["results"]:
            for row in result["results"]:
                assert "bytes" in row
                assert row["bytes"] > 100

    def test_property_filter_user_id(self):
        """
        Query: MATCH (n) WHERE n.user_id = 1
        Expected: Only User-like types queried (have user_id property)
        """
        query = """
        USE test_fixtures
        MATCH (n) WHERE n.user_id = 1
        RETURN n.user_id AS uid
        """
        
        response = requests.post(
            f"{BASE_URL}/query",
            json={"query": query}
        )
        
        assert response.status_code == 200, f"Query failed: {response.text}"
        result = response.json()
        
        # Should return results
        assert "results" in result
        if result["results"]:
            assert result["results"][0]["uid"] == 1

    def test_nonexistent_property_returns_empty(self):
        """
        Query: MATCH (n) WHERE n.nonexistent_property = 123
        Expected: 0 results (no types have this property)
        """
        query = """
        USE test_fixtures
        MATCH (n) WHERE n.nonexistent_xyz_999 = 123
        RETURN n
        """
        
        response = requests.post(
            f"{BASE_URL}/query",
            json={"query": query}
        )
        
        # Query should succeed but return 0 results
        assert response.status_code == 200, f"Query failed: {response.text}"
        result = response.json()
        
        assert "results" in result
        # Should return empty (LogicalPlan::Empty optimization)
        assert len(result["results"]) == 0

    def test_multiple_properties_must_intersect(self):
        """
        Query: MATCH (n) WHERE n.prop1 = 1 AND n.prop2 = 2
        Expected: Only types with BOTH properties queried
        """
        query = """
        USE test_fixtures
        MATCH (n) WHERE n.user_id = 1 AND n.user_id IS NOT NULL
        RETURN n.user_id AS uid
        """
        
        response = requests.post(
            f"{BASE_URL}/query",
            json={"query": query}
        )
        
        assert response.status_code == 200, f"Query failed: {response.text}"
        result = response.json()
        
        assert "results" in result


class TestPropertyFilteringRelationships:
    """Test property-based filtering for relationship patterns"""

    @pytest.mark.skip(reason="Untyped relationship patterns not yet supported")
    def test_relationship_property_filter(self):
        """
        Query: MATCH ()-[r]->() WHERE r.property IS NOT NULL
        Expected: Only relationship types with that property queried
        """
        query = """
        USE test_fixtures
        MATCH ()-[r]->() WHERE r.follow_date IS NOT NULL
        RETURN r.follow_date AS date
        LIMIT 5
        """
        
        response = requests.post(
            f"{BASE_URL}/query",
            json={"query": query}
        )
        
        assert response.status_code == 200, f"Query failed: {response.text}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
