"""
Test heterogeneous multi-type VLP (Variable-Length Path) patterns.

HETEROGENEOUS MULTI-TYPE VLP:
These are patterns where multiple edge types in the same VLP pattern have
different target node types. For example:
  MATCH (u:User)-[r:FOLLOWS|AUTHORED*1..1]->(neighbor)
  - FOLLOWS: User -> User
  - AUTHORED: User -> Post

Current Status: PARTIALLY WORKING (documented limitations)

The type inference correctly detects heterogeneous targets but the query
execution currently only uses the first valid target type, not all of them.
"""

import pytest
import requests


class TestHeterogeneousMultiTypeVLP:
    """Test heterogeneous multi-type VLP patterns."""

    @pytest.fixture(autouse=True)
    def setup(self, verify_clickgraph_running):
        """Ensure ClickGraph is running."""
        self.base_url = "http://localhost:8080"
        self.schema_name = "social_demo"

    def query(self, cypher, parameters=None):
        """Execute a Cypher query."""
        payload = {"query": cypher, "schema_name": self.schema_name}
        if parameters:
            payload["parameters"] = parameters
        response = requests.post(
            f"{self.base_url}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        return response

    @pytest.mark.xfail(reason="Heterogeneous multi-type VLP not fully supported - type inference only uses first target type")
    def test_heterogeneous_vlp_return_neighbor(self):
        """Test heterogeneous multi-type VLP returning neighbor nodes.
        
        Expected: Returns both User nodes (via FOLLOWS) and Post nodes (via AUTHORED)
        Actual: Only returns User nodes (first target type detected)
        """
        response = self.query(
            "MATCH (u:User)-[r:FOLLOWS|AUTHORED*1..1]->(neighbor) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id, neighbor.post_id "
            "LIMIT 10"
        )
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        # Should have both users (FOLLOWS) and posts (AUTHORED)
        assert len(data["results"]) >= 3

    @pytest.mark.xfail(reason="Heterogeneous multi-type VLP not fully supported")
    def test_heterogeneous_vlp_return_relationship_type(self):
        """Test heterogeneous multi-type VLP returning relationship type."""
        response = self.query(
            "MATCH (u:User)-[r:FOLLOWS|AUTHORED*1..1]->(neighbor) "
            "WHERE u.user_id = 1 "
            "RETURN type(r) as rel_type "
            "LIMIT 5"
        )
        assert response.status_code == 200, f"Query failed: {response.text}"

    def test_homogeneous_multi_type_vlp(self):
        """Test multi-type VLP with same target type - this should work."""
        response = self.query(
            "MATCH (u:User)-[r:FOLLOWS*1..1]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id, r.type "
            "LIMIT 5"
        )
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert len(data["results"]) >= 1

    def test_single_type_vlp_depth_1(self):
        """Test single-type VLP depth=1 - baseline test."""
        response = self.query(
            "MATCH (u:User)-[r:FOLLOWS*1..1]->(neighbor) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id, r.type "
            "LIMIT 5"
        )
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert len(data["results"]) >= 1

    def test_single_type_vlp_depth_2(self):
        """Test single-type VLP depth=2 - baseline test."""
        response = self.query(
            "MATCH (u:User)-[r:FOLLOWS*1..2]->(neighbor) "
            "WHERE u.user_id = 1 "
            "RETURN DISTINCT neighbor.user_id "
            "LIMIT 10"
        )
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        # Should have both 1-hop and 2-hop neighbors
        assert len(data["results"]) >= 3


class TestWorkaroundForHeterogeneousVLP:
    """Workarounds for heterogeneous multi-type VLP."""

    @pytest.fixture(autouse=True)
    def setup(self, verify_clickgraph_running):
        """Ensure ClickGraph is running."""
        self.base_url = "http://localhost:8080"
        self.schema_name = "social_demo"

    def query(self, cypher, parameters=None):
        """Execute a Cypher query."""
        payload = {"query": cypher, "schema_name": self.schema_name}
        if parameters:
            payload["parameters"] = parameters
        response = requests.post(
            f"{self.base_url}/query",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        return response

    def test_workaround_union_separate_queries(self):
        """Workaround: Use UNION of separate queries for each edge type."""
        # Query 1: FOLLOWS -> User
        response1 = self.query(
            "MATCH (u:User)-[r:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id as id, 'User' as type, 'FOLLOWS' as rel_type "
            "LIMIT 5"
        )
        assert response1.status_code == 200
        
        # Query 2: AUTHORED -> Post
        response2 = self.query(
            "MATCH (u:User)-[r:AUTHORED]->(neighbor:Post) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.post_id as id, 'Post' as type, 'AUTHORED' as rel_type "
            "LIMIT 5"
        )
        assert response2.status_code == 200

    def test_workaround_explicit_target_labels(self):
        """Workaround: Specify target label explicitly."""
        # Query for FOLLOWS -> User
        response1 = self.query(
            "MATCH (u:User)-[r:FOLLOWS*1..1]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id as id, 'User' as target_type "
            "LIMIT 5"
        )
        assert response1.status_code == 200
        
        # Query for AUTHORED -> Post  
        response2 = self.query(
            "MATCH (u:User)-[r:AUTHORED*1..1]->(neighbor:Post) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.post_id as id, 'Post' as target_type "
            "LIMIT 5"
        )
        assert response2.status_code == 200
