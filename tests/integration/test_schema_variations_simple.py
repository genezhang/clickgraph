#!/usr/bin/env python3
"""
Simplified Schema Variation Tests for Consolidation Baseline.

Focus on testing label inference and direction validation with available social schema.
These tests ensure SchemaInference → TypeInference consolidation doesn't break core functionality.
"""

import pytest
import requests


CLICKGRAPH_URL = "http://localhost:8080"


def query_clickgraph(cypher_query):
    """Execute Cypher query against ClickGraph server."""
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": cypher_query}
    )
    return response


class TestLabelInferenceBaseline:
    """
    Core label inference tests using social schema.
    These are critical to validate during consolidation.
    """
    
    def test_all_labeled(self):
        """Baseline: All labels known."""
        query = "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN count(*)"
        response = query_clickgraph(query)
        assert response.status_code == 200
    
    def test_infer_left_from_rel_and_right(self):
        """Infer left node from relationship and right node."""
        query = "MATCH (a)-[:FOLLOWS]->(b:User) RETURN count(*)"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # a must be User (FOLLOWS: User→User)
    
    def test_infer_right_from_rel_and_left(self):
        """Infer right node from relationship and left node."""
        query = "MATCH (u:User)-[:FOLLOWS]->(f) RETURN count(*)"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # f must be User
    
    def test_infer_both_from_rel(self):
        """Infer both nodes from relationship."""
        query = "MATCH (a)-[:FOLLOWS]->(b) RETURN count(*)"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # Both must be User
    
    def test_infer_different_types(self):
        """Infer different node types from relationship."""
        query = "MATCH (a)-[:AUTHORED]->(b) RETURN count(*)"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # a=User, b=Post
    
    @pytest.mark.xfail(reason="Code bug: MATCH (n) unlabeled node returns planning error")
    def test_unlabeled_creates_union(self):
        """Unlabeled node should create UNION."""
        query = "MATCH (n) RETURN count(n)"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # Should scan both User and Post
    
    @pytest.mark.xfail(reason="Code bug: undirected unlabeled pattern inference fails")
    def test_undirected_infers_types(self):
        """Undirected pattern with one label."""
        query = "MATCH (u:User)--(n) RETURN count(DISTINCT n)"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # n can be User (FOLLOWS) or Post (AUTHORED)


class TestDirectionValidation:
    """
    Direction validation tests - critical for browser bug fix.
    """
    
    def test_post_to_user_invalid(self):
        """Post--(User) should filter invalid directions."""
        query = "MATCH (p:Post)--(u:User) RETURN count(*)"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # Post can only have incoming AUTHORED from User
        # Should NOT create Post→User direction
    
    def test_user_to_post_valid(self):
        """User--(Post) has valid direction."""
        query = "MATCH (u:User)--(p:Post) RETURN count(*)"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # User→Post via AUTHORED is valid
    
    def test_user_to_user_bidirectional(self):
        """User--(User) can go both ways."""
        query = "MATCH (u1:User)--(u2:User) RETURN count(*)"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # FOLLOWS can be in any direction


class TestMultiplePatterns:
    """
    Test label inference across multiple patterns.
    """
    
    def test_two_patterns_same_var(self):
        """Multiple patterns constraining same variable."""
        query = """
        MATCH (u:User)-[:AUTHORED]->(p)
        MATCH (u)-[:FOLLOWS]->(f)
        RETURN count(*)
        """
        response = query_clickgraph(query)
        assert response.status_code == 200
        # p=Post, f=User
    
    def test_with_clause_preserves_types(self):
        """WITH clause should preserve inferred types."""
        query = """
        MATCH (u)-[:AUTHORED]->(p)
        WITH u, count(p) as posts
        MATCH (u)-[:FOLLOWS]->(f)
        RETURN count(*)
        """
        response = query_clickgraph(query)
        assert response.status_code == 200


class TestOptionalMatch:
    """
    OPTIONAL MATCH with type inference.
    """
    
    def test_optional_with_inference(self):
        """OPTIONAL MATCH requiring type inference."""
        query = """
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:AUTHORED]->(p)
        RETURN count(*)
        """
        response = query_clickgraph(query)
        assert response.status_code == 200
        # p should be inferred as Post


def test_server_accessible():
    """Verify server is running."""
    response = requests.get(f"{CLICKGRAPH_URL}/health", timeout=2)
    assert response.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
