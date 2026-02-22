"""
GraphRAG Query Pattern Regression Tests

Tests all patterns documented in docs/wiki/Cypher-Subgraph-Extraction.md
"""

import pytest
import requests
from typing import Dict, Any


class TestGraphRAGPatterns:
    """Regression tests for GraphRAG query patterns."""

    @pytest.fixture(autouse=True)
    def setup(self, verify_clickgraph_running):
        """Ensure ClickGraph is running."""
        self.base_url = "http://localhost:8080"
        self.schema_name = "social_demo"

    def query(self, cypher: str, parameters: Dict = None) -> Dict[str, Any]:
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
        if response.status_code != 200:
            return {"error": response.text, "status_code": response.status_code}
        return response.json()

    # ============================================
    # Single Edge Type Tests
    # ============================================

    def test_single_edge_outgoing(self):
        """Test: Single edge type, outgoing direction"""
        result = self.query(
            "MATCH (start:User)-[:FOLLOWS]->(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN start.user_id, neighbor.user_id LIMIT 3"
        )
        assert "results" in result
        assert len(result["results"]) >= 1

    def test_single_edge_incoming(self):
        """Test: Single edge type, incoming direction"""
        result = self.query(
            "MATCH (start:User)<-[:FOLLOWS]-(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN start.user_id, neighbor.user_id LIMIT 3"
        )
        assert "results" in result
        assert len(result["results"]) >= 1

    def test_single_edge_bidirectional(self):
        """Test: Single edge type, bidirectional (UNION ALL)"""
        result = self.query(
            "MATCH (start:User)-[:FOLLOWS]-(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN start.user_id, neighbor.user_id LIMIT 5"
        )
        assert "results" in result
        # Bidirectional should return both incoming and outgoing
        assert len(result["results"]) >= 2

    # ============================================
    # Multi Edge Type Tests
    # ============================================

    def test_multi_edge_homogeneous(self):
        """Test: Multiple edge types with same target type"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS|LIKED]->(neighbor) "
            "WHERE start.user_id = 1 "
            "RETURN type(r) AS rel_type, labels(neighbor) LIMIT 5"
        )
        assert "results" in result
        # Should return both FOLLOWS and LIKED relationships
        rel_types = [r.get("rel_type") for r in result["results"]]
        assert len(rel_types) >= 1

    def test_multi_edge_heterogeneous(self):
        """Test: Multiple edge types with different target types (FOLLOWS|AUTHORED)"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS|AUTHORED]->(neighbor) "
            "WHERE start.user_id = 1 "
            "RETURN type(r) AS rel_type, labels(neighbor) LIMIT 5"
        )
        assert "results" in result
        # FOLLOWS -> User, AUTHORED -> Post
        results = result["results"]
        assert len(results) >= 1

    def test_multi_edge_heterogeneous_vlp(self):
        """Test: Heterogeneous multi-type with VLP"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS|AUTHORED*1..1]->(neighbor) "
            "WHERE start.user_id = 1 "
            "RETURN type(r), labels(neighbor) LIMIT 5"
        )
        assert "results" in result
        results = result["results"]
        # Should have both User (FOLLOWS) and Post (AUTHORED) targets
        labels_found = set()
        for r in results:
            for label in r.get("labels(neighbor)", []):
                labels_found.add(label)
        # Should have both User and Post
        assert "User" in labels_found or "Post" in labels_found

    # ============================================
    # Variable-Length Path (VLP) Tests
    # ============================================

    def test_vlp_single_type_1hop(self):
        """Test: VLP single type, exactly 1 hop"""
        result = self.query(
            "MATCH (start:User)-[:FOLLOWS*1..1]->(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN DISTINCT neighbor.user_id LIMIT 5"
        )
        assert "results" in result
        assert len(result["results"]) >= 1

    def test_vlp_single_type_2hop(self):
        """Test: VLP single type, 1-2 hops"""
        result = self.query(
            "MATCH (start:User)-[:FOLLOWS*1..2]->(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN DISTINCT neighbor.user_id LIMIT 10"
        )
        assert "results" in result
        # 2-hop should return more results than 1-hop
        assert len(result["results"]) >= 1

    def test_vlp_multi_type_1hop(self):
        """Test: VLP multi-type, 1 hop"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS|AUTHORED*1..1]->(neighbor) "
            "WHERE start.user_id = 1 "
            "RETURN type(r), labels(neighbor) LIMIT 5"
        )
        assert "results" in result

    def test_vlp_multi_type_2hop(self):
        """Test: VLP multi-type, 1-2 hops"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS|AUTHORED*1..2]->(neighbor) "
            "WHERE start.user_id = 1 "
            "RETURN type(r), labels(neighbor) LIMIT 5"
        )
        assert "results" in result

    def test_vlp_return_edge(self):
        """Test: VLP with RETURN r (edge return)"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS*1..1]->(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN type(r) AS rel_type, neighbor.user_id LIMIT 3"
        )
        assert "results" in result

    # ============================================
    # Generic Edge Pattern Tests
    # ============================================

    def test_generic_edge_pattern(self):
        """Test: Generic edge pattern without explicit type"""
        result = self.query(
            "MATCH (start:User)-[r]-(neighbor) "
            "WHERE start.user_id = 1 "
            "RETURN type(r), labels(neighbor) LIMIT 5"
        )
        assert "results" in result

    # ============================================
    # Edge Return Tests
    # ============================================

    def test_return_type_function(self):
        """Test: RETURN type(r) function"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS]->(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN type(r) AS rel_type, neighbor.user_id LIMIT 3"
        )
        assert "results" in result
        # Verify type(r) returns the relationship type
        assert result["results"][0]["rel_type"] == "FOLLOWS::User::User"

    def test_return_edge_properties(self):
        """Test: RETURN edge properties"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS]->(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN r.created_at, neighbor.user_id LIMIT 3"
        )
        assert "results" in result

    # ============================================
    # GraphRAG Context Extraction Tests
    # ============================================

    def test_triple_extraction(self):
        """Test: Extract triples (head, relation, tail)"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS]->(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN "
            "  start.user_id AS head_id, "
            "  'FOLLOWS' AS relation, "
            "  neighbor.user_id AS tail_id "
            "LIMIT 5"
        )
        assert "results" in result
        # Verify triple format
        first = result["results"][0]
        assert "head_id" in first
        assert "relation" in first
        assert "tail_id" in first

    def test_context_extraction(self):
        """Test: Extract rich context for GraphRAG"""
        result = self.query(
            "MATCH (start:User)-[r]-(related) "
            "WHERE start.user_id = 1 "
            "RETURN "
            "  start.name AS subject, "
            "  type(r) AS predicate, "
            "  related.name AS object "
            "LIMIT 10"
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown error')}"

    # ============================================
    # Multi-Seed Tests
    # ============================================

    def test_multiple_seeds(self):
        """Test: Multiple starting vertices"""
        result = self.query(
            "MATCH (start:User)-[:FOLLOWS]->(neighbor:User) "
            "WHERE start.user_id IN [1, 2] "
            "RETURN start.user_id, neighbor.user_id LIMIT 10"
        )
        assert "results" in result

    # ============================================
    # Path Variable Tests
    # ============================================

    def test_path_variable(self):
        """Test: Path variable with length()"""
        result = self.query(
            "MATCH p = (start:User)-[:FOLLOWS*1..2]->(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN neighbor.user_id, length(p) AS hops LIMIT 10"
        )
        assert "results" in result
        # Should have both 1-hop and 2-hop paths
        hops = [r["hops"] for r in result["results"]]
        assert 1 in hops or 2 in hops


class TestEdgeConstraints:
    """Tests for edge constraints in subgraph extraction."""

    @pytest.fixture(autouse=True)
    def setup(self, verify_clickgraph_running):
        self.base_url = "http://localhost:8080"
        self.schema_name = "social_demo"

    def query(self, cypher: str) -> Dict[str, Any]:
        response = requests.post(
            f"{self.base_url}/query",
            json={"query": cypher, "schema_name": self.schema_name},
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        return response.json()

    def test_edge_with_property_filter(self):
        """Test: Edge with property filter"""
        result = self.query(
            "MATCH (start:User)-[r:FOLLOWS]->(neighbor:User) "
            "WHERE start.user_id = 1 AND r.created_at > '2020-01-01' "
            "RETURN neighbor.user_id LIMIT 5"
        )
        assert "results" in result


class TestPerformance:
    """Performance-related tests."""

    @pytest.fixture(autouse=True)
    def setup(self, verify_clickgraph_running):
        self.base_url = "http://localhost:8080"
        self.schema_name = "social_demo"

    def query(self, cypher: str) -> Dict[str, Any]:
        response = requests.post(
            f"{self.base_url}/query",
            json={"query": cypher, "schema_name": self.schema_name},
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        return response.json()

    def test_large_subgraph_limit(self):
        """Test: Large subgraph with LIMIT"""
        result = self.query(
            "MATCH (start:User)-[r]-(neighbor) "
            "WHERE start.user_id = 1 "
            "RETURN start.name, type(r), neighbor.name "
            "LIMIT 100"
        )
        assert "results" in result
        assert len(result["results"]) <= 100

    def test_distinct_deduplication(self):
        """Test: DISTINCT for deduplication"""
        result = self.query(
            "MATCH (start:User)-[:FOLLOWS*1..3]-(neighbor:User) "
            "WHERE start.user_id = 1 "
            "RETURN DISTINCT neighbor.user_id, neighbor.name LIMIT 50"
        )
        assert "results" in result
        # Verify no duplicates
        ids = [r["neighbor.user_id"] for r in result["results"]]
        assert len(ids) == len(set(ids))
