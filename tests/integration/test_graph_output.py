"""
Integration tests for Graph output format (format=Graph).

Tests the structured graph response that returns deduplicated nodes and edges
instead of flat JSONEachRow results.

Requires: Running ClickGraph server with social_integration schema loaded.
"""

import pytest
import requests
import os

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")


def query_graph(cypher: str, schema: str = "social_integration", **extra_fields):
    """Execute a Cypher query with format=Graph."""
    payload = {
        "query": f"USE {schema} {cypher}",
        "format": "Graph",
        **extra_fields,
    }
    resp = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def query_standard(cypher: str, schema: str = "social_integration"):
    """Execute a Cypher query with default JSONEachRow format."""
    payload = {"query": f"USE {schema} {cypher}"}
    resp = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


class TestGraphFormatBasic:
    """Basic graph format response structure tests."""

    def test_graph_format_returns_nodes_and_edges(self):
        """Graph format should return nodes, edges, and stats for a pattern query."""
        result = query_graph(
            "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f LIMIT 5"
        )

        assert "nodes" in result, f"Expected 'nodes' in response, got: {list(result.keys())}"
        assert "edges" in result, f"Expected 'edges' in response, got: {list(result.keys())}"
        assert "stats" in result, f"Expected 'stats' in response, got: {list(result.keys())}"

        # Should have at least one node and one edge
        assert len(result["nodes"]) > 0, "Expected at least one node"
        assert len(result["edges"]) > 0, "Expected at least one edge"

        # Verify node structure
        node = result["nodes"][0]
        assert "element_id" in node
        assert "labels" in node
        assert "properties" in node
        assert isinstance(node["labels"], list)
        assert isinstance(node["properties"], dict)

        # Verify edge structure
        edge = result["edges"][0]
        assert "element_id" in edge
        assert "rel_type" in edge
        assert "start_node_element_id" in edge
        assert "end_node_element_id" in edge
        assert "properties" in edge

    def test_graph_format_stats_fields(self):
        """Stats object should contain timing breakdown."""
        result = query_graph("MATCH (u:User) RETURN u LIMIT 1")

        stats = result["stats"]
        assert "total_time_ms" in stats
        assert "parse_time_ms" in stats
        assert "planning_time_ms" in stats
        assert "execution_time_ms" in stats
        assert "query_type" in stats
        assert stats["query_type"] == "read"
        assert stats["total_time_ms"] > 0


class TestGraphFormatDeduplication:
    """Tests that graph format deduplicates nodes/edges across rows."""

    def test_graph_format_deduplicates_nodes(self):
        """Same node appearing in multiple rows should be returned once."""
        result = query_graph(
            "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f LIMIT 10"
        )

        # Check uniqueness of node element_ids
        node_ids = [n["element_id"] for n in result["nodes"]]
        assert len(node_ids) == len(set(node_ids)), (
            f"Duplicate nodes found: {node_ids}"
        )

        # Check uniqueness of edge element_ids
        edge_ids = [e["element_id"] for e in result["edges"]]
        assert len(edge_ids) == len(set(edge_ids)), (
            f"Duplicate edges found: {edge_ids}"
        )


class TestGraphFormatScalar:
    """Tests for scalar-only queries with graph format."""

    def test_graph_format_scalar_only_query(self):
        """Scalar-only query should return empty nodes and edges."""
        result = query_graph("MATCH (u:User) RETURN u.name LIMIT 5")

        assert result["nodes"] == [], f"Expected empty nodes for scalar query, got: {result['nodes']}"
        assert result["edges"] == [], f"Expected empty edges for scalar query, got: {result['edges']}"
        assert "stats" in result


class TestGraphFormatNodeOnly:
    """Tests for node-only queries with graph format."""

    def test_graph_format_node_query(self):
        """Node-only query should return nodes but no edges."""
        result = query_graph("MATCH (u:User) RETURN u LIMIT 5")

        assert len(result["nodes"]) > 0, "Expected at least one node"
        assert result["edges"] == [], f"Expected empty edges for node-only query, got: {result['edges']}"

        # Each node should have User label
        for node in result["nodes"]:
            assert "User" in node["labels"]
