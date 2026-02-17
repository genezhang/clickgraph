"""
Tests for VLP + WITH clause path function rewriting (KNOWN_ISSUES.md #1 fix).

This tests the fix for: "length(path) in WITH clauses generates invalid SQL"

Core validated tests:
- TestVLPWithStandardSchema: All path functions (length, nodes, relationships) on social_integration schema

Other test classes are exploratory and may test pre-existing issues.
"""

import pytest
import requests


def query_api(query: str, schema_name: str = "social_integration", port: int = 8080) -> dict:
    """Execute a Cypher query against the API."""
    response = requests.post(
        f"http://localhost:{port}/query",
        json={"query": query, "schema_name": schema_name}
    )
    result = response.json()
    # Normalize API response format
    if "results" in result:
        return {"status": "success", "data": result["results"]}
    elif "error" in result or "Clickhouse Error" in result.get("message", ""):
        return {"status": "error", "error": result}
    return result


class TestVLPWithStandardSchema:
    """VLP + WITH on standard node/edge tables (separate users and follows tables)."""

    def test_length_path_in_with(self):
        """Standard schema: length(path) in WITH clause."""
        query = """
        USE social_integration
        MATCH path = (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        WITH u1, u2, length(path) as path_len
        WHERE path_len = 2
        RETURN u1.name, u2.name, path_len
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["path_len"] == 2

    def test_nodes_path_in_with(self):
        """Standard schema: nodes(path) in WITH clause."""
        query = """
        USE social_integration
        MATCH path = (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        WITH u1, u2, nodes(path) as path_nodes, length(path) as hops
        WHERE hops = 2
        RETURN u1.name, u2.name, size(path_nodes) as node_count
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["node_count"] == 3  # 2 hops = 3 nodes

    def test_relationships_path_in_with(self):
        """Standard schema: relationships(path) in WITH clause."""
        query = """
        USE social_integration
        MATCH path = (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        WITH u1, u2, relationships(path) as path_rels, length(path) as hops
        WHERE hops = 2
        RETURN u1.name, u2.name, size(path_rels) as rel_count
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["rel_count"] == 2  # 2 hops = 2 relationships

    def test_multiple_path_functions_in_with(self):
        """Standard schema: Multiple path functions in WITH clause."""
        query = """
        USE social_integration
        MATCH path = (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        WITH u1, u2, 
             length(path) as hops,
             nodes(path) as path_nodes,
             relationships(path) as path_rels
        WHERE hops = 2
        RETURN u1.name, u2.name, hops, 
               size(path_nodes) as node_count,
               size(path_rels) as rel_count
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["hops"] == 2
            assert row["node_count"] == 3
            assert row["rel_count"] == 2

    def test_with_non_path_properties(self):
        """Standard schema: Non-path properties in WITH clause."""
        query = """
        USE social_integration
        MATCH path = (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        WITH u1.name as start_name, u2.name as end_name
        WHERE start_name IS NOT NULL
        RETURN start_name, end_name
        ORDER BY end_name
        LIMIT 5
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["start_name"] is not None

    @pytest.mark.xfail(reason="GROUP BY with path functions not yet rewritten - separate issue")
    def test_with_aggregation_and_path_function(self):
        """Standard schema: Aggregation + path function in WITH clause."""
        query = """
        USE social_integration
        MATCH path = (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        WITH u1, length(path) as hops, COUNT(*) as path_count
        RETURN u1.name, hops, path_count
        ORDER BY hops
        """
        result = query_api(query)
        assert result["status"] == "success"
        assert len(result["data"]) >= 1


class TestVLPWithDenormalizedSchema:
    """VLP + WITH on denormalized edge tables (node properties in edge table)."""

    @pytest.mark.xfail(reason="Denormalized VLP + WITH not yet supported - pre-existing issue")
    def test_denorm_length_path_in_with(self):
        """Denormalized schema: length(path) in WITH clause."""
        query = """
        USE denormalized_flights_test
        MATCH path = (a:Airport)-[:FLIGHT*1..2]->(b:Airport)
        WHERE a.code = 'LAX'
        WITH a, b, length(path) as hops
        WHERE hops = 2
        RETURN a.city, b.city, hops
        ORDER BY b.city
        LIMIT 5
        """
        result = query_api(query, schema_name="denormalized_flights_test")
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["hops"] == 2

    @pytest.mark.xfail(reason="Denormalized VLP + WITH not yet supported - pre-existing issue")
    def test_denorm_nodes_path_in_with(self):
        """Denormalized schema: nodes(path) in WITH clause."""
        query = """
        USE denormalized_flights_test
        MATCH path = (a:Airport)-[:FLIGHT*1..2]->(b:Airport)
        WHERE a.code = 'LAX'
        WITH a, b, nodes(path) as path_nodes, length(path) as hops
        WHERE hops = 2
        RETURN a.city, b.city, size(path_nodes) as node_count
        ORDER BY b.city
        LIMIT 5
        """
        result = query_api(query, schema_name="denormalized_flights_test")
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["node_count"] == 3  # 2 hops = 3 nodes

    @pytest.mark.xfail(reason="Denormalized VLP + WITH not yet supported - pre-existing issue")
    def test_denorm_with_properties_and_path(self):
        """Denormalized schema: Properties + path function in WITH."""
        query = """
        USE denormalized_flights_test
        MATCH path = (a:Airport)-[:FLIGHT*1..2]->(b:Airport)
        WHERE a.code = 'LAX'
        WITH a.city as origin, b.city as dest, b.state as dest_state, length(path) as hops
        WHERE hops = 2
        RETURN origin, dest, dest_state, hops
        ORDER BY dest
        LIMIT 5
        """
        result = query_api(query, schema_name="denormalized_flights_test")
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["hops"] == 2
            assert row["origin"] == "Los Angeles"


class TestVLPWithMultipleRelTypes:
    """VLP + WITH with multiple relationship types (coupled edge tables)."""

    def test_multi_rel_length_path_in_with(self):
        """Multiple rel types: length(path) in WITH clause."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS|TEST_LIKES*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH u1, u2, length(path) as hops
        WHERE hops = 2
        RETURN u1.name, u2.name, hops
        LIMIT 5
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["hops"] == 2

    def test_multi_rel_relationships_in_with(self):
        """Multiple rel types: relationships(path) in WITH clause."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS|TEST_LIKES*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH u1, u2, relationships(path) as path_rels, length(path) as hops
        WHERE hops = 1
        RETURN u1.name, u2.name, size(path_rels) as rel_count
        LIMIT 5
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["rel_count"] == 1


class TestVLPWithComplexPatterns:
    """VLP + WITH with complex patterns: nested WITH, chaining, etc."""

    def test_chained_with_clauses(self):
        """Chained WITH clauses with path functions."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH u1, u2, length(path) as hops
        WHERE hops = 2
        WITH u1.name as start_name, u2.name as end_name, hops
        RETURN start_name, end_name, hops
        ORDER BY end_name
        LIMIT 5
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["hops"] == 2

    def test_with_order_by_limit(self):
        """WITH clause with ORDER BY and LIMIT."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH u1, u2, length(path) as hops
        ORDER BY hops DESC
        LIMIT 10
        WITH u1, u2, hops
        WHERE hops >= 1
        RETURN u1.name, u2.name, hops
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["hops"] >= 1

    def test_with_distinct(self):
        """WITH clause with DISTINCT."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH DISTINCT u2, length(path) as hops
        RETURN u2.name, hops
        ORDER BY u2.name
        LIMIT 5
        """
        result = query_api(query)
        assert result["status"] == "success"
        # Check that results are distinct by u2.name
        names = [row["u2.name"] for row in result["data"]]
        assert len(names) == len(set(names))

    def test_with_collect_and_path_function(self):
        """WITH clause with COLLECT and path function."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH u1, COLLECT(u2.name) as reachable, MAX(length(path)) as max_hops
        RETURN u1.name, reachable, max_hops
        """
        result = query_api(query)
        assert result["status"] == "success"
        assert len(result["data"]) == 1
        row = result["data"][0]
        assert isinstance(row["reachable"], list)
        assert row["max_hops"] >= 1


class TestVLPWithEdgeCases:
    """Edge cases for VLP + WITH combinations."""

    def test_zero_length_path_with_with(self):
        """Zero-length path (reflexive) with WITH clause."""
        query = """
        MATCH path = (u:TestUser)-[:TEST_FOLLOWS*0..1]->(u)
        WHERE u.user_id = 1
        WITH u, length(path) as hops
        WHERE hops = 0
        RETURN u.name, hops
        """
        result = query_api(query)
        assert result["status"] == "success"
        assert len(result["data"]) == 1
        assert result["data"][0]["hops"] == 0

    def test_unbounded_vlp_with_limit_in_with(self):
        """Unbounded VLP with LIMIT in WITH clause."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH u1, u2, length(path) as hops
        ORDER BY hops
        LIMIT 5
        WITH u1, u2, hops
        WHERE hops >= 1
        RETURN u1.name, u2.name, hops
        """
        result = query_api(query)
        assert result["status"] == "success"
        assert len(result["data"]) <= 5

    def test_path_function_in_where_and_with(self):
        """Path function used in both WHERE (before WITH) and WITH clause."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..3]->(u2:TestUser)
        WHERE u1.user_id = 1 AND length(path) <= 2
        WITH u1, u2, length(path) as hops
        WHERE hops = 2
        RETURN u1.name, u2.name, hops
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["hops"] == 2

    def test_with_arithmetic_on_path_function(self):
        """Arithmetic operations on path functions in WITH."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH u1, u2, length(path) * 2 as doubled_hops, length(path) as hops
        WHERE doubled_hops = 4
        RETURN u1.name, u2.name, hops, doubled_hops
        """
        result = query_api(query)
        assert result["status"] == "success"
        for row in result["data"]:
            assert row["hops"] == 2
            assert row["doubled_hops"] == 4
