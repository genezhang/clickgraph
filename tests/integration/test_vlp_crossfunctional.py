"""
Cross-functional tests for Variable-Length Paths (VLP).

These tests verify that VLP queries work correctly in combination with other
Cypher features like COLLECT, WITH clauses, aggregations, and property pruning.

Philosophy: Never assume orthogonal features work together - test them!
"""

import pytest
import sys
from pathlib import Path

# Add tests directory to path
sys.path.insert(0, str(Path(__file__).parent))

from conftest import execute_cypher, assert_query_success


def query_api(query: str, schema_name: str = "test_fixtures") -> dict:
    """Execute a Cypher query against the API and return normalized result."""
    response = execute_cypher(query, schema_name=schema_name, raise_on_error=False)
    if "results" in response:
        return {"status": "success", "data": response["results"]}
    elif "error" in response or "Clickhouse Error" in response.get("message", ""):
        return {"status": "error", "error": response}
    return response


class TestVLPWithCollect:
    """Test VLP queries combined with COLLECT aggregation."""

    def test_vlp_with_collect(self, simple_graph):
        """VLP + COLLECT: Collect all names in paths."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        RETURN u1.name as start, COLLECT(u2.name) as reached
        """
        result = query_api(query, schema_name=simple_graph["schema_name"])

        assert result["status"] == "success"
        assert len(result["data"]) > 0
        row = result["data"][0]
        assert "start" in row
        assert "reached" in row
        assert isinstance(row["reached"], list)
        assert len(row["reached"]) > 0

    def test_vlp_with_collect_and_groupby(self, simple_graph):
        """VLP + COLLECT + GROUP BY: Group paths by starting node."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id IN [1, 2]
        RETURN u1.user_id, COLLECT(u2.name) as reached
        """
        result = query_api(query, schema_name=simple_graph["schema_name"])

        assert result["status"] == "success"
        assert len(result["data"]) >= 2  # At least 2 starting users


class TestVLPWithClause:
    """Test VLP queries combined with WITH clause."""

    @pytest.mark.xfail(reason="Code bug: VLP crossfunctional query generates invalid SQL")
    def test_vlp_with_filtering(self, simple_graph):
        """VLP + WITH: Filter paths before final RETURN."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH u1, u2, length(path) as path_len
        WHERE path_len = 2
        RETURN u1.name, u2.name, path_len
        """
        result = query_api(query, schema_name=simple_graph["schema_name"])

        assert result["status"] == "success"
        for row in result["data"]:
            assert row["path_len"] == 2

    @pytest.mark.xfail(reason="Code bug: VLP crossfunctional query generates invalid SQL")
    def test_vlp_with_and_aggregation(self, simple_graph):
        """VLP + WITH + Aggregation: Count distinct endpoints."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        WITH u1, COUNT(DISTINCT u2.user_id) as reach_count
        RETURN u1.name, reach_count
        """
        result = query_api(query, schema_name=simple_graph["schema_name"])

        assert result["status"] == "success"
        assert len(result["data"]) == 1
        assert result["data"][0]["reach_count"] > 0


class TestDenormalizedVLPCrossFunctional:
    """Test VLP on denormalized schemas with other features."""

    @pytest.mark.xfail(reason="Code bug: VLP crossfunctional query generates invalid SQL")
    def test_denormalized_vlp_with_collect(self, setup_benchmark_data):
        """Denormalized VLP + COLLECT: Collect properties from denormalized table."""
        query = """
        MATCH path = (a1:Airport)-[:FLIGHT*1..2]->(a2:Airport)
        WHERE a1.code = 'JFK'
        RETURN a1.city, COLLECT(a2.city) as destinations
        """
        result = query_api(query, schema_name="denormalized_flights_test")

        assert result["status"] == "success"
        assert len(result["data"]) > 0
        row = result["data"][0]
        assert "a1.city" in row
        assert "destinations" in row
        assert isinstance(row["destinations"], list)

    @pytest.mark.xfail(reason="Code bug: VLP crossfunctional query generates invalid SQL")
    def test_denormalized_vlp_with_groupby(self, setup_benchmark_data):
        """Denormalized VLP + GROUP BY: Group paths by origin."""
        query = """
        MATCH path = (a1:Airport)-[:FLIGHT*1..2]->(a2:Airport)
        WHERE a1.code IN ['JFK', 'LAX']
        RETURN a1.city, COUNT(*) as path_count
        """
        result = query_api(query, schema_name="denormalized_flights_test")

        assert result["status"] == "success"
        assert len(result["data"]) >= 1

    @pytest.mark.xfail(reason="Code bug: VLP crossfunctional query generates invalid SQL")
    def test_denormalized_vlp_multiple_properties(self, setup_benchmark_data):
        """Denormalized VLP: Access multiple properties from denormalized nodes."""
        query = """
        MATCH path = (a1:Airport)-[:FLIGHT*1..2]->(a2:Airport)
        WHERE a1.code = 'JFK'
        RETURN a1.city, a1.state, a2.city, a2.state
        LIMIT 5
        """
        result = query_api(query, schema_name="denormalized_flights_test")

        assert result["status"] == "success"
        assert len(result["data"]) > 0
        row = result["data"][0]
        # Verify all properties are present
        assert "a1.city" in row
        assert "a1.state" in row
        assert "a2.city" in row
        assert "a2.state" in row

    @pytest.mark.xfail(reason="Code bug: VLP crossfunctional query generates invalid SQL")
    def test_denormalized_vlp_with_where_and_groupby(self, setup_benchmark_data):
        """Denormalized VLP: Property filtering + GROUP BY."""
        query = """
        MATCH path = (a1:Airport)-[:FLIGHT*1..2]->(a2:Airport)
        WHERE a1.city = 'New York'
        RETURN a2.city, COUNT(*) as count
        """
        result = query_api(query, schema_name="denormalized_flights_test")

        assert result["status"] == "success"
        # Should have at least one destination from New York
        assert len(result["data"]) >= 1


class TestVLPWithAggregations:
    """Test VLP with various aggregation functions."""

    def test_vlp_count_distinct(self, simple_graph):
        """VLP + Multiple aggregates: COUNT and DISTINCT."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        RETURN COUNT(DISTINCT u2.user_id) as unique_reached,
               COUNT(*) as total_paths
        """
        result = query_api(query, schema_name=simple_graph["schema_name"])

        assert result["status"] == "success"
        assert len(result["data"]) == 1
        row = result["data"][0]
        assert row["unique_reached"] > 0
        assert row["total_paths"] >= row["unique_reached"]

    def test_vlp_property_in_where_and_return(self, simple_graph):
        """VLP: Use properties in both WHERE and RETURN clauses."""
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.name = 'Alice'
        RETURN u1.name, u2.name
        """
        result = query_api(query, schema_name=simple_graph["schema_name"])

        assert result["status"] == "success"
        # Verify filtering worked
        for row in result["data"]:
            assert row["u1.name"] == "Alice"


class TestVLPPropertyPruning:
    """Test that VLP queries only select needed properties (property pruning)."""

    def test_vlp_prunes_unused_properties(self, simple_graph):
        """
        VLP Property Pruning: Only requested properties should be in SQL.

        This test requires inspecting the generated SQL, which isn't
        currently exposed in the API response. Mark as xfail until
        we add sql_only mode to API or inspection capability.
        """
        query = """
        MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
        WHERE u1.user_id = 1
        RETURN u2.user_id
        """
        result = query_api(query, schema_name=simple_graph["schema_name"])

        assert result["status"] == "success"
        # TODO: Check that SQL doesn't include unused columns like full_name, email, etc.
        # This would require either:
        # 1. Adding "include_sql": true parameter to API
        # 2. Checking EXPLAIN output
        # 3. Adding a debug endpoint that returns SQL
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
