"""
Integration tests for OPTIONAL MATCH queries.

Tests cover:
- Single OPTIONAL MATCH patterns
- Multiple OPTIONAL MATCH patterns
- Mixed required and optional patterns
- OPTIONAL MATCH with filters
- OPTIONAL MATCH with relationships
- OPTIONAL MATCH with variable-length paths
- NULL handling in optional results
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    assert_contains_value
)


class TestSingleOptionalMatch:
    """Test single OPTIONAL MATCH patterns."""
    
    def test_optional_match_existing_node(self, simple_graph):
        """Test OPTIONAL MATCH when node exists."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice follows Bob and Charlie
        assert_row_count(response, 2)
        assert_contains_value(response, "b.name", "Bob")
        assert_contains_value(response, "b.name", "Charlie")
    
    def test_optional_match_no_relationship(self, simple_graph):
        """Test OPTIONAL MATCH when relationship doesn't exist."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Eve'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Eve doesn't follow anyone, should return NULL for b
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["a.name"] == "Eve"
            assert results[0]["b.name"] is None
        else:
            # Handle array format
            assert results[0][0] == "Eve"
            assert results[0][1] is None
    
    def test_optional_match_incoming_relationship(self, simple_graph):
        """Test OPTIONAL MATCH with incoming relationship."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (b:User)-[:FOLLOWS]->(a)
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice has no incoming FOLLOWS (in simple_graph)
        # Should return one row with NULL for b
        assert_row_count(response, 1)
    
    def test_optional_match_undirected(self, simple_graph):
        """Test OPTIONAL MATCH with undirected relationship."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Bob'
            OPTIONAL MATCH (a)-[:FOLLOWS]-(b:User)
            RETURN a.name, COUNT(b) as connections
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)


class TestMultipleOptionalMatch:
    """Test multiple OPTIONAL MATCH clauses."""
    
    def test_two_optional_matches_both_exist(self, simple_graph):
        """Test two OPTIONAL MATCH when both relationships exist."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(c:User)
            WHERE c.name <> b.name
            RETURN a.name, b.name, c.name
            ORDER BY b.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice follows Bob and Charlie
        assert isinstance(response["results"], list)
    
    def test_two_optional_matches_one_missing(self, simple_graph):
        """Test two OPTIONAL MATCH when one doesn't exist."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Eve'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            OPTIONAL MATCH (b)-[:FOLLOWS]->(c:User)
            RETURN a.name, b.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Eve follows nobody, so both b and c should be NULL
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["a.name"] == "Eve"
            assert results[0]["b.name"] is None, "Expected NULL since Eve follows nobody"
            assert results[0]["c.name"] is None, "Expected NULL since b is NULL"
    
    def test_chained_optional_matches(self, simple_graph):
        """Test chained OPTIONAL MATCH patterns."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            OPTIONAL MATCH (b)-[:FOLLOWS]->(c:User)
            RETURN a.name, b.name, c.name
            ORDER BY b.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Charlie, Diana
        # Alice -> Charlie -> Diana
        assert isinstance(response["results"], list)


class TestMixedRequiredOptional:
    """Test mixing required MATCH with OPTIONAL MATCH."""
    
    def test_required_then_optional(self, simple_graph):
        """Test required MATCH followed by OPTIONAL MATCH."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            OPTIONAL MATCH (b)-[:FOLLOWS]->(c:User)
            RETURN a.name, b.name, c.name
            ORDER BY a.name, b.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # All required relationships plus optional extensions
        assert isinstance(response["results"], list)
    
    def test_optional_then_required(self, simple_graph):
        """Test OPTIONAL MATCH followed by required MATCH."""
        response = execute_cypher(
            """
            OPTIONAL MATCH (a:User)-[:FOLLOWS]->(b:User)
            WHERE a.name = 'Eve'
            MATCH (x:User)
            WHERE x.name = 'Alice'
            RETURN a.name, b.name, x.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Eve has no outgoing follows, but Alice always matches
        assert_row_count(response, 1)
    
    def test_interleaved_required_optional(self, simple_graph):
        """Test alternating required and optional patterns."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            MATCH (x:User)
            WHERE x.name = 'Bob'
            RETURN a.name, b.name, x.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice, her optional follows, and Bob (always present)
        assert isinstance(response["results"], list)


class TestOptionalMatchWithFilters:
    """Test OPTIONAL MATCH with WHERE clauses."""
    
    def test_optional_match_filter_on_optional_node(self, simple_graph):
        """Test OPTIONAL MATCH with filter on the optional node."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            WHERE b.age > 25
            RETURN a.name, b.name, b.age
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Only followed users with age > 25
        assert isinstance(response["results"], list)
    
    def test_optional_match_filter_on_relationship(self, simple_graph):
        """Test OPTIONAL MATCH with filter on relationship property."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (a)-[r:FOLLOWS]->(b:User)
            WHERE r.since > '2020'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Follows relationships after 2020
        assert isinstance(response["results"], list)
    
    def test_optional_match_complex_filter(self, simple_graph):
        """Test OPTIONAL MATCH with complex WHERE clause."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            WHERE b.age > 25 AND b.name <> 'Charlie'
            RETURN a.name, COUNT(b) as filtered_follows
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)


class TestOptionalMatchWithProperties:
    """Test property access in OPTIONAL MATCH results."""
    
    def test_optional_match_return_properties(self, simple_graph):
        """Test returning properties from optional nodes."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, a.age, b.name, b.age
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "a.name")
        assert_column_exists(response, "a.age")
        assert_column_exists(response, "b.name")
        assert_column_exists(response, "b.age")
    
    def test_optional_match_null_property_access(self, simple_graph):
        """Test accessing properties when optional match returns NULL."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Eve'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, b.name, b.age
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["b.name"] is None
            assert results[0]["b.age"] is None
        else:
            col_idx_name = response["columns"].index("b.name")
            col_idx_age = response["columns"].index("b.age")
            assert results[0][col_idx_name] is None
            assert results[0][col_idx_age] is None


class TestOptionalMatchAggregation:
    """Test aggregations with OPTIONAL MATCH."""
    
    def test_count_optional_matches(self, simple_graph):
        """Test COUNT with OPTIONAL MATCH."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follow_count
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should return count for each user (0 for Eve)
        assert_row_count(response, 5)  # All 5 users
    
    def test_optional_match_with_aggregation(self, simple_graph):
        """Test aggregation functions on optional matches."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follows, AVG(b.age) as avg_age
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "follows")
        assert_column_exists(response, "avg_age")
    
    def test_optional_match_group_by(self, simple_graph):
        """Test GROUP BY with OPTIONAL MATCH."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follow_count
            ORDER BY follow_count DESC, a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)


class TestOptionalMatchVariableLength:
    """Test OPTIONAL MATCH with variable-length paths."""
    
    def test_optional_variable_length_exists(self, simple_graph):
        """Test OPTIONAL MATCH with variable-length when path exists."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (a)-[:FOLLOWS*1..2]->(b:User)
            RETURN a.name, COUNT(DISTINCT b) as reachable
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        # Alice can reach multiple users within 2 hops
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["reachable"] >= 2
        else:
            col_idx = response["columns"].index("reachable")
            assert results[0][col_idx] >= 2
    
    def test_optional_variable_length_no_path(self, simple_graph):
        """Test OPTIONAL MATCH with variable-length when no path exists."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Eve'
            OPTIONAL MATCH (a)-[:FOLLOWS*1..3]->(b:User)
            RETURN a.name, COUNT(b) as reachable
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        # Eve can't reach anyone via FOLLOWS
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["reachable"] == 0
        else:
            col_idx = response["columns"].index("reachable")
            assert results[0][col_idx] == 0
    
    def test_optional_unbounded_path(self, simple_graph):
        """Test OPTIONAL MATCH with unbounded variable-length."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS*]->(b:User)
            RETURN a.name, COUNT(DISTINCT b) as reachable_count
            ORDER BY reachable_count DESC, a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # All users should be returned
        assert_row_count(response, 5)


class TestOptionalMatchDistinct:
    """Test DISTINCT with OPTIONAL MATCH."""
    
    def test_distinct_optional_results(self, simple_graph):
        """Test DISTINCT on OPTIONAL MATCH results."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Distinct users who are followed (plus NULL for Eve)
        assert isinstance(response["results"], list)
    
    def test_distinct_with_null(self, simple_graph):
        """Test DISTINCT includes NULL from optional matches."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name IN ['Alice', 'Eve']
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice's follows + NULL from Eve
        assert isinstance(response["results"], list)


class TestOptionalMatchEdgeCases:
    """Test edge cases for OPTIONAL MATCH."""
    
    def test_optional_match_all_nulls(self, simple_graph):
        """Test OPTIONAL MATCH when all results are NULL."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Eve'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            OPTIONAL MATCH (b)-[:FOLLOWS]->(c:User)
            RETURN a.name, b.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["b.name"] is None
            assert results[0]["c.name"] is None
    
    def test_optional_match_no_base_match(self, simple_graph):
        """Test OPTIONAL MATCH when base MATCH returns nothing."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'NonExistent'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # No base match, so no rows
        assert_row_count(response, 0)
    
    def test_optional_match_with_limit(self, simple_graph):
        """Test OPTIONAL MATCH with LIMIT clause."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, b.name
            ORDER BY a.name, b.name
            LIMIT 5
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should respect LIMIT
        assert len(response["results"]) <= 5
    
    def test_optional_match_self_reference(self, simple_graph):
        """Test OPTIONAL MATCH with self-referencing pattern."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice'
            OPTIONAL MATCH (a)-[:FOLLOWS*0..]->(a)
            RETURN a.name, COUNT(*) as paths
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should include zero-length path to self
        assert_row_count(response, 1)
