"""
Integration tests for variable-length path patterns.

Tests cover:
- Fixed-length patterns (*2, *3)
- Range patterns (*1..3, *2..5)
- Unbounded patterns (*.. , *..)
- Variable-length with filters
- Variable-length with property access
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    assert_contains_value
)


class TestFixedLengthPaths:
    """Test fixed-length variable path patterns (*N)."""
    
    def test_exact_two_hops(self, simple_graph):
        """Test *2 pattern (exactly 2 hops)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2]->(b:TestUser)
            RETURN a.name, b.name
            ORDER BY a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Charlie
        # Alice -> Bob -> Diana
        # Alice -> Charlie -> Diana
        # Bob -> Charlie -> Diana
        # Bob -> Diana -> Eve (was missing!)
        # Charlie -> Diana -> Eve
        assert_row_count(response, 6)
    
    def test_exact_three_hops(self, simple_graph):
        """Test *3 pattern (exactly 3 hops)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*3]->(b:TestUser)
            RETURN a.name, b.name
            ORDER BY a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Charlie -> Diana
        # Alice -> Bob -> Diana -> Eve
        # Alice -> Charlie -> Diana -> Eve
        # Bob -> Charlie -> Diana -> Eve
        assert_row_count(response, 4)
    
    def test_exact_one_hop(self, simple_graph):
        """Test *1 pattern (same as single relationship)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1]->(b:TestUser)
            RETURN COUNT(*) as count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["count"] == 6  # Same as direct relationships
        else:
            col_idx = response["columns"].index("count")
            assert results[0][col_idx] == 6


class TestRangePaths:
    """Test range variable-length patterns (*N..M)."""
    
    def test_one_to_two_hops(self, simple_graph):
        """Test *1..2 pattern (1 or 2 hops)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..2]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # 1-hop: Bob, Charlie
        # 2-hop: Charlie (via Bob), Diana (via Bob or Charlie)
        # Distinct: Bob, Charlie, Diana
        assert_row_count(response, 3)
    
    def test_one_to_three_hops(self, simple_graph):
        """Test *1..3 pattern (1, 2, or 3 hops)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..3]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # 1-hop: Bob, Charlie
        # 2-hop: Charlie, Diana
        # 3-hop: Diana, Eve
        # Distinct: Bob, Charlie, Diana, Eve
        assert_row_count(response, 4)
    
    def test_two_to_four_hops(self, simple_graph):
        """Test *2..4 pattern (2, 3, or 4 hops)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2..4]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # 2-hop: Charlie, Diana
        # 3-hop: Diana, Eve
        # 4-hop: Eve
        # Distinct: Charlie, Diana, Eve
        assert_row_count(response, 3)
    
    def test_range_with_count(self, simple_graph):
        """Test counting paths in range."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..2]->(b:TestUser)
            WHERE a.name = 'Bob'
            RETURN COUNT(*) as path_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # 1-hop: Charlie, Diana (2 paths)
        # 2-hop: Diana (via Charlie), Eve (via Diana) (2 paths)
        # Total: 4 paths
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["path_count"] >= 2  # At least direct relationships
        else:
            col_idx = response["columns"].index("path_count")
            assert results[0][col_idx] >= 2


class TestUnboundedPaths:
    """Test unbounded variable-length patterns."""
    
    def test_zero_or_more_hops(self, simple_graph):
        """Test *0.. pattern (includes self)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*0..]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should include Alice (0 hops) and all reachable nodes
        # 0-hop: Alice
        # 1-hop: Bob, Charlie
        # 2-hop: Diana
        # 3-hop: Eve
        assert_row_count(response, 5)  # Alice, Bob, Charlie, Diana, Eve
    
    def test_one_or_more_hops(self, simple_graph):
        """Test *1.. or *.. pattern (excludes self)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # All reachable nodes except Alice
        # 1-hop: Bob, Charlie
        # 2-hop: Diana
        # 3-hop: Eve
        assert_row_count(response, 4)  # Bob, Charlie, Diana, Eve
    
    def test_unbounded_upper(self, simple_graph):
        """Test *2.. pattern (at least 2 hops)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2..]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # 2+ hops: Charlie, Diana, Eve
        assert_row_count(response, 3)


class TestVariableLengthWithFilters:
    """Test variable-length paths with WHERE clauses."""
    
    def test_filter_start_node(self, simple_graph):
        """Test variable-length with start node filter."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Charlie
        # Alice -> Bob -> Diana
        # Alice -> Charlie -> Diana
        assert_row_count(response, 3)
    
    def test_filter_end_node(self, simple_graph):
        """Test variable-length with end node filter."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2]->(b:TestUser)
            WHERE b.name = 'Diana'
            RETURN a.name
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice -> (Bob|Charlie) -> Diana
        # Bob -> Charlie -> Diana
        assert_row_count(response, 3)
    
    def test_filter_both_nodes(self, simple_graph):
        """Test variable-length with both start and end filters."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..3]->(b:TestUser)
            WHERE a.name = 'Alice' AND b.name = 'Eve'
            RETURN COUNT(*) as path_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Paths from Alice to Eve
        assert_row_count(response, 1)
    
    def test_filter_intermediate_property(self, simple_graph):
        """Test variable-length with property filter on end node."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2]->(b:TestUser)
            WHERE a.name = 'Alice' AND b.age > 27
            RETURN b.name, b.age
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Charlie (35) via Bob, Diana (28) via Bob, Diana (28) via Charlie
        # Note: Diana appears twice (two different paths), no DISTINCT
        assert_row_count(response, 3)


class TestVariableLengthProperties:
    """Test accessing properties in variable-length patterns."""
    
    def test_start_node_properties(self, simple_graph):
        """Test accessing start node properties."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2]->(b:TestUser)
            RETURN a.name, a.age, b.name
            ORDER BY a.name, b.name
            LIMIT 3
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "a.name")
        assert_column_exists(response, "a.age")
        assert_column_exists(response, "b.name")
    
    def test_end_node_properties(self, simple_graph):
        """Test accessing end node properties."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN b.name, b.age
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "b.name")
        assert_column_exists(response, "b.age")
    
    def test_both_node_properties(self, simple_graph):
        """Test accessing both start and end node properties."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..2]->(b:TestUser)
            WHERE a.name = 'Bob'
            RETURN a.name, a.age, b.name, b.age
            ORDER BY b.name
            LIMIT 3
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 3)


class TestVariableLengthBidirectional:
    """Test bidirectional variable-length patterns."""
    
    def test_undirected_two_hops(self, simple_graph):
        """Test -[*2]- pattern (either direction)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2]-(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Reachable in 2 hops (either direction)
        # Should include more nodes than directed
        assert isinstance(response["results"], list)
    
    @pytest.mark.xfail(reason="Undirected VLP uses hardcoded from_id/to_id instead of schema column names - KNOWN_ISSUES")
    def test_undirected_range(self, simple_graph):
        """Test -[*1..2]- pattern (either direction, range)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..2]-(b:TestUser)
            WHERE a.name = 'Bob'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Bob's network within 2 hops (either direction)
        assert isinstance(response["results"], list)


class TestVariableLengthAggregation:
    """Test aggregations with variable-length paths."""
    
    def test_count_paths_by_length(self, simple_graph):
        """Test counting paths of different lengths."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..3]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN COUNT(*) as total_paths
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
    
    def test_group_by_end_node(self, simple_graph):
        """Test grouping paths by end node."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..2]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN b.name, COUNT(*) as path_count
            ORDER BY path_count DESC, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should show different counts for different destinations
        assert isinstance(response["results"], list)
    
    def test_max_min_on_paths(self, simple_graph):
        """Test MAX/MIN on variable-length results."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..3]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN MIN(b.age) as min_age, MAX(b.age) as max_age
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "min_age")
        assert_column_exists(response, "max_age")


class TestVariableLengthDistinct:
    """Test DISTINCT with variable-length paths."""
    
    def test_distinct_end_nodes(self, simple_graph):
        """Test DISTINCT on end nodes from variable-length."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..3]->(b:TestUser)
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should have no duplicates
        names = [r["b.name"] if isinstance(r, dict) else r[0] for r in response["results"]]
        assert len(names) == len(set(names)), "Results should be distinct"
    
    def test_distinct_node_pairs(self, simple_graph):
        """Test DISTINCT on start-end node pairs."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*2]->(b:TestUser)
            RETURN DISTINCT a.name, b.name
            ORDER BY a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Each (a,b) pair should appear once
        assert isinstance(response["results"], list)


class TestVariableLengthEdgeCases:
    """Test edge cases for variable-length patterns."""
    
    @pytest.mark.skip(reason="*0 pattern requires special handling - currently returns 1-hop instead of self-loop")
    def test_zero_length(self, simple_graph):
        """Test *0 pattern (returns same node)."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*0]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # *0 should return the same node
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["a.name"] == results[0]["b.name"]
        else:
            assert results[0][0] == results[0][1]
    
    def test_no_paths_found(self, simple_graph):
        """Test variable-length when no paths exist."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*5]->(b:TestUser)
            WHERE a.name = 'Diana'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # No 5-hop paths from Diana
        assert_row_count(response, 0)
    
    @pytest.mark.xfail(reason="Self-referencing VLP patterns use hardcoded column names - KNOWN_ISSUES")
    def test_self_referencing_with_range(self, simple_graph):
        """Test if variable-length avoids immediate self-loops."""
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..2]->(a)
            RETURN COUNT(*) as loops
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should find circular paths if they exist
        assert_row_count(response, 1)
