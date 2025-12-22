"""
Integration tests for path variables and path functions.

Tests cover:
- Path variable assignment: p = (a)-[r]->(b)
- length(p) function
- nodes(p) function  
- relationships(p) function
- Path variables with variable-length patterns
- Path variables with shortest paths
- Path properties and filtering
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    assert_contains_value
)


class TestPathVariableAssignment:
    """Test basic path variable assignment."""
    
    def test_path_variable_simple(self, simple_graph):
        """Test assigning path to variable."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN a.name, b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice follows Bob and Charlie
        assert_row_count(response, 2)
    
    def test_path_variable_multi_hop(self, simple_graph):
        """Test path variable with multi-hop pattern."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)-[:TEST_FOLLOWS]->(c:TestUser)
            WHERE a.name = 'Alice'
            RETURN a.name, c.name
            ORDER BY c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # 2-hop paths from Alice
        assert isinstance(response["results"], list)
    
    def test_path_variable_variable_length(self, simple_graph):
        """Test path variable with variable-length pattern."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*1..2]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN a.name, b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # All nodes reachable within 2 hops
        assert isinstance(response["results"], list)


class TestLengthFunction:
    """Test length(p) function on paths."""
    
    def test_length_single_hop(self, simple_graph):
        """Test length() on single-hop path."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN a.name, b.name, length(p) as path_length
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "path_length")
        # All should have length 1
        results = response["results"]
        for row in results:
            if isinstance(row, dict):
                assert row["path_length"] == 1
            else:
                col_idx = response["columns"].index("path_length")
                assert row[col_idx] == 1
    
    def test_length_multi_hop(self, simple_graph):
        """Test length() on multi-hop paths."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)-[:TEST_FOLLOWS]->(c:TestUser)
            WHERE a.name = 'Alice'
            RETURN a.name, c.name, length(p) as path_length
            ORDER BY c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # All should have length 2
        results = response["results"]
        for row in results:
            if isinstance(row, dict):
                assert row["path_length"] == 2
            else:
                col_idx = response["columns"].index("path_length")
                assert row[col_idx] == 2
    
    @pytest.mark.xfail(reason="Path functions with VLP have SQL generation issues - KNOWN_ISSUES")
    def test_length_variable_length_paths(self, simple_graph):
        """Test length() on variable-length paths."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*1..3]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT length(p) as path_length
            ORDER BY path_length
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should have paths of various lengths (1, 2, possibly 3)
        assert isinstance(response["results"], list)
    
    @pytest.mark.xfail(reason="Path functions with VLP have SQL generation issues - KNOWN_ISSUES")
    def test_length_filter(self, simple_graph):
        """Test filtering paths by length."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser)
            WHERE a.name = 'Alice' AND length(p) = 2
            RETURN a.name, b.name, length(p) as path_length
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Only 2-hop paths
        results = response["results"]
        for row in results:
            if isinstance(row, dict):
                assert row["path_length"] == 2
            else:
                col_idx = response["columns"].index("path_length")
                assert row[col_idx] == 2
    
    @pytest.mark.xfail(reason="Path functions with VLP have SQL generation issues - KNOWN_ISSUES")
    def test_length_in_aggregation(self, simple_graph):
        """Test aggregating path lengths."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN MIN(length(p)) as min_length, 
                   MAX(length(p)) as max_length,
                   AVG(length(p)) as avg_length
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "min_length")
        assert_column_exists(response, "max_length")
        assert_column_exists(response, "avg_length")


class TestNodesFunction:
    """Test nodes(p) function on paths."""
    
    def test_nodes_simple_path(self, simple_graph):
        """Test nodes() returning all nodes in path."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            WHERE a.name = 'Alice' AND b.name = 'Bob'
            RETURN nodes(p) as path_nodes
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        # Should return array [Alice, Bob]
        assert_column_exists(response, "path_nodes")
    
    def test_nodes_count(self, simple_graph):
        """Test counting nodes in path."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)-[:TEST_FOLLOWS]->(c:TestUser)
            WHERE a.name = 'Alice'
            RETURN length(nodes(p)) as node_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # 2-hop path should have 3 nodes
        results = response["results"]
        for row in results:
            if isinstance(row, dict):
                assert row["node_count"] == 3
            else:
                col_idx = response["columns"].index("node_count")
                assert row[col_idx] == 3
    
    @pytest.mark.xfail(reason="nodes() with VLP has SQL generation issues - KNOWN_ISSUES")
    def test_nodes_with_variable_length(self, simple_graph):
        """Test nodes() on variable-length paths."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*2]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN a.name, b.name, length(nodes(p)) as node_count
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # All 2-hop paths should have 3 nodes
        results = response["results"]
        for row in results:
            if isinstance(row, dict):
                assert row["node_count"] == 3
            else:
                col_idx = response["columns"].index("node_count")
                assert row[col_idx] == 3


class TestRelationshipsFunction:
    """Test relationships(p) function on paths."""
    
    @pytest.mark.xfail(reason="relationships() has SQL generation issues - KNOWN_ISSUES")
    def test_relationships_simple_path(self, simple_graph):
        """Test relationships() returning all relationships in path."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            WHERE a.name = 'Alice' AND b.name = 'Bob'
            RETURN relationships(p) as path_rels
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        # Should return array with one relationship
        assert_column_exists(response, "path_rels")
    
    @pytest.mark.xfail(reason="relationships() has SQL generation issues - KNOWN_ISSUES")
    def test_relationships_count(self, simple_graph):
        """Test counting relationships in path."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*2]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN a.name, b.name, length(relationships(p)) as rel_count
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # 2-hop paths should have 2 relationships
        results = response["results"]
        for row in results:
            if isinstance(row, dict):
                assert row["rel_count"] == 2
            else:
                col_idx = response["columns"].index("rel_count")
                assert row[col_idx] == 2
    
    @pytest.mark.xfail(reason="relationships() with VLP has SQL generation issues - KNOWN_ISSUES")
    def test_relationships_equals_length(self, simple_graph):
        """Test that length(relationships(p)) equals length(p)."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*1..3]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN length(p) as path_length, 
                   length(relationships(p)) as rel_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # path_length should always equal rel_count
        results = response["results"]
        for row in results:
            if isinstance(row, dict):
                assert row["path_length"] == row["rel_count"]
            else:
                len_idx = response["columns"].index("path_length")
                rel_idx = response["columns"].index("rel_count")
                assert row[len_idx] == row[rel_idx]


class TestPathWithShortestPath:
    """Test path variables with shortest path functions."""
    
    @pytest.mark.xfail(reason="Path functions with shortest path have SQL generation issues - KNOWN_ISSUES")
    def test_path_variable_shortest_path(self, simple_graph):
        """Test path variable with shortestPath()."""
        response = execute_cypher(
            """
            MATCH p = shortestPath((a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser))
            WHERE a.name = 'Alice' AND b.name = 'Diana'
            RETURN a.name, b.name, length(p) as path_length
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        # Shortest path from Alice to Diana is 2 hops
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["path_length"] == 2
        else:
            col_idx = response["columns"].index("path_length")
            assert results[0][col_idx] == 2
    
    @pytest.mark.xfail(reason="Path functions with shortest path have SQL generation issues - KNOWN_ISSUES")
    def test_shortest_path_length_comparison(self, simple_graph):
        """Test comparing shortest path lengths."""
        response = execute_cypher(
            """
            MATCH p = shortestPath((a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser))
            WHERE a.name = 'Alice'
            RETURN b.name, length(p) as distance
            ORDER BY distance, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should be ordered by distance
        assert isinstance(response["results"], list)
    
    @pytest.mark.xfail(reason="Path functions with shortest path have SQL generation issues - KNOWN_ISSUES")
    def test_all_shortest_paths_length(self, simple_graph):
        """Test path lengths with allShortestPaths()."""
        response = execute_cypher(
            """
            MATCH p = allShortestPaths((a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser))
            WHERE a.name = 'Alice' AND b.name = 'Diana'
            RETURN COUNT(*) as path_count, MIN(length(p)) as min_length
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # All shortest paths should have same length
        assert_row_count(response, 1)


class TestPathFunctionsInWhere:
    """Test path functions in WHERE clause."""
    
    @pytest.mark.xfail(reason="Path functions with VLP have SQL generation issues - KNOWN_ISSUES")
    def test_filter_by_path_length(self, simple_graph):
        """Test filtering paths by length in WHERE."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser)
            WHERE a.name = 'Alice' AND length(p) >= 2
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Nodes reachable in 2+ hops
        assert isinstance(response["results"], list)
    
    @pytest.mark.xfail(reason="Path functions with VLP have SQL generation issues - KNOWN_ISSUES")
    def test_filter_by_node_count(self, simple_graph):
        """Test filtering by node count in path."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser)
            WHERE a.name = 'Alice' AND length(nodes(p)) = 3
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Paths with exactly 3 nodes (2-hop paths)
        assert isinstance(response["results"], list)


class TestPathFunctionsInReturn:
    """Test path functions in RETURN clause."""
    
    @pytest.mark.xfail(reason="Path functions with VLP have SQL generation issues - KNOWN_ISSUES")
    def test_return_all_path_functions(self, simple_graph):
        """Test returning multiple path functions."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*1..2]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT
                length(p) as path_length,
                length(nodes(p)) as node_count,
                length(relationships(p)) as rel_count
            ORDER BY path_length
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "path_length")
        assert_column_exists(response, "node_count")
        assert_column_exists(response, "rel_count")
    
    @pytest.mark.xfail(reason="Path functions with VLP have SQL generation issues - KNOWN_ISSUES")
    def test_path_functions_with_aggregation(self, simple_graph):
        """Test path functions in aggregation."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN 
                AVG(length(p)) as avg_path_length,
                MIN(length(p)) as min_path_length,
                MAX(length(p)) as max_path_length
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "avg_path_length")
        assert_column_exists(response, "min_path_length")
        assert_column_exists(response, "max_path_length")


class TestPathEdgeCases:
    """Test edge cases for path variables and functions."""
    
    @pytest.mark.xfail(reason="Zero-length VLP (*0) has SQL generation issues - KNOWN_ISSUES")
    def test_zero_length_path(self, simple_graph):
        """Test path of length zero."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*0]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN a.name, b.name, length(p) as path_length
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Zero-length path to self
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["path_length"] == 0
            assert results[0]["a.name"] == results[0]["b.name"]
        else:
            len_idx = response["columns"].index("path_length")
            a_idx = response["columns"].index("a.name")
            b_idx = response["columns"].index("b.name")
            assert results[0][len_idx] == 0
            assert results[0][a_idx] == results[0][b_idx]
    
    @pytest.mark.xfail(reason="Path functions with VLP have SQL generation issues - KNOWN_ISSUES")
    def test_path_no_results(self, simple_graph):
        """Test path functions when no paths exist."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser)
            WHERE a.name = 'Eve' AND b.name = 'Alice'
            RETURN length(p) as path_length
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # No paths from Eve to Alice
        assert_row_count(response, 0)
    
    def test_path_undirected(self, simple_graph):
        """Test path functions on undirected patterns."""
        response = execute_cypher(
            """
            MATCH p = (a:TestUser)-[:TEST_FOLLOWS]-(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN b.name, length(p) as path_length
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Undirected single-hop paths
        results = response["results"]
        for row in results:
            if isinstance(row, dict):
                assert row["path_length"] == 1
            else:
                col_idx = response["columns"].index("path_length")
                assert row[col_idx] == 1
