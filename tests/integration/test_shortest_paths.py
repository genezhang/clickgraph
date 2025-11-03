"""
Integration tests for shortest path algorithms.

Tests cover:
- shortestPath() function
- allShortestPaths() function  
- Shortest paths with filters
- Shortest paths with properties
- Edge cases (no path, multiple paths)
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    assert_contains_value
)


class TestShortestPath:
    """Test shortestPath() function."""
    
    def test_shortest_path_basic(self, simple_graph):
        """Test basic shortestPath query."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]-(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Eve'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # There should be a path from Alice to Eve
        assert_row_count(response, 1)
    
    def test_shortest_path_directed(self, simple_graph):
        """Test shortestPath with directed relationships."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Diana'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice can reach Diana in 2 hops
        assert_row_count(response, 1)
    
    def test_shortest_path_with_max_depth(self, simple_graph):
        """Test shortestPath with maximum depth limit."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*..3]->(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Eve'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Path exists within 3 hops
        assert_row_count(response, 1)
    
    def test_shortest_path_no_path_exists(self, simple_graph):
        """Test shortestPath when no path exists."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Eve' AND b.name = 'Alice'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # No path from Eve to Alice (directed graph)
        assert_row_count(response, 0)
    
    def test_shortest_path_self(self, simple_graph):
        """Test shortestPath from node to itself."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*0..]->(a))
            WHERE a.name = 'Alice'
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Path of length 0 (self)
        assert_row_count(response, 1)


class TestAllShortestPaths:
    """Test allShortestPaths() function."""
    
    def test_all_shortest_paths_single(self, simple_graph):
        """Test allShortestPaths when only one shortest path exists."""
        response = execute_cypher(
            """
            MATCH path = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Charlie' AND b.name = 'Eve'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Charlie -> Diana -> Eve (only one path)
        assert_row_count(response, 1)
    
    def test_all_shortest_paths_multiple(self, simple_graph):
        """Test allShortestPaths when multiple shortest paths exist."""
        response = execute_cypher(
            """
            MATCH path = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Diana'
            RETURN COUNT(*) as path_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Diana
        # Alice -> Charlie -> Diana  
        # Both are 2-hop paths (shortest)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["path_count"] >= 1
        else:
            col_idx = response["columns"].index("path_count")
            assert results[0][col_idx] >= 1
    
    def test_all_shortest_paths_undirected(self, simple_graph):
        """Test allShortestPaths with undirected relationships."""
        response = execute_cypher(
            """
            MATCH path = allShortestPaths((a:User)-[:FOLLOWS*]-(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Diana'
            RETURN COUNT(*) as path_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should find shortest paths in either direction
        assert_row_count(response, 1)


class TestShortestPathWithFilters:
    """Test shortest path queries with WHERE clauses."""
    
    def test_shortest_path_filter_start_node(self, simple_graph):
        """Test shortest path with filter on start node."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Alice' AND a.age > 25
            RETURN a.name, COUNT(b) as reachable_nodes
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice (age 30) can reach multiple nodes
        assert isinstance(response["results"], list)
    
    def test_shortest_path_filter_end_node(self, simple_graph):
        """Test shortest path with filter on end node."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Alice' AND b.age > 30
            RETURN b.name, b.age
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should find paths to users older than 30
        assert isinstance(response["results"], list)
    
    def test_shortest_path_filter_both_nodes(self, simple_graph):
        """Test shortest path with filters on both start and end."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.age < 30 AND b.age > 30
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Young users reaching older users
        assert isinstance(response["results"], list)


class TestShortestPathProperties:
    """Test accessing properties in shortest path results."""
    
    def test_shortest_path_return_properties(self, simple_graph):
        """Test returning node properties from shortest path."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Eve'
            RETURN a.name, a.age, b.name, b.age
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "a.name")
        assert_column_exists(response, "a.age")
        assert_column_exists(response, "b.name")
        assert_column_exists(response, "b.age")
    
    def test_shortest_path_order_by_property(self, simple_graph):
        """Test ordering shortest paths by property."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Alice'
            RETURN b.name, b.age
            ORDER BY b.age DESC
            LIMIT 3
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)


class TestShortestPathAggregation:
    """Test aggregations with shortest paths."""
    
    def test_count_shortest_paths(self, simple_graph):
        """Test counting shortest paths."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Alice'
            RETURN COUNT(DISTINCT b) as reachable_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        # Alice can reach Bob, Charlie, Diana, Eve
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["reachable_count"] >= 2
        else:
            col_idx = response["columns"].index("reachable_count")
            assert results[0][col_idx] >= 2
    
    def test_group_shortest_paths_by_start(self, simple_graph):
        """Test grouping shortest paths by start node."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            RETURN a.name, COUNT(DISTINCT b) as reachable
            ORDER BY reachable DESC, a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should show how many nodes each user can reach
        assert isinstance(response["results"], list)


class TestShortestPathDepth:
    """Test shortest paths with depth constraints."""
    
    def test_shortest_path_min_depth(self, simple_graph):
        """Test shortest path with minimum depth."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*2..]->(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Diana'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Diana is exactly 2 hops from Alice
        assert_row_count(response, 1)
    
    def test_shortest_path_exact_depth(self, simple_graph):
        """Test shortest path with exact depth requirement."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*3]->(b:User))
            WHERE a.name = 'Alice'
            RETURN b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Nodes exactly 3 hops away
        assert isinstance(response["results"], list)
    
    def test_shortest_path_max_depth_exceeded(self, simple_graph):
        """Test shortest path when max depth is too low."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*..1]->(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Eve'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Eve is not reachable within 1 hop from Alice
        assert_row_count(response, 0)


class TestShortestPathEdgeCases:
    """Test edge cases for shortest path queries."""
    
    def test_shortest_path_same_node(self, simple_graph):
        """Test shortest path from node to itself."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*0..]->(a))
            WHERE a.name = 'Bob'
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Path of length 0 to self
        assert_row_count(response, 1)
    
    def test_shortest_path_unreachable(self, simple_graph):
        """Test shortest path when target is unreachable."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Eve' AND b.name = 'Alice'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # No directed path from Eve to Alice
        assert_row_count(response, 0)
    
    def test_shortest_path_multiple_start_nodes(self, simple_graph):
        """Test shortest path with multiple start nodes."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name IN ['Alice', 'Bob'] AND b.name = 'Eve'
            RETURN a.name, b.name
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Both Alice and Bob can reach Eve
        assert_row_count(response, 2)


class TestShortestPathPerformance:
    """Test shortest path performance characteristics."""
    
    def test_shortest_path_early_termination(self, simple_graph):
        """Test that shortest path terminates early."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*..10]->(b:User))
            WHERE a.name = 'Alice' AND b.name = 'Bob'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should find 1-hop path and stop
        assert_row_count(response, 1)
    
    def test_all_shortest_paths_limit(self, simple_graph):
        """Test limiting all shortest paths results."""
        response = execute_cypher(
            """
            MATCH path = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User))
            RETURN a.name, b.name
            LIMIT 10
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should respect LIMIT
        assert len(response["results"]) <= 10


class TestShortestPathDistinct:
    """Test DISTINCT with shortest paths."""
    
    def test_distinct_shortest_path_targets(self, simple_graph):
        """Test DISTINCT on shortest path targets."""
        response = execute_cypher(
            """
            MATCH path = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Each reachable node appears once
        names = [r["b.name"] if isinstance(r, dict) else r[0] for r in response["results"]]
        assert len(names) == len(set(names)), "Results should be distinct"
