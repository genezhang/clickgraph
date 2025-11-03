"""
Integration tests for aggregation functions and GROUP BY.

Tests cover:
- COUNT, SUM, AVG, MIN, MAX
- GROUP BY with single and multiple keys
- HAVING clause
- Aggregations with ORDER BY
- Aggregations with LIMIT
- Aggregations on relationships
- DISTINCT in aggregations
- Complex aggregation queries
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    assert_contains_value,
    get_single_value,
    get_column_values
)


class TestBasicAggregations:
    """Test basic aggregation functions."""
    
    def test_count_all_nodes(self, simple_graph):
        """Test COUNT(*) on all nodes."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(*) as total_users
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert get_single_value(response, "total_users", convert_to_int=True) == 5
    
    def test_count_distinct_nodes(self, simple_graph):
        """Test COUNT(DISTINCT) on nodes."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN COUNT(DISTINCT a) as unique_followers
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        # Alice, Bob, Charlie, Diana follow someone
        assert get_single_value(response, "unique_followers", convert_to_int=True) >= 3
    
    def test_sum_aggregation(self, simple_graph):
        """Test SUM aggregation on property."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN SUM(n.age) as total_age
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "total_age")
    
    def test_avg_aggregation(self, simple_graph):
        """Test AVG aggregation on property."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN AVG(n.age) as average_age
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["average_age"] > 0
        else:
            col_idx = response["columns"].index("average_age")
            assert results[0][col_idx] > 0
    
    def test_min_max_aggregation(self, simple_graph):
        """Test MIN and MAX aggregations."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN MIN(n.age) as youngest, MAX(n.age) as oldest
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "youngest")
        assert_column_exists(response, "oldest")
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["youngest"] <= results[0]["oldest"]
        else:
            col_idx_min = response["columns"].index("youngest")
            col_idx_max = response["columns"].index("oldest")
            assert results[0][col_idx_min] <= results[0][col_idx_max]


class TestGroupBy:
    """Test GROUP BY functionality."""
    
    def test_group_by_single_key(self, simple_graph):
        """Test GROUP BY with single key."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follows_count
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Each user who follows someone
        assert isinstance(response["results"], list)
        assert_column_exists(response, "a.name")
        assert_column_exists(response, "follows_count")
    
    def test_group_by_with_aggregation(self, simple_graph):
        """Test GROUP BY with multiple aggregations."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follow_count, AVG(b.age) as avg_age
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "a.name")
        assert_column_exists(response, "follow_count")
        assert_column_exists(response, "avg_age")
    
    def test_group_by_multiple_keys(self, simple_graph):
        """Test GROUP BY with multiple grouping keys."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, b.name, COUNT(*) as connection_count
            ORDER BY a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)
    
    def test_group_by_order_by(self, simple_graph):
        """Test GROUP BY with ORDER BY on aggregation."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follows
            ORDER BY follows DESC, a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Results should be ordered by follow count descending
        assert isinstance(response["results"], list)


class TestHavingClause:
    """Test HAVING clause with GROUP BY."""
    
    def test_having_count(self, simple_graph):
        """Test HAVING with COUNT condition."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WITH a, COUNT(b) as follows
            WHERE follows > 1
            RETURN a.name, follows
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Users who follow more than 1 person
        assert isinstance(response["results"], list)
    
    def test_having_avg(self, simple_graph):
        """Test HAVING with AVG condition."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WITH a, AVG(b.age) as avg_age
            WHERE avg_age > 25
            RETURN a.name, avg_age
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Users who follow people with avg age > 25
        assert isinstance(response["results"], list)
    
    def test_having_multiple_conditions(self, simple_graph):
        """Test HAVING with multiple conditions."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WITH a, COUNT(b) as follows, AVG(b.age) as avg_age
            WHERE follows > 0 AND avg_age > 20
            RETURN a.name, follows, avg_age
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)


class TestAggregationWithWhere:
    """Test aggregations combined with WHERE filters."""
    
    def test_where_before_aggregation(self, simple_graph):
        """Test WHERE clause before aggregation."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WHERE b.age > 25
            RETURN a.name, COUNT(b) as follows_older
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Count only follows to users older than 25
        assert isinstance(response["results"], list)
    
    def test_where_on_grouped_result(self, simple_graph):
        """Test WHERE after grouping (HAVING equivalent)."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WITH a, COUNT(b) as follow_count
            WHERE follow_count >= 1
            RETURN a.name, follow_count
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)
    
    def test_complex_filter_with_aggregation(self, simple_graph):
        """Test complex filtering with aggregation."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WHERE a.age > 25 AND b.age > 25
            RETURN a.name, COUNT(b) as mature_follows
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)


class TestAggregationWithLimit:
    """Test aggregations with LIMIT and SKIP."""
    
    def test_aggregation_with_limit(self, simple_graph):
        """Test GROUP BY with LIMIT."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follows
            ORDER BY follows DESC, a.name
            LIMIT 3
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert len(response["results"]) <= 3
    
    def test_aggregation_with_skip(self, simple_graph):
        """Test GROUP BY with SKIP."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follows
            ORDER BY a.name
            SKIP 1
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should skip first result
        assert isinstance(response["results"], list)
    
    def test_aggregation_with_limit_skip(self, simple_graph):
        """Test GROUP BY with both LIMIT and SKIP."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follows
            ORDER BY a.name
            SKIP 1
            LIMIT 2
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert len(response["results"]) <= 2


class TestRelationshipAggregations:
    """Test aggregations on relationships."""
    
    def test_count_relationships(self, simple_graph):
        """Test counting relationships."""
        response = execute_cypher(
            """
            MATCH ()-[r:FOLLOWS]->()
            RETURN COUNT(r) as total_follows
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        # Should have 6 FOLLOWS relationships
        assert get_single_value(response, "total_follows", convert_to_int=True) == 6
    
    def test_count_incoming_outgoing(self, simple_graph):
        """Test counting incoming and outgoing relationships."""
        response = execute_cypher(
            """
            MATCH (n:User)
            OPTIONAL MATCH (n)-[:FOLLOWS]->(out)
            OPTIONAL MATCH (in)-[:FOLLOWS]->(n)
            RETURN n.name, 
                   COUNT(DISTINCT out) as following,
                   COUNT(DISTINCT in) as followers
            ORDER BY n.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)  # All 5 users
        assert_column_exists(response, "following")
        assert_column_exists(response, "followers")
    
    def test_aggregate_relationship_properties(self, simple_graph):
        """Test aggregating relationship properties."""
        response = execute_cypher(
            """
            MATCH (a:User)-[r:FOLLOWS]->(b:User)
            RETURN MIN(r.since) as earliest, MAX(r.since) as latest
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "earliest")
        assert_column_exists(response, "latest")


class TestDistinctInAggregations:
    """Test DISTINCT within aggregation functions."""
    
    def test_count_distinct(self, simple_graph):
        """Test COUNT(DISTINCT) in aggregation."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)
            RETURN a.name, COUNT(DISTINCT b) as unique_reachable
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)
        assert_column_exists(response, "unique_reachable")
    
    def test_distinct_vs_non_distinct(self, simple_graph):
        """Test difference between COUNT and COUNT(DISTINCT)."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)
            WHERE a.name = 'Alice'
            RETURN COUNT(b) as total_paths, COUNT(DISTINCT b) as unique_nodes
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            # Total paths should be >= unique nodes
            assert results[0]["total_paths"] >= results[0]["unique_nodes"]
        else:
            col_idx_total = response["columns"].index("total_paths")
            col_idx_unique = response["columns"].index("unique_nodes")
            assert results[0][col_idx_total] >= results[0][col_idx_unique]


class TestComplexAggregations:
    """Test complex aggregation scenarios."""
    
    def test_nested_aggregations(self, simple_graph):
        """Test nested aggregation with WITH clause."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WITH a, COUNT(b) as follows
            RETURN AVG(follows) as avg_follows_per_user
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "avg_follows_per_user")
    
    def test_aggregation_with_case(self, simple_graph):
        """Test aggregation with CASE expression."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN 
                COUNT(CASE WHEN n.age < 30 THEN 1 END) as young,
                COUNT(CASE WHEN n.age >= 30 THEN 1 END) as mature
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "young")
        assert_column_exists(response, "mature")
    
    def test_multiple_aggregations_different_patterns(self, simple_graph):
        """Test multiple aggregations on different patterns."""
        response = execute_cypher(
            """
            MATCH (n:User)
            OPTIONAL MATCH (n)-[:FOLLOWS]->(out)
            OPTIONAL MATCH (in)-[:FOLLOWS]->(n)
            RETURN n.name,
                   COUNT(DISTINCT out) as following,
                   COUNT(DISTINCT in) as followers,
                   COUNT(DISTINCT out) + COUNT(DISTINCT in) as total_connections
            ORDER BY total_connections DESC, n.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_column_exists(response, "following")
        assert_column_exists(response, "followers")
        assert_column_exists(response, "total_connections")


class TestAggregationEdgeCases:
    """Test edge cases for aggregations."""
    
    def test_aggregation_empty_result(self, simple_graph):
        """Test aggregation on empty result set."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WHERE a.name = 'NonExistent'
            RETURN COUNT(b) as count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert get_single_value(response, "count", convert_to_int=True) == 0
    
    def test_aggregation_with_null_values(self, simple_graph):
        """Test aggregation handling NULL values."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follows
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Eve should have 0 follows
        assert_row_count(response, 5)
    
    def test_aggregation_all_same_group(self, simple_graph):
        """Test aggregation when all rows in same group."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN 'all_users' as group_key, COUNT(n) as total
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert get_single_value(response, "total", convert_to_int=True) == 5
