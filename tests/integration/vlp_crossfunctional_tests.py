"""
Cross-functional tests for VLP with other features.

Tests VLP interaction with:
- COLLECT/UNWIND
- Property pruning
- WITH clauses
- Complex aggregations
- Denormalized schemas
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
)


class TestVLPWithCollect:
    """Test VLP combined with COLLECT aggregation."""
    
    def test_vlp_collect_paths(self):
        """Collect all paths from VLP into array."""
        response = execute_cypher(
            """
            MATCH p = (u:TestUser)-[:TEST_FOLLOWS*1..2]->(friend:TestUser)
            WHERE u.user_id = 1
            RETURN u.full_name, collect(friend.full_name) as friend_names
            """
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        row = response['results'][0]
        assert 'friend_names' in row
        # Should have collected multiple friends
        assert isinstance(row['friend_names'], list)
        assert len(row['friend_names']) > 0
    
    def test_vlp_collect_with_unwind(self, simple_graph):
        """VLP + COLLECT + UNWIND pattern."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS*1..2]->(friend:User)
            WHERE u.user_id = 1
            WITH u, collect(friend) as friends
            UNWIND friends as f
            RETURN u.name, f.name
            ORDER BY f.name
            LIMIT 5
            """,
            schema_name="brahmand"
        )
        
        assert_query_success(response)
        assert 'results' in response
        # Should have unwound the collected friends
        assert len(response['results']) > 0


class TestVLPWithPropertyPruning:
    """Test VLP with property pruning optimization."""
    
    def test_vlp_select_only_needed_properties(self, simple_graph):
        """VLP should only materialize properties that are used."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS*1..2]->(friend:User)
            WHERE u.user_id = 1
            RETURN friend.name
            LIMIT 5
            """,
            schema_name="brahmand"
        )
        
        assert_query_success(response)
        # Only friend.name should be in results (not email, city, etc.)
        row = response['results'][0]
        assert 'friend.name' in row
        # Property pruning is working if SQL doesn't have unnecessary columns
    
    def test_vlp_property_in_where_and_return(self, simple_graph):
        """Properties used in WHERE and RETURN should both be pruned."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS*1..2]->(friend:User)
            WHERE u.user_id = 1 AND friend.country = 'USA'
            RETURN friend.name, friend.country
            LIMIT 5
            """,
            schema_name="brahmand"
        )
        
        assert_query_success(response)
        row = response['results'][0]
        assert 'friend.name' in row
        assert 'friend.country' in row


class TestVLPWithWITH:
    """Test VLP combined with WITH clause."""
    
    def test_vlp_then_with(self, simple_graph):
        """VLP followed by WITH clause."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS*1..2]->(friend:User)
            WHERE u.user_id = 1
            WITH friend, length(p) as hops
            WHERE hops = 2
            RETURN friend.name, hops
            ORDER BY friend.name
            LIMIT 5
            """,
            schema_name="brahmand"
        )
        
        assert_query_success(response)
        # All results should have hops = 2
        for row in response['results']:
            assert row['hops'] == 2
    
    def test_vlp_with_aggregation_then_filter(self, simple_graph):
        """VLP with aggregation in WITH, then filter."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS*1..2]->(friend:User)
            WHERE u.user_id = 1
            WITH friend.country as country, count(*) as friend_count
            WHERE friend_count > 1
            RETURN country, friend_count
            ORDER BY friend_count DESC
            """,
            schema_name="brahmand"
        )
        
        assert_query_success(response)
        # All results should have friend_count > 1
        for row in response['results']:
            assert row['friend_count'] > 1


class TestVLPDenormalizedCrossFunctional:
    """Test denormalized VLP with other features."""
    
    def test_denorm_vlp_with_collect(self, denormalized_flights_graph):
        """Denormalized VLP + COLLECT."""
        response = execute_cypher(
            """
            MATCH p = (origin:Airport)-[:FLIGHT*1..2]->(dest:Airport)
            WHERE origin.code = 'LAX'
            RETURN origin.city, collect(dest.city) as destinations
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        row = response['results'][0]
        assert row['origin.city'] == 'Los Angeles'
        assert isinstance(row['destinations'], list)
        assert len(row['destinations']) > 0
    
    def test_denorm_vlp_with_aggregation(self, denormalized_flights_graph):
        """Denormalized VLP + GROUP BY aggregation."""
        response = execute_cypher(
            """
            MATCH p = (origin:Airport)-[:FLIGHT*1..2]->(dest:Airport)
            WHERE origin.code = 'LAX'
            RETURN dest.state, count(*) as path_count
            ORDER BY path_count DESC
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should have paths to multiple states
        assert len(response['results']) > 0
        for row in response['results']:
            assert row['path_count'] > 0


class TestVLPComplexAggregations:
    """Test VLP with complex aggregation patterns."""
    
    def test_vlp_multiple_aggregates(self, simple_graph):
        """VLP with multiple aggregate functions."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS*1..2]->(friend:User)
            RETURN 
                u.name,
                count(DISTINCT friend) as unique_friends,
                count(*) as total_paths,
                collect(DISTINCT friend.country) as countries
            ORDER BY total_paths DESC
            LIMIT 5
            """,
            schema_name="brahmand"
        )
        
        assert_query_success(response)
        row = response['results'][0]
        assert 'unique_friends' in row
        assert 'total_paths' in row
        assert 'countries' in row
        assert row['total_paths'] >= row['unique_friends']
    
    def test_vlp_nested_aggregation(self, simple_graph):
        """VLP with nested aggregation (HAVING clause)."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS*1..2]->(friend:User)
            WITH u, count(DISTINCT friend) as friend_count
            WHERE friend_count > 2
            RETURN u.name, friend_count
            ORDER BY friend_count DESC
            LIMIT 5
            """,
            schema_name="brahmand"
        )
        
        assert_query_success(response)
        # All results should have friend_count > 2
        for row in response['results']:
            assert row['friend_count'] > 2


class TestVLPPathFunctions:
    """Test VLP with path functions (nodes, relationships, length)."""
    
    def test_vlp_nodes_function(self, simple_graph):
        """Test nodes() function on VLP."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS*2]->(friend:User)
            WHERE u.user_id = 1
            RETURN u.name, friend.name, length(nodes(p)) as node_count
            LIMIT 5
            """,
            schema_name="brahmand"
        )
        
        assert_query_success(response)
        # 2 hops = 3 nodes (start, middle, end)
        for row in response['results']:
            assert row['node_count'] == 3
    
    def test_vlp_relationships_function(self, simple_graph):
        """Test relationships() function on VLP."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS*2]->(friend:User)
            WHERE u.user_id = 1
            RETURN u.name, friend.name, length(relationships(p)) as rel_count
            LIMIT 5
            """,
            schema_name="brahmand"
        )
        
        assert_query_success(response)
        # 2 hops = 2 relationships
        for row in response['results']:
            assert row['rel_count'] == 2
