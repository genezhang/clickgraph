"""
Integration tests for relationship traversal patterns.

Tests cover:
- Single-hop relationships
- Multi-hop traversals
- Bidirectional patterns
- Multiple relationship types
- Relationship property filtering
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    assert_contains_value
)


class TestSingleHopTraversal:
    """Test single-hop relationship traversal."""
    
    def test_outgoing_relationship(self, simple_graph):
        """Test (a)-[r]->(b) pattern."""
        response = execute_cypher(
            "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN a.name, b.name",
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 6)  # 6 follow relationships
        assert_column_exists(response, "a.name")
        assert_column_exists(response, "b.name")
    
    def test_incoming_relationship(self, simple_graph):
        """Test (a)<-[r]-(b) pattern."""
        response = execute_cypher(
            "MATCH (a:User)<-[r:FOLLOWS]-(b:User) WHERE a.name = 'Charlie' RETURN b.name ORDER BY b.name",
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)  # Alice and Bob follow Charlie
        assert_contains_value(response, "b.name", "Alice")
        assert_contains_value(response, "b.name", "Bob")
    
    def test_undirected_relationship(self, simple_graph):
        """Test (a)-[r]-(b) pattern (either direction)."""
        response = execute_cypher(
            "MATCH (a:User)-[r:FOLLOWS]-(b:User) WHERE a.name = 'Bob' RETURN b.name ORDER BY b.name",
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Bob follows: Charlie, Diana
        # Bob is followed by: Alice
        assert_row_count(response, 3)
    
    def test_relationship_with_source_filter(self, simple_graph):
        """Test relationship with WHERE on source node."""
        response = execute_cypher(
            "MATCH (a:User)-[r:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b.name ORDER BY b.name",
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)  # Alice follows Bob and Charlie
        assert_contains_value(response, "b.name", "Bob")
        assert_contains_value(response, "b.name", "Charlie")
    
    def test_relationship_with_target_filter(self, simple_graph):
        """Test relationship with WHERE on target node."""
        response = execute_cypher(
            "MATCH (a:User)-[r:FOLLOWS]->(b:User) WHERE b.name = 'Diana' RETURN a.name ORDER BY a.name",
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)  # Bob and Charlie follow Diana
        assert_contains_value(response, "a.name", "Bob")
        assert_contains_value(response, "a.name", "Charlie")


class TestMultiHopTraversal:
    """Test multi-hop relationship patterns."""
    
    def test_two_hop_traversal(self, simple_graph):
        """Test (a)-[]->(b)-[]->(c) pattern."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
            RETURN a.name, b.name, c.name
            ORDER BY a.name, b.name, c.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Charlie
        # Alice -> Bob -> Diana
        # Alice -> Charlie -> Diana
        # Bob -> Charlie -> Diana
        assert_row_count(response, 4)
    
    def test_three_hop_traversal(self, simple_graph):
        """Test (a)-[]->(b)-[]->(c)-[]->(d) pattern."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)-[:FOLLOWS]->(d:User)
            RETURN a.name, b.name, c.name, d.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Charlie -> Diana
        # Alice -> Bob -> Diana -> Eve
        # Alice -> Charlie -> Diana -> Eve
        # Bob -> Charlie -> Diana -> Eve
        assert_row_count(response, 4)
    
    def test_multi_hop_with_filter(self, simple_graph):
        """Test multi-hop with WHERE clause."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
            WHERE a.name = 'Alice'
            RETURN b.name, c.name
            ORDER BY b.name, c.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 3)
        # Alice -> Bob -> Charlie
        # Alice -> Bob -> Diana
        # Alice -> Charlie -> Diana


class TestBidirectionalPatterns:
    """Test bidirectional relationship patterns."""
    
    def test_mutual_follows(self, simple_graph):
        """Test finding mutual follows: (a)-[]->(b)-[]->(a)."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(a)
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # No mutual follows in our test data
        assert_row_count(response, 0)
    
    def test_triangle_pattern(self, simple_graph):
        """Test triangle: (a)-[]->(b)-[]->(c)-[]->(a)."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)-[:FOLLOWS]->(a)
            RETURN a.name, b.name, c.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # No triangles in our test data
        assert_row_count(response, 0)


class TestRelationshipProperties:
    """Test relationship property access and filtering."""
    
    def test_return_relationship_property(self, simple_graph):
        """Test returning relationship properties."""
        response = execute_cypher(
            """
            MATCH (a:User)-[r:FOLLOWS]->(b:User)
            WHERE a.name = 'Alice'
            RETURN a.name, b.name, r.since
            ORDER BY b.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)
        assert_column_exists(response, "r.since")
    
    def test_filter_by_relationship_property(self, simple_graph):
        """Test WHERE clause on relationship property."""
        response = execute_cypher(
            """
            MATCH (a:User)-[r:FOLLOWS]->(b:User)
            WHERE r.since >= '2023-02-01'
            RETURN a.name, b.name
            ORDER BY a.name, b.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Bob -> Charlie (2023-02-01)
        # Charlie -> Diana (2023-02-15)
        # Diana -> Eve (2023-03-01)
        # Bob -> Diana (2023-03-15)
        assert_row_count(response, 4)


class TestMultipleNodes:
    """Test patterns with multiple distinct nodes."""
    
    def test_three_nodes_linear(self, simple_graph):
        """Test linear pattern with three nodes."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
            WHERE a.name = 'Alice' AND c.name = 'Diana'
            RETURN a.name, b.name, c.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Diana
        # Alice -> Charlie -> Diana
        assert_row_count(response, 2)
    
    def test_four_nodes_path(self, simple_graph):
        """Test path with four distinct nodes."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)-[:FOLLOWS]->(d:User)
            WHERE a.name = 'Alice' AND d.name = 'Eve'
            RETURN a.name, b.name, c.name, d.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Charlie -> Diana -> Eve (but only 3 hops shown)
        # Alice -> Bob -> Diana -> Eve
        # Alice -> Charlie -> Diana -> Eve
        assert_row_count(response, 2)


class TestRelationshipCounting:
    """Test aggregations on relationships."""
    
    def test_count_outgoing_relationships(self, simple_graph):
        """Test counting outgoing relationships."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as following_count
            ORDER BY following_count DESC, a.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Alice: 2 (Bob, Charlie)
        # Bob: 2 (Charlie, Diana)
        # Charlie: 1 (Diana)
        # Diana: 1 (Eve)
        # Eve: 0
        assert_row_count(response, 4)  # Users with at least 1 follow
    
    def test_count_incoming_relationships(self, simple_graph):
        """Test counting incoming relationships (followers)."""
        response = execute_cypher(
            """
            MATCH (a:User)<-[:FOLLOWS]-(b:User)
            RETURN a.name, COUNT(b) as follower_count
            ORDER BY follower_count DESC, a.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Charlie: 2 (Alice, Bob)
        # Diana: 2 (Bob, Charlie)
        # Bob: 1 (Alice)
        # Eve: 1 (Diana)
        # Alice: 0
        assert_row_count(response, 4)  # Users with at least 1 follower
    
    def test_relationship_degree(self, simple_graph):
        """Test total relationship count (degree)."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]-(b:User)
            RETURN a.name, COUNT(DISTINCT b) as connections
            ORDER BY connections DESC, a.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Bob: 3 (follows Charlie, Diana; followed by Alice)
        # Charlie: 3 (followed by Alice, Bob; follows Diana)
        # Diana: 3 (followed by Bob, Charlie; follows Eve)
        assert_row_count(response, 5)


class TestComplexPatterns:
    """Test complex relationship patterns."""
    
    def test_friends_of_friends(self, simple_graph):
        """Test finding friends of friends (2-hop connections)."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(friend:User)-[:FOLLOWS]->(fof:User)
            WHERE a.name = 'Alice' AND fof.name <> 'Alice'
            RETURN DISTINCT fof.name
            ORDER BY fof.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Alice -> Bob -> Charlie
        # Alice -> Bob -> Diana
        # Alice -> Charlie -> Diana
        assert_row_count(response, 2)  # Charlie and Diana
    
    def test_common_connections(self, simple_graph):
        """Test finding users with common connections."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(common:User)<-[:FOLLOWS]-(b:User)
            WHERE a.name = 'Alice' AND b.name = 'Bob' AND a.name < b.name
            RETURN common.name
            ORDER BY common.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Both Alice and Bob follow Charlie
        assert_row_count(response, 1)
        assert_contains_value(response, "common.name", "Charlie")
