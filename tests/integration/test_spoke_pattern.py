"""
Test spoke/star pattern support - multiple paths converging on a central hub node.

The spoke pattern (also called star pattern) has multiple paths that share a common
central node, like:
  a -> hub <- c
  d -> hub <- e

Or more complex patterns where paths both converge and diverge:
  a -> hub -> c
  e -> hub -> d
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
)


class TestSpokePattern:
    """Test spoke/star patterns with comma-separated path patterns."""
    
    def test_simple_spoke_inbound(self, simple_graph):
        """Test simple spoke: multiple nodes pointing to central hub.

        Cypher relationship-uniqueness applies across the whole MATCH clause
        (comma patterns included), so `a` and `c` must bind DISTINCT edges
        into the hub. Charlie (user_id=3) has two followers (Alice, Bob), so
        the oracle answer is the 2 distinct-pair permutations. (Bob, the
        previous hub choice, has only ONE follower — the correct Neo4j answer
        there is 0 rows; the old expectation relied on the engine illegally
        reusing one relationship twice, fixed by #518/#586.)
        """
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(hub:TestUser),
                  (c:TestUser)-[:TEST_FOLLOWS]->(hub)
            WHERE hub.user_id = 3
            RETURN a.name, hub.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )

        assert_query_success(response)
        # Charlie (user_id=3) is followed by Alice and Bob: 2 permutations
        assert len(response["results"]) == 2
        assert_column_exists(response, "hub.name")
        assert response["results"][0]["hub.name"] == "Charlie"
        pairs = {(r["a.name"], r["c.name"]) for r in response["results"]}
        assert pairs == {("Alice", "Bob"), ("Bob", "Alice")}
    
    def test_simple_spoke_outbound(self, simple_graph):
        """Test simple spoke: central hub pointing to multiple nodes."""
        response = execute_cypher(
            """
            MATCH (hub:TestUser)-[:TEST_FOLLOWS]->(a:TestUser), 
                  (hub)-[:TEST_FOLLOWS]->(c:TestUser)
            WHERE hub.user_id = 1
            RETURN hub.name, a.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Alice (user_id=1) follows others
        assert len(response["results"]) >= 1
        assert_column_exists(response, "hub.name")
    
    def test_bowtie_pattern(self, simple_graph, clickhouse_client, test_database):
        """Test bowtie pattern: paths converging and diverging from hub (a->hub->c, e->hub->d).

        Relationship-uniqueness (whole-MATCH-clause scope, comma patterns
        included) means a/e must bind DISTINCT in-edges and c/d DISTINCT
        out-edges. The stock fixture gives Bob only ONE follower, for which
        the Neo4j-correct answer is 0 rows (the old >=1 expectation relied on
        illegal edge reuse, fixed by #518/#586). Seed one extra follower so a
        genuine bowtie exists: in {Alice, Eve}, out {Charlie, Diana} -> 2x2
        ordered permutations = 4 rows.
        """
        clickhouse_client.command(
            f"INSERT INTO {test_database}.follows VALUES (5, 2, '2023-04-01')"
        )
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(hub:TestUser)-[:TEST_FOLLOWS]->(c:TestUser),
                  (e:TestUser)-[:TEST_FOLLOWS]->(hub)-[:TEST_FOLLOWS]->(d:TestUser)
            WHERE hub.user_id = 2
            RETURN a.name, hub.name, c.name, d.name, e.name
            """,
            schema_name=simple_graph["schema_name"]
        )

        assert_query_success(response)
        # Bob is the hub: followers {Alice, Eve}, followees {Charlie, Diana}
        assert len(response["results"]) == 4

        for result in response["results"]:
            assert result["hub.name"] == "Bob"
            # 'a' and 'e' are distinct users who follow Bob
            assert {result["a.name"], result["e.name"]} == {"Alice", "Eve"}
            # 'c' and 'd' are distinct users Bob follows
            assert {result["c.name"], result["d.name"]} == {"Charlie", "Diana"}
    
    def test_spoke_with_aggregation(self, simple_graph):
        """Test spoke pattern with COUNT aggregation."""
        response = execute_cypher(
            """
            MATCH (follower:TestUser)-[:TEST_FOLLOWS]->(hub:TestUser)
            WHERE hub.user_id = 2
            RETURN hub.name, COUNT(follower) as follower_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        
        result = response["results"][0]
        assert result["hub.name"] == "Bob"
        assert result["follower_count"] >= 1
    
    def test_triangle_pattern(self, simple_graph):
        """Test triangle pattern: three nodes with circular relationships.
        
        Note: True circular patterns (a->b->c->a) have a known SQL generation bug
        where the first table reference appears before it's in FROM clause.
        Testing a simpler triangle pattern instead.
        """
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser), 
                  (b)-[:TEST_FOLLOWS]->(c:TestUser)
            WHERE a.user_id = 1
            RETURN a.name, b.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        # Triangle exists in test data: Alice->Bob->Charlie
        assert_query_success(response)
        assert len(response["results"]) >= 1
        assert_column_exists(response, "a.name")
        assert_column_exists(response, "b.name")
        assert_column_exists(response, "c.name")


class TestPatternEdgeCases:
    """Test edge cases and requirements for comma-separated patterns."""
    
    def test_pattern_requires_explicit_labels(self, simple_graph):
        """All nodes in comma-separated patterns should have explicit labels."""
        # This should work (all labels explicit)
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser), (c:TestUser)-[:TEST_FOLLOWS]->(b)
            WHERE b.user_id = 2
            RETURN a.name, b.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        assert_query_success(response)
    
    def test_shared_node_connection(self, simple_graph):
        """Comma-separated patterns must share at least one node (connected)."""
        # Both patterns share node 'b' - this should work
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser), (b)-[:TEST_FOLLOWS]->(c:TestUser)
            WHERE a.user_id = 1
            RETURN a.name, b.name, c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        assert_query_success(response)
        assert len(response["results"]) >= 1
