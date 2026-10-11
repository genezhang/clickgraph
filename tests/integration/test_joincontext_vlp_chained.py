"""
Integration tests for JoinContext with VLP + chained patterns + aggregation.

This test file specifically covers Bug #1: VLP + chained pattern + aggregation.
The bug scenario:
  MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
  WHERE u1.user_id = 1
  RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount

The fix ensures that when a VLP pattern ([:FOLLOWS*1..2]) is followed by a
chained fixed-length pattern ([:AUTHORED]), the JOIN for the chained pattern
correctly references the VLP CTE endpoint (t.end_id) instead of trying to
JOIN against the original node table.

This is achieved via JoinContext tracking VLP endpoints and marking them
so subsequent JOINs use CTE references.

The chained tests check RESULT rows against a brute-force oracle over the
fixture's FOLLOWS/AUTHORED edges (trails of 1..2 FOLLOWS hops from user 1,
then each AUTHORED edge of the path's end), not the SQL text, so they hold
whatever the renderer's naming (values verified against Neo4j 5.26).

Test dependencies:
- ClickGraph server running on localhost:7475
- social_integration schema loaded
- test_integration.users_test and test_integration.posts_test tables populated
"""

import collections
import os
import pytest
import requests

# Test configuration
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:7475") + "/query"
TIMEOUT = 30

def query_api(cypher: str, sql_only: bool = False) -> dict:
    """Execute a Cypher query via the ClickGraph API."""
    payload = {"query": cypher, "schema_name": "social_integration"}
    if sql_only:
        payload["sql_only"] = True
    response = requests.post(CLICKGRAPH_URL, json=payload, timeout=TIMEOUT)
    return response.json()


def _rows(cypher: str) -> list:
    result = query_api(cypher)
    assert "results" in result, f"Query failed: {result}"
    return result["results"]


def _chained_oracle():
    """(u2 id, u2 name, post id, post content) per row of
    `(u1 {user_id: 1})-[:FOLLOWS*1..2]->(u2)-[:AUTHORED]->(p)`."""
    follows = [(r["a"], r["b"]) for r in _rows(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS a, b.user_id AS b")]
    authored = [(r["u"], r["name"], r["p"], r["content"]) for r in _rows(
        "MATCH (u:User)-[:AUTHORED]->(p:Post) "
        "RETURN u.user_id AS u, u.name AS name, p.post_id AS p, p.content AS content")]
    ends = []

    def walk(node, used, depth):
        if depth:
            ends.append(node)
        if depth < 2:
            for i, (a, b) in enumerate(follows):
                if a == node and i not in used:
                    walk(b, used | {i}, depth + 1)

    walk(1, frozenset(), 0)
    return [row for end in ends for row in authored if row[0] == end]


class TestJoinContextVLPChained:
    """Tests for VLP + chained pattern with JoinContext integration."""

    def test_vlp_chained_aggregation_bug1(self):
        """
        Bug #1: VLP + chained pattern + aggregation.
        
        This query combines:
        1. VLP pattern: (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        2. Chained pattern: (u2:User)-[:AUTHORED]->(p:Post)
        3. Aggregation: COUNT(DISTINCT p)
        
        The AUTHORED hop must hang off the path's END node (u2), not the
        node table or the path's start.
        """
        rows = _rows("""
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount
        """)
        expected = {}
        for u2, _, post, _ in _chained_oracle():
            expected.setdefault(u2, set()).add(post)
        got = {r["userId"]: r["postCount"] for r in rows}
        assert len(rows) == len(got), rows
        assert got == {u: len(ps) for u, ps in expected.items()}
        assert got == {u: 2 for u in range(1, 9)}

    def test_vlp_chained_simple_return(self):
        """
        Simpler VLP + chained pattern without aggregation.
        
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.name, p.content
        """
        rows = _rows("""
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.name, p.content
        """)
        got = collections.Counter((r["u2.name"], r["p.content"]) for r in rows)
        expected = collections.Counter((name, content) for _, name, _, content in _chained_oracle())
        assert got == expected
        assert sum(got.values()) == 20

    def test_vlp_chained_with_intermediate_properties(self):
        """
        VLP + chained with properties from both path endpoints (u1 and u2).
        
        This tests that endpoint properties are correctly available.
        """
        rows = _rows("""
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u1.name AS starter, u2.name AS author, p.post_id
        """)
        starter = _rows("MATCH (u:User) WHERE u.user_id = 1 RETURN u.name AS name")[0]["name"]
        got = collections.Counter((r["starter"], r["author"], r["p.post_id"]) for r in rows)
        expected = collections.Counter(
            (starter, name, post) for _, name, post, _ in _chained_oracle())
        assert got == expected
        assert sum(got.values()) == 20

    def test_vlp_chained_multiple_aggregations(self):
        """
        VLP + chained with aggregation grouped by the path's end node.
        
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.user_id, COUNT(p) as post_count
        """
        rows = _rows("""
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.user_id, COUNT(p) as post_count
        """)
        got = {r["u2.user_id"]: r["post_count"] for r in rows}
        assert len(rows) == len(got), rows
        assert got == dict(collections.Counter(u2 for u2, _, _, _ in _chained_oracle()))
        # user 1 is reached by three trails (1->x->1 cycles), the others by one
        assert got == {1: 6, **{u: 2 for u in range(2, 9)}}


class TestJoinContextVLPOnlyPatterns:
    """Tests for VLP patterns without chaining (baseline)."""

    def test_vlp_simple(self):
        """Simple VLP pattern returns expected structure."""
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        RETURN u2.user_id
        LIMIT 10
        """
        result = query_api(query, sql_only=True)
        
        assert "generated_sql" in result
        sql = result["generated_sql"]
        assert "WITH RECURSIVE" in sql
        
    def test_vlp_with_aggregation(self):
        """VLP pattern with aggregation on endpoint."""
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        RETURN COUNT(DISTINCT u2.user_id) as reachable_count
        """
        result = query_api(query, sql_only=True)
        
        assert "generated_sql" in result
        sql = result["generated_sql"]
        assert "WITH RECURSIVE" in sql
        # count can be lowercase in ClickHouse SQL
        assert "count" in sql.lower()


class TestJoinContextFixedLengthChained:
    """Tests for fixed-length chained patterns (no VLP) - baseline."""

    def test_fixed_length_chained(self):
        """Fixed-length chained pattern without VLP."""
        query = """
        MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.user_id, COUNT(p) as post_count
        LIMIT 10
        """
        result = query_api(query, sql_only=True)
        
        assert "generated_sql" in result
        sql = result["generated_sql"]
        
        # Fixed-length should NOT use recursive CTE
        assert "WITH RECURSIVE" not in sql
        # Should have JOINs
        assert "JOIN" in sql


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
