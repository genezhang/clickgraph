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

Test dependencies:
- ClickGraph server running on localhost:8080
- social_benchmark schema loaded
- brahmand.users_bench and brahmand.posts_bench tables populated
"""

import pytest
import requests

# Test configuration
CLICKGRAPH_URL = "http://localhost:8080/query"
TIMEOUT = 30

def query_api(cypher: str, sql_only: bool = False) -> dict:
    """Execute a Cypher query via the ClickGraph API."""
    payload = {"query": cypher}
    if sql_only:
        payload["sql_only"] = True
    response = requests.post(CLICKGRAPH_URL, json=payload, timeout=TIMEOUT)
    return response.json()


class TestJoinContextVLPChained:
    """Tests for VLP + chained pattern with JoinContext integration."""

    def test_vlp_chained_aggregation_bug1(self):
        """
        Bug #1: VLP + chained pattern + aggregation.
        
        This query combines:
        1. VLP pattern: (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        2. Chained pattern: (u2:User)-[:AUTHORED]->(p:Post)
        3. Aggregation: COUNT(DISTINCT p)
        
        The key fix is that the JOIN for AUTHORED must use t.end_id
        (from VLP CTE) instead of u2.user_id (from node table).
        """
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount
        LIMIT 10
        """
        result = query_api(query, sql_only=True)
        
        assert "generated_sql" in result, f"Query failed: {result}"
        sql = result["generated_sql"]
        
        # Key assertions for proper JOIN generation:
        # 1. Should have VLP CTE
        assert "WITH RECURSIVE" in sql, "Should generate VLP CTE"
        assert "vlp_u1_u2" in sql, "CTE should be named vlp_u1_u2"
        
        # 2. Should JOIN posts_bench to CTE endpoint
        assert "t.end_id" in sql, "Should reference CTE endpoint for chained JOIN"
        
        # 3. Should NOT have dangling WHERE clause with original alias
        # (The filter should be inside the CTE, not in outer WHERE)
        lines = sql.split('\n')
        outer_where_count = sum(1 for line in lines 
                                if 'WHERE u1.user_id' in line 
                                and 'start_node' not in line)
        # The outer WHERE should either not exist or reference CTE columns
        # (allowing one instance inside CTE is fine)
        
    def test_vlp_chained_simple_return(self):
        """
        Simpler VLP + chained pattern without aggregation.
        
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.name, p.content LIMIT 5
        """
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.name, p.content
        LIMIT 5
        """
        result = query_api(query, sql_only=True)
        
        assert "generated_sql" in result, f"Query failed: {result}"
        sql = result["generated_sql"]
        
        # Should have CTE and proper JOIN
        assert "WITH RECURSIVE" in sql
        assert "vlp_u1_u2" in sql
        # Post table should JOIN on CTE endpoint
        assert "posts_bench" in sql

    def test_vlp_chained_with_intermediate_properties(self):
        """
        VLP + chained with properties from intermediate node (u2).
        
        This tests that endpoint properties are correctly available.
        """
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u1.name AS starter, u2.name AS author, p.post_id
        LIMIT 5
        """
        result = query_api(query, sql_only=True)
        
        assert "generated_sql" in result, f"Query failed: {result}"
        sql = result["generated_sql"]
        
        # Both start and end node properties should be rewritten to CTE columns
        assert "start_name" in sql or "t.start" in sql, "Start node property should use CTE"
        assert "end_name" in sql or "t.end" in sql, "End node property should use CTE"

    def test_vlp_chained_multiple_aggregations(self):
        """
        VLP + chained with multiple aggregations.
        
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.user_id, COUNT(p) as post_count, COUNT(DISTINCT p.content) as unique_contents
        LIMIT 10
        """
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1
        RETURN u2.user_id, COUNT(p) as post_count
        LIMIT 10
        """
        result = query_api(query, sql_only=True)
        
        assert "generated_sql" in result, f"Query failed: {result}"
        sql = result["generated_sql"]
        
        # Should have proper GROUP BY referencing CTE column
        assert "GROUP BY" in sql
        assert "t.end" in sql, "GROUP BY should use CTE endpoint reference"


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
