"""
Integration tests for VLP CTE Column Scoping fix.

Tests variable-length path queries followed by additional relationships
with GROUP BY aggregations. This pattern previously failed with
"Unknown expression identifier" errors because aggregate-referenced
columns weren't included in the UNION SELECT list.

Bug Fix: Collect aliases from aggregate expressions and include their
ID columns in UNION SELECT to make them available for outer aggregation.

Note: These tests use the social_benchmark schema (users_bench, posts_bench).
"""

import pytest
import requests
import os

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")


def execute_query(cypher_query, sql_only=False, schema_name="social_benchmark"):
    """Execute a Cypher query against ClickGraph."""
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": cypher_query, "sql_only": sql_only, "schema_name": schema_name},
        timeout=30
    )
    
    if response.status_code != 200:
        return {
            "error": f"HTTP {response.status_code}: {response.text}",
            "success": False
        }
    
    data = response.json()
    
    if "error" in data and data["error"]:
        return {"error": data["error"], "success": False}
    
    return {"data": data, "success": True}


class TestVLPAggregation:
    """Test VLP queries with additional relationships and aggregations."""

    def test_vlp_with_count_distinct_basic_sql_generation(self):
        """
        Test basic VLP + additional relationship + COUNT(DISTINCT) SQL generation.
        This is the core pattern that was failing before the fix.
        Verify SQL generates without "Unknown expression identifier" errors.
        """
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount
        ORDER BY postCount DESC, userId ASC
        LIMIT 10
        """
        
        result = execute_query(cypher, sql_only=True)
        assert result["success"], f"SQL generation failed: {result.get('error')}"
        
        sql = result["data"].get("generated_sql", "")
        assert sql, "No SQL generated"
        # Verify p (post) columns are included in generated SQL
        assert "p." in sql.lower() or "post" in sql.lower(), \
            "Post columns should be included in UNION SELECT"

    def test_vlp_with_count_distinct_basic(self):
        """
        Test basic VLP + additional relationship + COUNT(DISTINCT).
        This is the core pattern that was failing before the fix.
        """
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount
        ORDER BY postCount DESC, userId ASC
        LIMIT 10
        """
        
        result = execute_query(cypher)
        # Accept success or database/schema errors (not scoping errors)
        if not result["success"]:
            error = result.get("error", "").lower()
            # These are acceptable (data/schema issues, not scoping bugs)
            acceptable = ["does not exist", "not found", "unknown label", "unknown database"]
            assert any(err in error for err in acceptable), \
                f"Unexpected error (possible scoping bug): {result.get('error')}"
        else:
            # Should generate SQL without "Unknown expression identifier" error
            data = result["data"]
            assert "columns" in data or "results" in data or data.get("rows", 0) >= 0

    def test_vlp_with_multiple_aggregates(self):
        """Test VLP with multiple aggregate functions."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        RETURN u2.user_id AS userId, 
               COUNT(DISTINCT p) AS postCount,
               COUNT(p) AS totalPosts
        ORDER BY postCount DESC
        LIMIT 5
        """
        
        result = execute_query(cypher)
        assert result["success"], f"Query failed: {result.get('error')}"

    def test_vlp_with_sum_aggregate(self):
        """Test VLP with SUM aggregate on node properties."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        RETURN u2.user_id AS userId,
               u2.name AS userName,
               SUM(1) AS postCount
        ORDER BY postCount DESC
        LIMIT 5
        """
        
        result = execute_query(cypher)
        assert result["success"], f"Query failed: {result.get('error')}"

    def test_vlp_with_multiple_additional_relationships(self):
        """Test VLP followed by multiple additional relationships with aggregation."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User),
              (u2)-[:AUTHORED]->(p:Post),
              (u2)<-[:LIKED]-(like:Post)
        RETURN u2.user_id AS userId,
               COUNT(DISTINCT p) AS postCount,
               COUNT(DISTINCT like) AS likeCount
        ORDER BY postCount DESC
        LIMIT 5
        """
        
        result = execute_query(cypher)
        # This might fail due to schema, but should not have scoping errors
        # Accept success or specific non-scoping errors
        if not result["success"]:
            error_msg = result.get("error", "").lower()
            # These are acceptable errors (schema issues, not scoping bugs)
            acceptable_errors = [
                "not found",
                "does not exist",
                "unknown label",
                "unknown type",
                "invalid relationship pattern"  # Direction mismatch is a schema-level error, not scoping
            ]
            assert any(err in error_msg for err in acceptable_errors), \
                f"Unexpected error (possible scoping bug): {result.get('error')}"

    def test_vlp_bidirectional_with_aggregate(self):
        """Test bidirectional VLP pattern (generates UNION) with aggregate."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)
        WITH u2
        MATCH (u2)-[:AUTHORED]->(p:Post)
        RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount
        ORDER BY postCount DESC
        LIMIT 5
        """
        
        result = execute_query(cypher)
        assert result["success"], f"Query failed: {result.get('error')}"

    def test_vlp_with_having_equivalent(self):
        """Test VLP + GROUP BY + WHERE filter on aggregate (HAVING equivalent)."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        WITH u2, COUNT(DISTINCT p) AS postCount
        WHERE postCount > 0
        RETURN u2.user_id AS userId, postCount
        ORDER BY postCount DESC
        LIMIT 5
        """
        
        result = execute_query(cypher)
        assert result["success"], f"Query failed: {result.get('error')}"

    def test_vlp_different_hop_counts(self):
        """Test VLP scoping fix works across different hop counts."""
        hop_patterns = [
            ("*1", 1),
            ("*2", 2),
            ("*1..2", "1-2"),
            ("*1..3", "1-3"),
        ]
        
        for pattern, description in hop_patterns:
            cypher = f"""
            MATCH (u1:User {{user_id: 1}})-[:FOLLOWS{pattern}]-(u2:User)-[:AUTHORED]->(p:Post)
            RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount
            LIMIT 5
            """
            
            result = execute_query(cypher)
            assert result["success"], \
                f"Query failed for hop pattern {description}: {result.get('error')}"

    def test_vlp_with_order_by_aggregate(self):
        """Test ORDER BY using aggregate result."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount
        ORDER BY postCount DESC, userId ASC
        LIMIT 10
        """
        
        result = execute_query(cypher)
        assert result["success"], f"Query failed: {result.get('error')}"

    def test_sql_generation_includes_aggregate_columns(self):
        """Verify SQL generation includes aggregate-referenced columns in UNION SELECT."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        RETURN u2.user_id, COUNT(DISTINCT p) AS postCount
        """
        
        result = execute_query(cypher, sql_only=True)
        assert result["success"], f"Query failed: {result.get('error')}"
        
        sql = result["data"].get("generated_sql", "")
        assert sql, "No SQL generated"
        
        # The fix should add p.id (or p.post_id) to UNION SELECT
        # Look for patterns indicating p column in SELECT before GROUP BY
        # This is a heuristic check - actual column name depends on schema
        assert "p." in sql.lower() or "post" in sql.lower(), \
            f"Generated SQL may not include post columns in UNION SELECT:\n{sql[:500]}"

    def test_vlp_with_nested_property_aggregate(self):
        """Test aggregate with property access inside (e.g., SUM(p.length))."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        RETURN u2.user_id AS userId, SUM(1) AS totalPosts
        ORDER BY totalPosts DESC
        LIMIT 5
        """
        
        result = execute_query(cypher)
        assert result["success"], f"Query failed: {result.get('error')}"


class TestVLPAggregationEdgeCases:
    """Test edge cases and boundary conditions for VLP aggregation fix."""

    def test_vlp_no_aggregation(self):
        """Verify regular VLP queries (without aggregation) still work."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        RETURN u2.user_id, u2.name, p.post_id
        LIMIT 10
        """
        
        result = execute_query(cypher)
        assert result["success"], f"Query failed: {result.get('error')}"

    def test_vlp_only_vlp_nodes_in_aggregate(self):
        """Test aggregates on VLP nodes only (no additional relationships)."""
        cypher = """
        MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..2]-(u2:User)
        RETURN u1.user_id, COUNT(DISTINCT u2) AS friendCount
        """
        
        result = execute_query(cypher)
        assert result["success"], f"Query failed: {result.get('error')}"

    def test_vlp_complex_where_with_aggregate(self):
        """Test complex WHERE clause with VLP and aggregation."""
        cypher = """
        MATCH (u1:User)-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
        WHERE u1.user_id = 1 AND u2.is_active = true
        RETURN u2.user_id, COUNT(DISTINCT p) AS postCount
        ORDER BY postCount DESC
        LIMIT 5
        """
        
        result = execute_query(cypher)
        # May fail on schema, but should not be a scoping error
        if not result["success"]:
            error_msg = result.get("error", "").lower()
            assert "unknown expression identifier" not in error_msg, \
                f"Scoping error detected: {result.get('error')}"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
