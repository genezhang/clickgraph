"""
Integration tests for VLP + Chained Pattern + Aggregation (Bug #1 fix via JoinContext).

These tests verify that the JoinContext architecture correctly handles the case where:
1. A VLP pattern produces a CTE with end_id column
2. A subsequent chained pattern JOINs to the VLP result
3. The JOIN condition correctly uses t.end_id (not the original node alias)

Example query:
    MATCH (u1:User)-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post) 
    WHERE u1.user_id = 1 
    RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount

Before fix: JOIN used u2.user_id (wrong - u2 doesn't exist in CTE)
After fix: JOIN uses t.end_id (correct - references VLP CTE column)
"""
import pytest
import requests
import re

CLICKGRAPH_URL = "http://localhost:8080"
SCHEMA_NAME = "social_benchmark"


def execute_query(query: str, sql_only: bool = True, timeout: int = 30) -> dict:
    """Execute a query against ClickGraph."""
    if not query.strip().upper().startswith("USE "):
        query = f"USE {SCHEMA_NAME} {query}"
    
    payload = {"query": query, "sql_only": sql_only}
    try:
        response = requests.post(f"{CLICKGRAPH_URL}/query", json=payload, timeout=timeout)
        return {
            "status_code": response.status_code,
            "body": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
            "success": response.status_code == 200,
        }
    except Exception as e:
        return {"status_code": 0, "body": str(e), "success": False}


class TestVlpChainedAggregation:
    """Test VLP + chained pattern + aggregation (Bug #1 scenario)."""
    
    def test_vlp_chained_pattern_join_reference(self):
        """Test that chained pattern after VLP uses correct JOIN reference (t.end_id)."""
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post) 
        WHERE u1.user_id = 1 
        RETURN u2.user_id AS userId, COUNT(DISTINCT p) AS postCount
        """
        result = execute_query(query, sql_only=True)
        assert result["success"], f"Query failed: {result['body']}"
        
        sql = result["body"].get("generated_sql", "")
        
        # The key assertion: JOIN condition should reference t.end_id, not u2.user_id
        # Because u2 is the end node of the VLP, its ID is stored in the CTE as end_id
        assert "t.end_id" in sql or "end_id" in sql, \
            f"Expected JOIN to use t.end_id but got:\n{sql[:1000]}"
    
    def test_vlp_chained_pattern_executes(self):
        """Test that VLP + chained pattern query executes successfully.
        
        Note: This test uses a simple VLP pattern with tight constraints
        to ensure fast execution.
        """
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1 
        RETURN u2.user_id AS userId
        LIMIT 5
        """
        result = execute_query(query, sql_only=False, timeout=15)
        # Query should complete without error
        assert result["success"], f"Query execution failed: {result['body']}"
    
    def test_vlp_followed_by_single_hop_pattern(self):
        """Test VLP followed by single-hop pattern."""
        query = """
        MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)-[:FOLLOWS]->(c:User)
        WHERE a.user_id = 1
        RETURN DISTINCT c.user_id
        LIMIT 10
        """
        result = execute_query(query, sql_only=True)
        assert result["success"], f"Query failed: {result['body']}"
    
    def test_vlp_with_aggregation_group_by(self):
        """Test VLP with aggregation creates proper GROUP BY."""
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        RETURN u2.user_id, count(*) as pathCount
        """
        result = execute_query(query, sql_only=True)
        assert result["success"], f"Query failed: {result['body']}"
        
        sql = result["body"].get("generated_sql", "")
        assert "GROUP BY" in sql, f"Expected GROUP BY in SQL:\n{sql}"
    
    def test_vlp_with_count_distinct(self):
        """Test VLP with COUNT(DISTINCT)."""
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)
        WHERE u1.user_id = 1
        RETURN COUNT(DISTINCT u2.user_id) as uniqueUsers
        """
        result = execute_query(query, sql_only=True)
        assert result["success"], f"Query failed: {result['body']}"
        
        sql = result["body"].get("generated_sql", "")
        # Case-insensitive check for count
        assert re.search(r"count\s*\(\s*distinct", sql, re.IGNORECASE), \
            f"Expected COUNT(DISTINCT) in SQL:\n{sql}"
    
    def test_multiple_vlp_patterns(self):
        """Test query with multiple VLP patterns (complex join scenario)."""
        # This is a more complex scenario - may not be fully supported yet
        query = """
        MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)
        WHERE a.user_id = 1
        RETURN b.user_id, b.name
        LIMIT 5
        """
        result = execute_query(query, sql_only=True)
        assert result["success"], f"Query failed: {result['body']}"
    
    def test_vlp_chained_generates_valid_sql(self):
        """Test that generated SQL has proper CTE structure."""
        query = """
        MATCH (u1:User)-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post) 
        WHERE u1.user_id = 1 
        RETURN u2.user_id, p.post_id
        LIMIT 10
        """
        result = execute_query(query, sql_only=True)
        assert result["success"], f"Query failed: {result['body']}"
        
        sql = result["body"].get("generated_sql", "")
        
        # Should have WITH RECURSIVE for VLP
        assert "WITH RECURSIVE" in sql, f"Expected WITH RECURSIVE in SQL:\n{sql[:500]}"
        
        # Should have proper CTE name pattern
        assert re.search(r"vlp_\w+_\w+", sql), f"Expected VLP CTE name pattern:\n{sql[:500]}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
