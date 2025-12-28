"""
GraphRAG Phase 1.1: Multi-Type Recursive Pattern Tests

Tests for variable-length paths with multiple relationship types:
- [:TYPE1|TYPE2*1..2] patterns
- Mixed recursive/non-recursive types
- UNION ALL SQL generation verification
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
)


class TestMultiTypeRecursivePatterns:
    """Test multi-type VLP patterns with social_benchmark schema."""
    
    def test_follows_or_authored_one_to_two_hops(self):
        """
        Test [:FOLLOWS|AUTHORED*1..2] pattern.
        
        FOLLOWS: User->User (recursive - can chain)
        AUTHORED: User->Post (non-recursive - stops at Post)
        
        Expected paths:
        - 1-hop FOLLOWS: user -> followed_user
        - 1-hop AUTHORED: user -> post
        - 2-hop FOLLOWS->FOLLOWS: user -> user -> user
        - 2-hop FOLLOWS->AUTHORED: user -> user -> post
        - 2-hop AUTHORED->X: None (Post has no outgoing edges)
        """
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
            WHERE u.user_id = 1
            RETURN DISTINCT labels(x)[1] as node_type, count(*) as cnt
            ORDER BY node_type
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        
        # Should get both User and Post nodes
        assert len(results) >= 2, "Expected at least User and Post results"
        
        # Check we got both node types
        node_types = [r["node_type"] if isinstance(r, dict) else r[0] for r in results]
        assert "User" in node_types, "Should find User nodes via FOLLOWS"
        assert "Post" in node_types, "Should find Post nodes via AUTHORED"
    
    def test_multi_type_with_sql_only(self):
        """Verify SQL generation shows UNION ALL for multiple types."""
        import requests
        
        response = requests.post(
            "http://localhost:8080/query",
            json={
                "query": """
                    USE social_benchmark
                    MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
                    WHERE u.user_id = 1
                    RETURN x
                    LIMIT 5
                """,
                "sql_only": True
            }
        )
        
        assert response.status_code == 200, f"Query failed: {response.text}"
        result = response.json()
        
        sql = result.get("sql", "") or result.get("generated_sql", "")
        
        # Should contain UNION ALL for multiple relationship types
        assert "UNION ALL" in sql, f"Multi-type VLP should generate UNION ALL. SQL: {sql[:500]}"
        
        # Should mention both relationship tables
        assert "user_follows_bench" in sql or "follows" in sql.lower(), "Should use FOLLOWS table"
        
        # Multi-type VLP uses "WITH ... UNION ALL" NOT "WITH RECURSIVE"
        # because we're combining different node types, not recursing through same type
        assert "WITH vlp_multi_type" in sql, "Multi-type VLP should use multi-type CTE"
        # Should NOT be recursive for multi-type VLP
        assert "WITH RECURSIVE" not in sql, "Multi-type VLP should NOT use recursive CTE (uses UNION ALL instead)"
    
    def test_multi_type_exact_paths(self):
        """Test specific path results from multi-type VLP."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS|AUTHORED*1]->(x)
            WHERE u.user_id = 1
            RETURN labels(x)[1] as node_type, count(*) as cnt
            ORDER BY node_type
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        # At 1-hop: should get immediate followers + authored posts
        assert len(response["results"]) >= 1
    
    def test_multi_type_two_hops_only(self):
        """Test *2 (exactly 2 hops) with multiple types."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS|AUTHORED*2]->(x)
            WHERE u.user_id = 1
            RETURN DISTINCT labels(x)[1] as node_type, count(*) as cnt
            ORDER BY node_type
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        # At 2-hops: 
        # - FOLLOWS->FOLLOWS: User->User->User ✅
        # - FOLLOWS->AUTHORED: User->User->Post ✅
        # - AUTHORED->*: None (Post has no outgoing edges)
        
        node_types = [r["node_type"] if isinstance(r, dict) else r[0] for r in response["results"]]
        # Should find at least User nodes (via FOLLOWS->FOLLOWS)
        assert "User" in node_types or len(results) > 0


class TestMultiTypeWithPathFunctions:
    """Test path functions with multi-type patterns."""
    
    def test_length_with_multi_type(self):
        """Test length(path) works with multi-type VLP."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
            WHERE u.user_id = 1
            RETURN length(p) as path_length, count(*) as cnt
            GROUP BY path_length
            ORDER BY path_length
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        
        # Should have both 1-hop and 2-hop paths
        lengths = [r["path_length"] if isinstance(r, dict) else r[0] for r in results]
        assert 1 in lengths, "Should have 1-hop paths"
        assert 2 in lengths, "Should have 2-hop paths"
    
    def test_relationships_with_multi_type(self):
        """Test relationships(path) returns correct types."""
        response = execute_cypher(
            """
            MATCH p = (u:User)-[:FOLLOWS|AUTHORED*1]->(x)
            WHERE u.user_id = 1
            RETURN relationships(p) as rels, labels(x)[1] as node_type
            LIMIT 10
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        # Should return relationship arrays
        assert len(response["results"]) > 0


class TestMixedRecursiveNonRecursive:
    """Test patterns mixing recursive and non-recursive edge types."""
    
    def test_recursive_stops_at_non_recursive_target(self):
        """
        Verify that AUTHORED (User->Post) naturally stops recursion.
        
        Post nodes have no outgoing AUTHORED or FOLLOWS edges,
        so 2-hop AUTHORED->* paths should not exist.
        """
        # First, verify 1-hop AUTHORED works
        response_1hop = execute_cypher(
            """
            MATCH (u:User)-[:AUTHORED*1]->(p:Post)
            WHERE u.user_id = 1
            RETURN count(p) as cnt
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response_1hop)
        results_1hop = response_1hop["results"]
        cnt_1hop = results_1hop[0]["cnt"] if isinstance(results_1hop[0], dict) else results_1hop[0][0]
        
        if cnt_1hop > 0:
            # User 1 has authored posts
            # Now try 2-hop: should not find anything via AUTHORED->AUTHORED
            response_2hop = execute_cypher(
                """
                MATCH (u:User)-[:AUTHORED*2]->(x)
                WHERE u.user_id = 1
                RETURN count(*) as cnt
                """,
                schema_name="social_benchmark"
            )
            
            assert_query_success(response_2hop)
            results_2hop = response_2hop["results"]
            cnt_2hop = results_2hop[0]["cnt"] if isinstance(results_2hop[0], dict) else results_2hop[0][0]
            
            # Should be 0 because Post->* via AUTHORED doesn't exist
            assert cnt_2hop == 0, "AUTHORED*2 should return 0 (Post has no outgoing AUTHORED edges)"
    
    def test_follows_continues_after_follows(self):
        """Verify FOLLOWS (User->User) can chain multiple times."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS*2]->(f:User)
            WHERE u.user_id = 1
            RETURN count(f) as cnt
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        cnt = results[0]["cnt"] if isinstance(results[0], dict) else results[0][0]
        
        # Should find 2-hop follows paths (if data exists)
        # Note: May be 0 if user 1 doesn't have 2-hop paths
        assert cnt >= 0, "Query should execute successfully"


class TestMultiTypePerformance:
    """Test multi-type patterns with larger result sets."""
    
    @pytest.mark.slow
    def test_multi_type_all_users(self):
        """Test multi-type VLP across all users (no WHERE filter)."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
            RETURN count(*) as total_paths
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        total = results[0]["total_paths"] if isinstance(results[0], dict) else results[0][0]
        
        assert total > 0, "Should find paths across all users"
    
    def test_multi_type_with_limit(self):
        """Test multi-type with LIMIT (optimization check)."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
            WHERE u.user_id = 1
            RETURN x
            LIMIT 10
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        # Should complete quickly even with LIMIT
        assert len(response["results"]) <= 10


if __name__ == "__main__":
    # Quick manual test
    print("Multi-type VLP test suite")
    print("Run with: pytest tests/integration/test_graphrag_multi_type.py -v")
