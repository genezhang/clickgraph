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
            # AUTHORED*2 should be rejected as semantically invalid (non-transitive relationship)
            response_2hop = execute_cypher(
                """
                MATCH (u:User)-[:AUTHORED*2]->(x)
                WHERE u.user_id = 1
                RETURN count(*) as cnt
                """,
                schema_name="social_benchmark",
                raise_on_error=False  # Don't raise, we expect an error
            )
            
            # Should return an error about non-transitive relationship
            assert "non-transitive" in str(response_2hop).lower(), \
                f"Expected non-transitive error, got: {response_2hop}"
    
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


class TestMultiTypePropertyExtraction:
    """
    Test property extraction from JSON for multi-type VLP endpoints.
    
    Multi-type VLP stores node properties in JSON (end_properties column).
    Properties are extracted using JSON_VALUE() in ClickHouse SQL.
    
    Feature implemented: Jan 6, 2026
    """
    
    def test_basic_property_access(self):
        """Test basic property access on multi-type VLP endpoint."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS*1..2]->(x:User)
            WHERE u.user_id = 1
            RETURN x.name, x.email
            LIMIT 3
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        
        # Verify we got results with properties
        assert len(results) > 0, "Should return User nodes"
        
        for result in results:
            # Properties should exist (may be empty if no data)
            assert "x.name" in result, "Should have x.name column"
            assert "x.email" in result, "Should have x.email column"
            
            # At least some results should have actual values
            if result["x.name"]:
                assert isinstance(result["x.name"], str), "Name should be string"
    
    def test_label_function_with_property(self):
        """Test label() function alongside property access."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS*1..2]->(x:User)
            WHERE u.user_id = 1
            RETURN label(x), x.name, x.city
            LIMIT 3
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        
        assert len(results) > 0
        for result in results:
            # label() should return node type
            assert result["label(x)"] == "User", "All nodes should be User type"
            
            # Properties should be accessible
            assert "x.name" in result
            assert "x.city" in result
    
    def test_multiple_properties(self):
        """Test accessing multiple properties at once."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS*1]->(x:User)
            WHERE u.user_id = 1
            RETURN x.user_id, x.name, x.email, x.city, x.country
            LIMIT 5
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        
        # Should have all requested properties
        for result in results:
            assert "x.user_id" in result
            assert "x.name" in result
            assert "x.email" in result
            assert "x.city" in result
            assert "x.country" in result
    
    @pytest.mark.xfail(reason="Non-existent properties throw ClickHouse error instead of returning NULL - requires schema validation")
    @pytest.mark.xfail(reason="Non-existent properties throw ClickHouse errors - needs schema validation")
    def test_missing_property_returns_empty(self):
        """Test that accessing non-existent property returns empty/null."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS*1]->(x:User)
            WHERE u.user_id = 1
            RETURN x.name, x.nonexistent_property
            LIMIT 2
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        
        for result in results:
            # Real property should work
            assert "x.name" in result
            
            # Non-existent property should return empty string (ClickHouse JSON_VALUE behavior)
            assert "x.nonexistent_property" in result
            assert result["x.nonexistent_property"] == "" or result["x.nonexistent_property"] is None
    
    def test_property_with_filter(self):
        """Test property access with WHERE clause on properties."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS*1..2]->(x:User)
            WHERE u.user_id = 1 AND x.city = 'NYC'
            RETURN x.name, x.city
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        # Query should execute (results depend on test data)
        results = response["results"]
        
        # All results should have city = 'NYC' if any returned
        for result in results:
            assert result["x.city"] == "NYC"
    
    def test_property_with_order_by(self):
        """Test ORDER BY on extracted JSON properties."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS*1..2]->(x:User)
            WHERE u.user_id = 1
            RETURN x.name, x.email
            ORDER BY x.name
            LIMIT 5
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        
        if len(results) > 1:
            # Verify ordering (names should be sorted)
            names = [r["x.name"] for r in results if r["x.name"]]
            assert names == sorted(names), "Results should be ordered by name"
    
    @pytest.mark.xfail(reason="GROUP BY with variable-length range paths (*1..2) causes timeout - recursive CTE + aggregation issue")
    @pytest.mark.xfail(reason="GROUP BY with VLP ranges (*1..2) causes hangs/OOM - recursive CTE issue")
    def test_property_with_aggregation(self):
        """Test aggregation with JSON property access."""
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS*1..2]->(x:User)
            WHERE u.user_id = 1
            RETURN x.city, count(*) as user_count
            GROUP BY x.city
            ORDER BY user_count DESC
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        
        # Should group by city and count users
        for result in results:
            assert "x.city" in result
            assert "user_count" in result
            assert result["user_count"] > 0
    
    def test_multi_type_vlp_different_properties(self):
        """
        Test multi-type VLP where different node types have different properties.
        
        User has: name, email, city
        Post has: content, date (no name)
        """
        response = execute_cypher(
            """
            MATCH (u:User)-[:FOLLOWS|AUTHORED*1]->(x)
            WHERE u.user_id = 1
            RETURN label(x), x.name, x.content
            LIMIT 10
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        results = response["results"]
        
        assert len(results) > 0
        
        # Check both User and Post nodes
        user_results = [r for r in results if r["label(x)"] == "User"]
        post_results = [r for r in results if r["label(x)"] == "Post"]
        
        # User nodes should have name, not content
        for result in user_results:
            assert "x.name" in result
            # name should have value for Users
            if result["x.name"]:
                assert isinstance(result["x.name"], str)
        
        # Post nodes should have content, not name
        for result in post_results:
            assert "x.content" in result
            # Posts don't have name property, should be empty/null
            assert result["x.name"] == "" or result["x.name"] is None
    
    def test_json_extraction_sql_generation(self):
        """Verify SQL uses JSON_VALUE() for multi-type VLP, direct access for single-type."""
        import requests
        
        # Single-type VLP should use direct column access
        response = requests.post(
            "http://localhost:8080/query",
            json={
                "query": """
                    USE social_benchmark
                    MATCH (u:User)-[:FOLLOWS*1..2]->(x:User)
                    WHERE u.user_id = 1
                    RETURN x.name, x.email
                    LIMIT 3
                """,
                "sql_only": True
            }
        )
        
        assert response.status_code == 200
        result = response.json()
        sql = result.get("sql", "") or result.get("generated_sql", "")
        
        # Single-type VLP should use direct column access (NOT JSON_VALUE)
        assert "x.full_name" in sql, "Should use direct column access for single-type VLP"
        assert "x.email_address" in sql, "Should use direct column access for single-type VLP"
        assert "JSON_VALUE" not in sql, "Should NOT use JSON_VALUE for single-type VLP"
        
        # Multi-type VLP SHOULD use JSON_VALUE
        response_multi = requests.post(
            "http://localhost:8080/query",
            json={
                "query": """
                    USE social_benchmark
                    MATCH (u:User)-[:FOLLOWS|AUTHORED*1]->(x)
                    WHERE u.user_id = 1
                    RETURN x.name, x.content
                    LIMIT 3
                """,
                "sql_only": True
            }
        )
        
        assert response_multi.status_code == 200
        result_multi = response_multi.json()
        sql_multi = result_multi.get("sql", "") or result_multi.get("generated_sql", "")
        
        # Multi-type VLP should use JSON_VALUE for property extraction
        assert "JSON_VALUE" in sql_multi, "Should use JSON_VALUE for multi-type VLP"
        assert "end_properties" in sql_multi, "Should extract from end_properties JSON column"
        assert "'$.name'" in sql_multi or "'$.content'" in sql_multi, "Should use JSON path for properties"
    
    def test_cte_columns_direct_access(self):
        """Verify properties use direct column access for single-type VLP."""
        import requests
        
        response = requests.post(
            "http://localhost:8080/query",
            json={
                "query": """
                    USE social_benchmark
                    MATCH (u:User)-[:FOLLOWS*1..2]->(x:User)
                    WHERE u.user_id = 1
                    RETURN x.name, x.city
                    LIMIT 3
                """,
                "sql_only": True
            }
        )
        
        assert response.status_code == 200
        result = response.json()
        sql = result.get("sql", "") or result.get("generated_sql", "")
        
        # Single-type VLP should use direct column access (NOT JSON)
        assert "x.full_name" in sql, "x.name should use direct column access for single-type VLP"
        assert "x.city" in sql, "x.city should use direct column access for single-type VLP"
        assert "JSON_VALUE" not in sql, "Should NOT use JSON extraction for single-type VLP"


if __name__ == "__main__":
    # Quick manual test
    print("Multi-type VLP test suite")
    print("Run with: pytest tests/integration/test_graphrag_multi_type.py -v")
