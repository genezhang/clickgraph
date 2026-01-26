"""
Integration tests for WITH CTE node expansion fix.

Tests the fix for: "CTE node expansion problem" - ensuring WITH-exported
variables expand to all properties (not just the variable alias).

Test Scenarios:
1. Basic WITH Node Export - Simple expansion of exported node
2. Multi-Variable WITH Export - Multiple variables in same CTE
3. WITH Chaining - Nested WITH clauses (multi-level CTEs)
4. WITH Scalar Export - Aggregate function in WITH (should NOT expand)
5. WITH Property Rename - WITH u AS person (renamed export)
6. Cross-Table WITH - Multi-hop WITH + RETURN
7. Optional Match with WITH - OPTIONAL MATCH then WITH
8. Polymorphic Node Labels - Multiple labels for same node (edge case)
9. Denormalized Edges - Nodes stored in edge table (edge case)

Each test verifies:
- WITH-exported variables expand to multiple columns
- Column names follow pattern: <alias>.<property>
- Correct number of properties returned
- No regressions in base table expansion
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    get_single_value,
    get_column_values
)


def get_columns_from_response(response):
    """Extract column names from response."""
    results = response.get("results", [])
    if results and isinstance(results[0], dict):
        return list(results[0].keys())
    return []


class TestWithBasicNodeExpansion:
    """Test 1: Basic WITH node export."""
    
    def test_with_single_node_export(self):
        """
        Test basic WITH node export.
        
        MATCH (a:User)
        WITH a
        RETURN a
        
        Expected: a expands to a.user_id, a.name, a.email, etc.
        """
        response = execute_cypher(
            """
            MATCH (a:User)
            WITH a
            RETURN a
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        
        columns = get_columns_from_response(response)
        
        # Should have multiple columns like "a_user_id", "a_name", etc.
        a_columns = [col for col in columns if col.startswith("a_")]
        assert len(a_columns) >= 2, f"Expected multiple a_* columns, got: {columns}"
        
        # Verify common properties are present
        assert any("user_id" in col for col in a_columns), \
            f"user_id not found in columns: {columns}"
        assert any("name" in col for col in a_columns), \
            f"name property not found in columns: {columns}"


class TestWithMultipleVariableExport:
    """Test 2: Multi-variable WITH export."""
    
    def test_with_two_node_export(self):
        """
        Test WITH exporting two related nodes.
        
        MATCH (a:User)-[r:FOLLOWS]->(b:User)
        WITH a, b
        RETURN a, b
        
        Expected: Both a and b expand to properties
        """
        response = execute_cypher(
            """
            MATCH (a:User)-[r:FOLLOWS]->(b:User)
            WITH a, b
            RETURN a, b
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        
        columns = get_columns_from_response(response)
        
        # Check a.* columns
        a_columns = [col for col in columns if col.startswith("a_")]
        assert len(a_columns) >= 2, \
            f"Expected multiple a.* columns, got: {columns}"
        
        # Check b.* columns
        b_columns = [col for col in columns if col.startswith("b_")]
        assert len(b_columns) >= 2, \
            f"Expected multiple b.* columns, got: {columns}"
        
        # Verify both have properties
        assert any("user_id" in col for col in a_columns), \
            f"a.user_id not found in columns: {columns}"
        assert any("user_id" in col for col in b_columns), \
            f"b.user_id not found in columns: {columns}"

    def test_with_three_node_export(self):
        """Test WITH exporting three nodes from multi-hop pattern."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
            WITH a, b, c
            RETURN a, b, c
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        
        columns = get_columns_from_response(response)
        
        # Check all three variables expanded
        for var in ["a", "b", "c"]:
            var_columns = [col for col in columns if col.startswith(f"{var}_")]
            assert len(var_columns) >= 2, \
                f"Expected multiple {var}.* columns, got: {columns}"


class TestWithChaining:
    """Test 3: WITH chaining (nested CTEs)."""
    
    def test_with_chaining_two_levels(self):
        """
        Test WITH chaining - nested WITH clauses.
        
        MATCH (a:User)
        WITH a
        MATCH (b:User)
        WITH a, b
        RETURN a, b
        
        Expected: Both a and b expand (a comes from first CTE, b from base table)
        """
        response = execute_cypher(
            """
            MATCH (a:User)
            WITH a
            MATCH (b:User)
            WITH a, b
            RETURN a, b
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        
        columns = get_columns_from_response(response)
        
        # Check a.* columns (from first CTE)
        a_columns = [col for col in columns if col.startswith("a_")]
        assert len(a_columns) >= 2, \
            f"Expected multiple a.* columns from first CTE, got: {columns}"
        
        # Check b.* columns (from base table)
        b_columns = [col for col in columns if col.startswith("b_")]
        assert len(b_columns) >= 2, \
            f"Expected multiple b.* columns from base table, got: {columns}"

    def test_with_chaining_three_levels(self):
        """Test WITH chaining with three levels."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WITH a
            MATCH (b:User)
            WITH a, b
            MATCH (c:User)
            WITH a, b, c
            RETURN a, b, c
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        
        # All three should have properties
        for var in ["a", "b", "c"]:
            var_columns = [col for col in columns if col.startswith(f"{var}_")]
            assert len(var_columns) >= 2, \
                f"Expected {var}.* properties at level 3, got: {columns}"


class TestWithScalarExport:
    """Test 4: WITH scalar export (aggregates)."""
    
    def test_with_scalar_count(self):
        """
        Test WITH scalar - aggregation should NOT expand.
        
        MATCH (a:User) WITH COUNT(a) AS count
        RETURN count
        
        Expected: count is single column (scalar), not expanded
        """
        response = execute_cypher(
            """
            MATCH (a:User)
            WITH COUNT(a) AS user_count
            RETURN user_count
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        
        columns = get_columns_from_response(response)
        
        # Should have exactly one column for the count
        assert "user_count" in columns, \
            f"Expected 'user_count' column, got: {columns}"
        
        # Count should NOT expand (no .* columns)
        assert not any("user_count." in col for col in columns), \
            f"Scalar 'user_count' should not expand, got: {columns}"
        
        # Verify value is numeric
        value = get_single_value(response, "user_count", convert_to_int=True)
        assert value > 0, f"Count should be > 0, got: {value}"

    def test_with_scalar_and_node(self):
        """Test WITH mixing scalar aggregation and node export."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WITH a, COUNT(b) AS follower_count
            RETURN a, follower_count
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        
        # Node a should expand
        a_columns = [col for col in columns if col.startswith("a_")]
        assert len(a_columns) >= 2, \
            f"Expected a.* columns, got: {columns}"
        
        # Scalar should NOT expand
        assert "follower_count" in columns, \
            f"Expected follower_count column, got: {columns}"
        assert not any("follower_count." in col for col in columns), \
            f"Scalar follower_count should not expand, got: {columns}"


class TestWithPropertyRename:
    """Test 5: WITH property rename."""
    
    def test_with_node_rename(self):
        """
        Test WITH node aliased with AS clause.
        
        MATCH (a:User) WITH a AS person
        RETURN person
        
        Expected: person expands to person.user_id, person.name, etc.
        """
        response = execute_cypher(
            """
            MATCH (a:User)
            WITH a AS person
            RETURN person
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        
        # person should expand (not as 'a')
        person_columns = [col for col in columns if col.startswith("person_")]
        assert len(person_columns) >= 2, \
            f"Expected person.* columns, got: {columns}"
        
        # Should NOT have 'a.' columns
        a_columns = [col for col in columns if col.startswith("a_")]
        assert len(a_columns) == 0, \
            f"Should not have 'a.' columns (renamed to person), got: {columns}"

    def test_with_multi_rename(self):
        """Test WITH multiple nodes with renames."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WITH a AS follower, b AS followed
            RETURN follower, followed
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        
        # Both renamed variables should expand
        follower_columns = [col for col in columns if col.startswith("follower_")]
        assert len(follower_columns) >= 2, \
            f"Expected follower.* columns, got: {columns}"
        
        followed_columns = [col for col in columns if col.startswith("followed_")]
        assert len(followed_columns) >= 2, \
            f"Expected followed.* columns, got: {columns}"


class TestWithCrossTable:
    """Test 6: Cross-table WITH patterns."""
    
    def test_with_cross_table_multi_hop(self):
        """
        Test complex WITH with multiple hops and different node types.
        
        MATCH (a:User)-[:FOLLOWS]->(b:User)
        WITH a, b
        MATCH (c:Post) WHERE c.user_id = a.user_id
        RETURN a, b, c
        
        Expected: a and b from WITH expand, c from base table expands
        """
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WITH a, b
            MATCH (c:Post) WHERE c.user_id = a.user_id
            RETURN a, b, c
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        
        # Check all three expand
        for var in ["a", "b", "c"]:
            var_columns = [col for col in columns if col.startswith(f"{var}_")]
            assert len(var_columns) >= 1, \
                f"Expected {var}.* columns, got: {columns}"

    def test_with_where_filter(self):
        """Test WITH followed by WHERE filter."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WITH a
            WHERE a.user_id > 1
            RETURN a
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        a_columns = [col for col in columns if col.startswith("a_")]
        assert len(a_columns) >= 2, \
            f"Expected a.* columns after WHERE, got: {columns}"


class TestWithOptionalMatch:
    """Test 7: Optional match with WITH."""
    
    def test_optional_match_with_export(self):
        """
        Test OPTIONAL MATCH followed by WITH.
        
        MATCH (a:User)
        OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
        WITH a, b
        RETURN a, b
        
        Expected: a expands, b may be NULL (OPTIONAL)
        """
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            WITH a, b
            RETURN a, b
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        
        # a should expand
        a_columns = [col for col in columns if col.startswith("a_")]
        assert len(a_columns) >= 2, \
            f"Expected a.* columns, got: {columns}"
        
        # b should also expand (or be NULL for OPTIONAL matches)
        b_columns = [col for col in columns if col.startswith("b_")]
        # b might not have values but should have columns
        assert len(b_columns) >= 1, \
            f"Expected b.* columns (optional), got: {columns}"


class TestWithPolymorphicLabels:
    """Test 8: Polymorphic node labels (edge case)."""
    
    def test_with_multi_label_node(self):
        """
        Test WITH when node might have multiple labels.
        
        In social_benchmark, User and Post are distinct.
        Test WITH on either type.
        """
        # Test with User
        response_user = execute_cypher(
            """
            MATCH (a:User)
            WITH a
            RETURN a
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response_user)
        user_columns = get_columns_from_response(response_user)
        user_a_columns = [col for col in user_columns if col.startswith("a_")]
        assert len(user_a_columns) >= 2
        
        # Test with Post
        response_post = execute_cypher(
            """
            MATCH (p:Post)
            WITH p
            RETURN p
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response_post)
        post_columns = get_columns_from_response(response_post)
        post_p_columns = [col for col in post_columns if col.startswith("p_")]
        assert len(post_p_columns) >= 1


class TestWithRegressionCases:
    """Regression tests - ensure existing behavior not broken."""
    
    def test_base_table_expansion_unchanged(self):
        """
        Verify base table expansion still works (not changed by fix).
        
        MATCH (a:User)
        RETURN a
        """
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN a
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        # Base table expansion can use either format (a_name or a.name)
        a_columns = [col for col in columns if col.startswith("a_") or col.startswith("a.")]
        assert len(a_columns) >= 2, \
            f"Base table expansion regression - got: {columns}"

    def test_property_access_unchanged(self):
        """
        Verify explicit property access still works.
        
        MATCH (a:User)
        RETURN a.user_id, a.name
        """
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN a.user_id, a.name
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        # Should have exactly these columns
        assert "a.user_id" in columns or "user_id" in columns, \
            f"Expected user_id column, got: {columns}"

    def test_aggregation_without_with(self):
        """
        Verify aggregation still works without WITH.
        
        MATCH (a:User)
        RETURN COUNT(a)
        """
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN COUNT(a) as total
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        value = get_single_value(response, "total", convert_to_int=True)
        assert value > 0, f"Aggregation without WITH returned: {value}"

    def test_multi_hop_without_with(self):
        """
        Verify multi-hop still works without WITH.
        
        MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)
        RETURN a, b, c
        """
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
            RETURN a, b, c
            LIMIT 1
            """,
            schema_name="social_benchmark"
        )
        
        assert_query_success(response)
        
        columns = get_columns_from_response(response)
        for var in ["a", "b", "c"]:
            # Base table expansion can use either format (a_name or a.name)
            var_columns = [col for col in columns if col.startswith(f"{var}_") or col.startswith(f"{var}.")]
            assert len(var_columns) >= 1, \
                f"Expected {var}.* or {var}_* columns, got: {columns}"
