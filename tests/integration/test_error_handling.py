"""
Integration tests for error handling and edge cases.

Tests cover:
- Malformed Cypher queries
- Invalid syntax
- Non-existent labels and relationships
- Type mismatches
- Database connection errors
- Query validation errors
- Schema validation errors
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success
)


class TestMalformedQueries:
    """Test handling of malformed Cypher queries."""
    
    def test_incomplete_match_pattern(self, simple_graph):
        """Test incomplete MATCH pattern."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"],
            raise_on_error=False
        )
        
        # Should return error response
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_missing_return_clause(self, simple_graph):
        """Test query without RETURN clause."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.age > 25
            """,
            schema_name=simple_graph["schema_name"],
            raise_on_error=False
        )
        
        # Should return error
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_unmatched_parentheses(self, simple_graph):
        """Test query with unmatched parentheses."""
        response = execute_cypher(
            """
            MATCH (a:User
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"],
            raise_on_error=False
        )
        
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_invalid_operator(self, simple_graph):
        """Test query with invalid operator."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.age === 30
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"],
            raise_on_error=False
        )
        
        # === is not valid Cypher (should be =)
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_incomplete_relationship_pattern(self, simple_graph):
        """Test incomplete relationship pattern."""
        response = execute_cypher(
            """
            MATCH (a)-[:FOLLOWS
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"],
            raise_on_error=False
        )
        
        assert "error" in response or "errors" in response or response.get("status") == "error"


class TestNonExistentElements:
    """Test queries referencing non-existent graph elements."""
    
    def test_nonexistent_label(self, simple_graph):
        """Test querying non-existent node label."""
        response = execute_cypher(
            """
            MATCH (n:NonExistentLabel)
            RETURN n.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should either error or return empty results
        if "error" not in response and "errors" not in response:
            # If no error, should return empty results
            assert response.get("results") == [] or len(response.get("results", [])) == 0
    
    def test_nonexistent_relationship_type(self, simple_graph):
        """Test querying non-existent relationship type."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:NONEXISTENT_REL]->(b:User)
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should either error or return empty results
        if "error" not in response and "errors" not in response:
            assert response.get("results") == [] or len(response.get("results", [])) == 0
    
    def test_nonexistent_property(self, simple_graph):
        """Test accessing non-existent property."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.nonexistent_property
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should succeed but return NULL for non-existent properties
        # This is valid Cypher behavior
        assert_query_success(response)
    
    def test_nonexistent_database(self, simple_graph):
        """Test querying non-existent database."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as count
            """,
            schema_name="completely_nonexistent_database_12345", raise_on_error=False)
        
        # Should return error about database not found
        assert "error" in response or "errors" in response or response.get("status") == "error"


class TestInvalidSyntax:
    """Test queries with invalid Cypher syntax."""
    
    def test_invalid_where_syntax(self, simple_graph):
        """Test invalid WHERE clause syntax."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.age > AND a.name = 'Alice'
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_invalid_return_syntax(self, simple_graph):
        """Test invalid RETURN syntax."""
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN a.name AS
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_invalid_order_by(self, simple_graph):
        """Test invalid ORDER BY syntax."""
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN a.name
            ORDER BY
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_invalid_aggregation(self, simple_graph):
        """Test invalid aggregation syntax."""
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN COUNT()
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # COUNT() without argument is invalid
        assert "error" in response or "errors" in response or response.get("status") == "error"


class TestTypeMismatches:
    """Test queries with type mismatches."""
    
    def test_string_comparison_with_number(self, simple_graph):
        """Test comparing string with number."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name > 30
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # May succeed with type coercion or error
        # Either is acceptable
        assert isinstance(response, dict)
    
    def test_invalid_arithmetic(self, simple_graph):
        """Test invalid arithmetic operations."""
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN a.name + a.age
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # String + integer may error or coerce
        assert isinstance(response, dict)
    
    def test_null_comparison(self, simple_graph):
        """Test NULL comparisons (edge case)."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.nonexistent_prop = NULL
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # = NULL is always false in Cypher (should use IS NULL)
        # This is valid syntax but semantic edge case
        assert_query_success(response)
        # Should return no results (NULL = NULL is false)
        assert len(response.get("results", [])) == 0


class TestInvalidPatterns:
    """Test invalid graph patterns."""
    
    def test_disconnected_pattern(self, simple_graph):
        """Test pattern with disconnected nodes."""
        response = execute_cypher(
            """
            MATCH (a:User), (b:User)
            WHERE a.name = 'Alice' AND b.name = 'Bob'
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Disconnected patterns are valid (Cartesian product)
        assert_query_success(response)
    
    def test_invalid_variable_length_range(self, simple_graph):
        """Test invalid variable-length range."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS*5..2]->(b:User)
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # *5..2 is invalid (min > max)
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_negative_variable_length(self, simple_graph):
        """Test negative variable-length hop count."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS*-1]->(b:User)
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Negative hop count is invalid
        assert "error" in response or "errors" in response or response.get("status") == "error"


class TestQueryComplexity:
    """Test edge cases with query complexity."""
    
    def test_deeply_nested_pattern(self, simple_graph):
        """Test very deep pattern (should succeed or timeout gracefully)."""
        response = execute_cypher(
            """
            MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)-[:FOLLOWS]->(d)
                 -[:FOLLOWS]->(e)-[:FOLLOWS]->(f)-[:FOLLOWS]->(g)
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should succeed (even if no results) or timeout gracefully
        assert isinstance(response, dict)
    
    def test_very_large_variable_length(self, simple_graph):
        """Test very large variable-length bound."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS*1..1000]->(b:User)
            WHERE a.name = 'Alice'
            RETURN COUNT(b) as count
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should succeed or return max recursion error
        assert isinstance(response, dict)
    
    def test_multiple_variable_length_paths(self, simple_graph):
        """Test multiple variable-length patterns in one query."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS*1..3]->(b:User),
                  (b)-[:FOLLOWS*1..3]->(c:User)
            WHERE a.name = 'Alice'
            RETURN COUNT(c) as count
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Complex but valid query
        assert_query_success(response)


class TestEmptyResults:
    """Test queries that should return empty results."""
    
    def test_impossible_filter(self, simple_graph):
        """Test filter that can never be true."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.age > 1000
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        assert_query_success(response)
        assert len(response.get("results", [])) == 0
    
    def test_contradictory_filters(self, simple_graph):
        """Test contradictory WHERE conditions."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.age > 30 AND a.age < 25
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        assert_query_success(response)
        assert len(response.get("results", [])) == 0
    
    def test_no_matching_relationship(self, simple_graph):
        """Test relationship pattern with no matches."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            WHERE a.name = 'Eve'
            RETURN b.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        assert_query_success(response)
        # Eve follows no one
        assert len(response.get("results", [])) == 0


class TestSpecialCharacters:
    """Test handling of special characters."""
    
    def test_single_quotes_in_string(self, simple_graph):
        """Test string with single quotes."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Alice\\'s Account'
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should handle escaped quotes
        assert_query_success(response)
    
    def test_unicode_in_query(self, simple_graph):
        """Test Unicode characters in query."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = '测试用户'
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should handle Unicode gracefully
        assert_query_success(response)
    
    def test_special_chars_in_property(self, simple_graph):
        """Test special characters in property values."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name CONTAINS '@#$%'
            RETURN a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # CONTAINS may not be implemented, but should handle gracefully
        assert isinstance(response, dict)


class TestLimitsAndBoundaries:
    """Test boundary conditions."""
    
    def test_zero_limit(self, simple_graph):
        """Test LIMIT 0."""
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN a.name
            LIMIT 0
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        assert_query_success(response)
        assert len(response.get("results", [])) == 0
    
    def test_negative_limit(self, simple_graph):
        """Test negative LIMIT."""
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN a.name
            LIMIT -1
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Negative LIMIT is invalid
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_very_large_limit(self, simple_graph):
        """Test very large LIMIT."""
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN a.name
            LIMIT 1000000
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should succeed (just returns all available rows)
        assert_query_success(response)
    
    def test_negative_skip(self, simple_graph):
        """Test negative SKIP."""
        response = execute_cypher(
            """
            MATCH (a:User)
            RETURN a.name
            SKIP -5
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Negative SKIP is invalid
        assert "error" in response or "errors" in response or response.get("status") == "error"


class TestCaseInsensitivity:
    """Test case sensitivity in Cypher keywords."""
    
    def test_lowercase_keywords(self, simple_graph):
        """Test query with lowercase keywords."""
        response = execute_cypher(
            """
            match (a:User)
            where a.age > 25
            return a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Cypher keywords are case-insensitive
        assert_query_success(response)
    
    def test_mixed_case_keywords(self, simple_graph):
        """Test query with mixed case keywords."""
        response = execute_cypher(
            """
            MaTcH (a:User)
            WhErE a.age > 25
            ReTuRn a.name
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should work (keywords are case-insensitive)
        assert_query_success(response)


class TestEmptyQuery:
    """Test empty or whitespace-only queries."""
    
    def test_empty_query(self, simple_graph):
        """Test completely empty query."""
        response = execute_cypher(
            "",
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should return error
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_whitespace_only_query(self, simple_graph):
        """Test query with only whitespace."""
        response = execute_cypher(
            "   \n\t  ",
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should return error
        assert "error" in response or "errors" in response or response.get("status") == "error"
    
    def test_comment_only_query(self, simple_graph):
        """Test query with only comments."""
        response = execute_cypher(
            """
            // This is just a comment
            /* And this is
               a multi-line comment */
            """,
            schema_name=simple_graph["schema_name"], raise_on_error=False)
        
        # Should return error (no actual query)
        assert "error" in response or "errors" in response or response.get("status") == "error"

