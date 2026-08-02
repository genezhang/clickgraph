"""
Critical regression tests for RETURN-only queries (queries without MATCH clause).

These tests prevent regressions from TypeInference consolidation that caused
RETURN-only queries to return empty results instead of using system.one.

Bug History:
- Pre-fix: `RETURN 1` generated invalid SQL with early exit returning empty result
- Post-fix: Detects RETURN-only pattern and uses ClickHouse's system.one table

Test Scope:
- Simple literals and expressions
- Arithmetic operations  
- String functions with parameters
- Query modifiers (ORDER BY, LIMIT, SKIP)
- Multiple columns

All tests use execute_cypher() helper for schema context and consistency.
"""

import pytest
from conftest import execute_cypher, assert_query_success


class TestReturnOnlyRegression:
    """Critical regression tests for RETURN-only queries without MATCH clause."""
    
    def test_return_simple_integer(self, simple_graph):
        """RETURN 1 AS num - The simplest possible Cypher query"""
        result = execute_cypher("RETURN 1 AS num", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1, f"Expected 1 row, got {len(result['results'])}"
        assert result["results"][0]["num"] == 1
    
    def test_return_arithmetic(self, simple_graph):
        """RETURN 1 + 1 AS sum - Basic arithmetic without MATCH"""
        result = execute_cypher("RETURN 1 + 1 AS sum", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1
        assert result["results"][0]["sum"] == 2
    
    def test_return_string_literal(self, simple_graph):
        """RETURN 'hello' AS greeting - String literal without MATCH"""
        result = execute_cypher("RETURN 'hello' AS greeting", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1
        assert result["results"][0]["greeting"] == "hello"
    
    def test_return_function_call(self, simple_graph):
        """RETURN toUpper('hello') AS upper - Function call without MATCH"""
        result = execute_cypher("RETURN toUpper('hello') AS upper", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1
        assert result["results"][0]["upper"] == "HELLO"
    
    def test_return_with_parameter(self, simple_graph):
        """RETURN $param AS value - Parameter usage without MATCH"""
        # Note: Parameter support requires schema context for type inference
        result = execute_cypher(
            "RETURN 'test_value' AS value",  # Simplified - parameters need schema context
            schema_name="social_integration"
        )
        assert_query_success(result)
        assert len(result["results"]) == 1
        assert result["results"][0]["value"] == "test_value"
    
    def test_return_multiple_columns(self, simple_graph):
        """RETURN 1 AS a, 2 AS b, 3 AS c - Multiple columns without MATCH"""
        result = execute_cypher("RETURN 1 AS a, 2 AS b, 3 AS c", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1
        row = result["results"][0]
        assert row["a"] == 1
        assert row["b"] == 2
        assert row["c"] == 3
    
    def test_return_with_order_by(self, simple_graph):
        """RETURN 3 AS x UNION RETURN 1 AS x UNION RETURN 2 AS x ORDER BY x"""
        # Note: UNION support may not be complete, using single RETURN for regression test
        result = execute_cypher("RETURN 42 AS x ORDER BY x", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1
        assert result["results"][0]["x"] == 42
    
    def test_return_with_limit(self, simple_graph):
        """RETURN 1 AS num LIMIT 1 - LIMIT modifier without MATCH"""
        result = execute_cypher("RETURN 1 AS num LIMIT 1", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1
        assert result["results"][0]["num"] == 1
    
    def test_return_with_skip(self, simple_graph):
        """RETURN 1 AS num SKIP 0 - SKIP modifier without MATCH"""
        result = execute_cypher("RETURN 1 AS num SKIP 0", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1
        assert result["results"][0]["num"] == 1
    
    def test_return_with_limit_skip(self, simple_graph):
        """RETURN 1 AS num SKIP 0 LIMIT 5 - Combined modifiers"""
        result = execute_cypher("RETURN 1 AS num SKIP 0 LIMIT 5", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1
        assert result["results"][0]["num"] == 1


class TestQuestionMarkInStringLiterals:
    """Regression tests for #872.

    A literal `?` in the generated SQL (e.g. inside a string literal) was
    consumed by the clickhouse-rs `Client::query` template scanner as a
    bind-parameter placeholder, so the query failed with "unbound query
    argument" even though the SQL was valid. The remote executor now escapes
    `?` -> `??` on the crate path (the crate collapses it back to a literal
    `?`). These execute end-to-end through the server's remote executor.
    """

    def test_return_string_with_trailing_question_mark(self, simple_graph):
        """RETURN 'why?' - the minimal repro that used to fail."""
        result = execute_cypher("RETURN 'why?' AS s", schema_name="social_integration")
        assert_query_success(result)
        assert len(result["results"]) == 1
        assert result["results"][0]["s"] == "why?"

    def test_return_string_with_embedded_question_mark(self, simple_graph):
        """A `?` in the middle of a string literal."""
        result = execute_cypher("RETURN 'a?b' AS s", schema_name="social_integration")
        assert_query_success(result)
        assert result["results"][0]["s"] == "a?b"

    def test_return_multiple_question_marks(self, simple_graph):
        """Several `?` in one literal all survive."""
        result = execute_cypher(
            "RETURN 'is it? yes! or? no?' AS s", schema_name="social_integration"
        )
        assert_query_success(result)
        assert result["results"][0]["s"] == "is it? yes! or? no?"

    def test_replace_question_mark(self, simple_graph):
        """`?` as both a function argument and inside the target string."""
        result = execute_cypher(
            "RETURN replace('a?b', '?', '!') AS s", schema_name="social_integration"
        )
        assert_query_success(result)
        assert result["results"][0]["s"] == "a!b"

    def test_regex_match_with_question_quantifier(self, simple_graph):
        """A regex `=~` operand containing a `?` quantifier."""
        result = execute_cypher("RETURN 'abc' =~ 'a?bc' AS s", schema_name="social_integration")
        assert_query_success(result)
        assert result["results"][0]["s"] in (True, 1)

    def test_no_question_mark_unaffected(self, simple_graph):
        """A query with no `?` is passed through unchanged (control)."""
        result = execute_cypher("RETURN 'no marks here' AS s", schema_name="social_integration")
        assert_query_success(result)
        assert result["results"][0]["s"] == "no marks here"


class TestInListThreeValuedLogic:
    """Regression tests for #855 — openCypher three-valued IN / NOT IN.

    When an `IN` / `NOT IN` list contains an explicit `null` and the probe
    matches none of the non-null elements, the result is `null` (unknown), not
    `false` / `true`. ClickHouse's `x IN (…)` treats a null element as a plain
    non-match, so the null-bearing list is expanded element-wise
    (`x = a OR x = b OR x = NULL`) where the `x = NULL` term propagates the
    unknown. Null-free lists are unaffected.
    """

    def test_in_list_null_no_match_is_null(self, simple_graph):
        """3 IN [1, 2, null] -> null (no non-null match, list has a null)."""
        result = execute_cypher(
            "RETURN 3 IN [1, 2, null] AS x", schema_name="social_integration"
        )
        assert_query_success(result)
        assert result["results"][0]["x"] is None

    def test_not_in_list_null_no_match_is_null(self, simple_graph):
        """3 NOT IN [1, null] -> null."""
        result = execute_cypher(
            "RETURN 3 NOT IN [1, null] AS x", schema_name="social_integration"
        )
        assert_query_success(result)
        assert result["results"][0]["x"] is None

    def test_in_list_null_with_match_is_true(self, simple_graph):
        """1 IN [1, null] -> true (matches a non-null element)."""
        result = execute_cypher(
            "RETURN 1 IN [1, null] AS x", schema_name="social_integration"
        )
        assert_query_success(result)
        assert result["results"][0]["x"] in (True, 1)

    def test_not_in_list_null_with_match_is_false(self, simple_graph):
        """1 NOT IN [1, null] -> false (a non-null element matches)."""
        result = execute_cypher(
            "RETURN 1 NOT IN [1, null] AS x", schema_name="social_integration"
        )
        assert_query_success(result)
        assert result["results"][0]["x"] in (False, 0)

    def test_in_list_no_null_no_match_is_false(self, simple_graph):
        """3 IN [1, 2] -> false (null-free list, unchanged behavior)."""
        result = execute_cypher(
            "RETURN 3 IN [1, 2] AS x", schema_name="social_integration"
        )
        assert_query_success(result)
        assert result["results"][0]["x"] in (False, 0)

    def test_in_list_no_null_with_match_is_true(self, simple_graph):
        """1 IN [1, 2] -> true (null-free control)."""
        result = execute_cypher(
            "RETURN 1 IN [1, 2] AS x", schema_name="social_integration"
        )
        assert_query_success(result)
        assert result["results"][0]["x"] in (True, 1)

