"""
Regression test suite for standalone RETURN queries (no MATCH clause)

These are the simplest possible Cypher queries and MUST work.
Failure here indicates a critical regression in query planning.
"""
import pytest
import requests


BASE_URL = "http://localhost:8080"


def execute_query(query, parameters=None):
    """Execute a query and return results"""
    payload = {"query": query, "replan": "force"}
    if parameters:
        payload["parameters"] = parameters
    
    response = requests.post(f"{BASE_URL}/query", json=payload)
    assert response.status_code == 200, f"Query failed: {response.text}"
    return response.json()


class TestReturnOnlyRegression:
    """Critical regression tests for RETURN-only queries"""
    
    def test_return_simple_integer(self):
        """RETURN 1 AS num - The simplest possible Cypher query"""
        result = execute_query("RETURN 1 AS num")
        assert "results" in result
        assert len(result["results"]) == 1, f"Expected 1 row, got {len(result['results'])}"
        assert result["results"][0]["num"] == 1
    
    def test_return_arithmetic(self):
        """RETURN 1 + 1 AS sum"""
        result = execute_query("RETURN 1 + 1 AS sum")
        assert len(result["results"]) == 1
        assert result["results"][0]["sum"] == 2
    
    def test_return_string_literal(self):
        """RETURN 'hello' AS greeting"""
        result = execute_query("RETURN 'hello' AS greeting")
        assert len(result["results"]) == 1
        assert result["results"][0]["greeting"] == "hello"
    
    def test_return_function_call(self):
        """RETURN toUpper('hello') AS upper"""
        result = execute_query("RETURN toUpper('hello') AS upper")
        assert len(result["results"]) == 1
        assert result["results"][0]["upper"] == "HELLO"
    
    def test_return_with_parameter(self):
        """RETURN $name AS param_value"""
        result = execute_query(
            "RETURN $name AS param_value",
            parameters={"name": "World"}
        )
        assert len(result["results"]) == 1
        assert result["results"][0]["param_value"] == "World"
    
    def test_return_function_with_parameter(self):
        """RETURN toUpper($text) AS result"""
        result = execute_query(
            "RETURN toUpper($text) AS result",
            parameters={"text": "hello world"}
        )
        assert len(result["results"]) == 1
        assert result["results"][0]["result"] == "HELLO WORLD"
    
    def test_return_multiple_expressions(self):
        """RETURN 1 AS a, 2 AS b, 'test' AS c"""
        result = execute_query("RETURN 1 AS a, 2 AS b, 'test' AS c")
        assert len(result["results"]) == 1
        row = result["results"][0]
        assert row["a"] == 1
        assert row["b"] == 2
        assert row["c"] == "test"
    
    def test_return_nested_functions(self):
        """RETURN length(toUpper('hello')) AS len"""
        result = execute_query("RETURN length(toUpper('hello')) AS len")
        assert len(result["results"]) == 1
        assert result["results"][0]["len"] == 5
    
    def test_return_with_order_by(self):
        """RETURN 1 AS num ORDER BY num - Tests Limit/OrderBy wrapping"""
        result = execute_query("RETURN 1 AS num ORDER BY num")
        assert len(result["results"]) == 1
        assert result["results"][0]["num"] == 1
    
    def test_return_with_limit(self):
        """RETURN 1 AS num LIMIT 1 - Tests Limit wrapping"""
        result = execute_query("RETURN 1 AS num LIMIT 1")
        assert len(result["results"]) == 1
        assert result["results"][0]["num"] == 1
    
    def test_return_with_skip(self):
        """RETURN 1 AS num SKIP 0 - Tests Skip wrapping"""
        result = execute_query("RETURN 1 AS num SKIP 0")
        assert len(result["results"]) == 1
        assert result["results"][0]["num"] == 1


if __name__ == "__main__":
    # Allow running directly for quick testing
    pytest.main([__file__, "-v"])
