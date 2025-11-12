"""
Integration tests for parameter + function cross-feature interactions.

Tests that parameters work correctly with Neo4j functions in various contexts:
- Parameters as function arguments
- Functions with parameters in WHERE clauses
- Functions with parameters in RETURN clauses
- Nested functions with parameters
- Multiple parameters with multiple functions
"""

import pytest
import requests
import os


# Configuration from environment
BASE_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
QUERY_ENDPOINT = f"{BASE_URL}/query"


def query_cypher(cypher_query, parameters=None, schema_name="social_network_demo"):
    """Helper function to execute Cypher query via HTTP API."""
    payload = {
        "query": cypher_query,
        "schema_name": schema_name
    }
    if parameters:
        payload["parameters"] = parameters
    
    response = requests.post(QUERY_ENDPOINT, json=payload)
    return response


class TestParameterFunctionBasics:
    """Test basic parameter + function interactions."""
    
    def test_function_on_parameter_in_return(self):
        """Test: Function directly applied to parameter in RETURN."""
        query = "RETURN toUpper($name) AS upper_name"
        params = {"name": "alice"}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert "data" in result
        assert len(result["data"]) > 0
        # The function should transform the parameter value
        assert result["data"][0]["upper_name"] == "ALICE"
    
    def test_math_function_on_parameter(self):
        """Test: Math function on parameter."""
        query = "RETURN abs($value) AS absolute"
        params = {"value": -42}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert result["data"][0]["absolute"] == 42
    
    def test_string_function_multiple_parameters(self):
        """Test: String function with multiple parameters."""
        query = "RETURN substring($text, $start, $length) AS substr"
        params = {"text": "Hello World", "start": 0, "length": 5}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert result["data"][0]["substr"] == "Hello"
    
    def test_nested_functions_with_parameters(self):
        """Test: Nested function calls with parameters."""
        query = "RETURN toUpper(substring($text, $start, $length)) AS result"
        params = {"text": "hello world", "start": 0, "length": 5}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert result["data"][0]["result"] == "HELLO"


class TestParameterFunctionInWhere:
    """Test parameter + function combinations in WHERE clauses."""
    
    def test_parameter_in_where_function_in_return(self):
        """Test: Parameter in WHERE, function in RETURN."""
        query = """
        MATCH (u:User)
        WHERE u.age > $minAge
        RETURN toUpper(u.name) AS upper_name, u.age
        ORDER BY u.age
        LIMIT 3
        """
        params = {"minAge": 25}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert "data" in result
        # All returned users should have age > 25
        for row in result["data"]:
            assert row["age"] > 25
            # Name should be uppercase
            assert row["upper_name"].isupper()
    
    def test_function_with_parameter_in_where(self):
        """Test: Function applied to property compared with parameter."""
        query = """
        MATCH (u:User)
        WHERE toUpper(u.name) = $upperName
        RETURN u.name, u.age
        """
        params = {"upperName": "ALICE"}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert len(result["data"]) > 0
        # Should find Alice (case-insensitive match via toUpper)
        assert result["data"][0]["name"].lower() == "alice"
    
    def test_math_function_with_parameter_in_where(self):
        """Test: Math function in WHERE with parameter."""
        query = """
        MATCH (u:User)
        WHERE abs(u.age - $targetAge) < $tolerance
        RETURN u.name, u.age
        ORDER BY u.age
        """
        params = {"targetAge": 30, "tolerance": 5}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        # All results should be within tolerance of target age
        for row in result["data"]:
            assert abs(row["age"] - 30) < 5


class TestParameterFunctionComplex:
    """Test complex scenarios with multiple parameters and functions."""
    
    def test_multiple_parameters_multiple_functions(self):
        """Test: Multiple parameters with multiple functions."""
        query = """
        MATCH (u:User)
        WHERE u.age >= $minAge AND u.age <= $maxAge
        RETURN 
            toUpper(u.name) AS upper_name,
            toLower(u.name) AS lower_name,
            u.age,
            ceil(u.age / 10.0) AS age_decade
        ORDER BY u.age
        LIMIT 5
        """
        params = {"minAge": 20, "maxAge": 40}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert len(result["data"]) > 0
        
        for row in result["data"]:
            # Age should be in range
            assert 20 <= row["age"] <= 40
            # Names should be transformed correctly
            assert row["upper_name"].isupper()
            assert row["lower_name"].islower()
            # Decade calculation should be correct
            assert row["age_decade"] == int((row["age"] + 9) / 10)
    
    def test_aggregation_with_parameter_and_functions(self):
        """Test: Aggregation functions with parameters and transformations."""
        query = """
        MATCH (u:User)
        WHERE u.age > $minAge
        RETURN 
            count(u) AS user_count,
            avg(u.age) AS avg_age,
            min(u.age) AS min_age,
            max(u.age) AS max_age,
            sum(u.age) AS total_age
        """
        params = {"minAge": 25}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert len(result["data"]) == 1
        
        row = result["data"][0]
        assert row["user_count"] > 0
        assert row["min_age"] > 25
        assert row["avg_age"] >= row["min_age"]
        assert row["avg_age"] <= row["max_age"]
    
    def test_case_expression_with_parameters(self):
        """Test: CASE expression using parameters."""
        query = """
        MATCH (u:User)
        RETURN 
            u.name,
            u.age,
            CASE 
                WHEN u.age < $youngThreshold THEN 'young'
                WHEN u.age < $middleThreshold THEN 'middle'
                ELSE 'senior'
            END AS age_category
        ORDER BY u.age
        LIMIT 5
        """
        params = {"youngThreshold": 25, "middleThreshold": 45}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert len(result["data"]) > 0
        
        for row in result["data"]:
            if row["age"] < 25:
                assert row["age_category"] == "young"
            elif row["age"] < 45:
                assert row["age_category"] == "middle"
            else:
                assert row["age_category"] == "senior"


class TestParameterFunctionRelationships:
    """Test parameter + function with relationship queries."""
    
    def test_function_on_relationship_with_parameter(self):
        """Test: Function on relationship traversal with parameter filter."""
        query = """
        MATCH (u:User)-[f:FOLLOWS]->(u2:User)
        WHERE u.age > $minAge
        RETURN 
            toUpper(u.name) AS follower_name,
            toLower(u2.name) AS followed_name,
            u.age
        ORDER BY u.age
        LIMIT 5
        """
        params = {"minAge": 20}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        if len(result["data"]) > 0:
            for row in result["data"]:
                assert row["age"] > 20
                assert row["follower_name"].isupper()
                assert row["followed_name"].islower()
    
    def test_aggregation_with_functions_and_parameters(self):
        """Test: COUNT with string functions and parameters."""
        query = """
        MATCH (u:User)-[:FOLLOWS]->(u2:User)
        WHERE u.age > $minAge
        RETURN 
            toUpper(u.name) AS name,
            count(u2) AS following_count
        ORDER BY following_count DESC
        LIMIT 5
        """
        params = {"minAge": 25}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        if len(result["data"]) > 0:
            for row in result["data"]:
                assert row["name"].isupper()
                assert row["following_count"] >= 0


class TestParameterFunctionEdgeCases:
    """Test edge cases and special scenarios."""
    
    def test_null_parameter_with_function(self):
        """Test: Function with null parameter."""
        query = "RETURN toUpper($name) AS upper_name"
        params = {"name": None}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        # Function on null should return null
        assert result["data"][0]["upper_name"] is None
    
    def test_coalesce_with_parameters(self):
        """Test: coalesce function with parameters."""
        query = "RETURN coalesce($value1, $value2, $default) AS result"
        params = {"value1": None, "value2": None, "default": "fallback"}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        assert result["data"][0]["result"] == "fallback"
    
    def test_multiple_function_composition(self):
        """Test: Complex function composition with parameters."""
        query = """
        RETURN 
            trim(toUpper(substring($text, $start, $length))) AS result
        """
        params = {"text": "  hello world  ", "start": 2, "length": 10}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        # Should extract substring, convert to upper, and trim
        assert isinstance(result["data"][0]["result"], str)
    
    def test_parameter_in_multiple_functions(self):
        """Test: Same parameter used in multiple functions."""
        query = """
        RETURN 
            toUpper($name) AS upper,
            toLower($name) AS lower,
            length($name) AS len
        """
        params = {"name": "Alice"}
        
        response = query_cypher(query, params)
        assert response.status_code == 200
        
        result = response.json()
        row = result["data"][0]
        assert row["upper"] == "ALICE"
        assert row["lower"] == "alice"
        assert row["len"] == 5


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
