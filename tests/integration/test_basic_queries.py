"""
Integration tests for basic Cypher query patterns.

Tests cover:
- Simple MATCH and RETURN
- WHERE clause filtering
- Property access
- ORDER BY and LIMIT
- Basic aggregations
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    assert_contains_value
)


class TestBasicMatch:
    """Test basic MATCH patterns."""
    
    def test_match_all_nodes(self, simple_graph):
        """Test MATCH (n) RETURN n pattern."""
        response = execute_cypher(
            "MATCH (n:TestUser) RETURN n.name",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_column_exists(response, "n.name")  # Neo4j returns qualified names by default
    
    def test_match_with_label(self, simple_graph):
        """Test MATCH with node label."""
        response = execute_cypher(
            "MATCH (u:TestUser) RETURN u.name, u.age",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_column_exists(response, "u.name")  # Neo4j returns qualified names
        assert_column_exists(response, "u.age")   # Neo4j returns qualified names
    
    def test_match_with_alias(self, simple_graph):
        """Test MATCH with different alias."""
        response = execute_cypher(
            "MATCH (person:TestUser) RETURN person.name",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_contains_value(response, "person.name", "Alice")  # Neo4j returns qualified names


class TestWhereClause:
    """Test WHERE clause filtering."""
    
    def test_where_equals(self, simple_graph):
        """Test WHERE with equality comparison."""
        response = execute_cypher(
            "MATCH (u:TestUser) WHERE u.name = 'Alice' RETURN u.name, u.age",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_contains_value(response, "u.name", "Alice")
    
    def test_where_greater_than(self, simple_graph):
        """Test WHERE with > comparison."""
        response = execute_cypher(
            "MATCH (u:TestUser) WHERE u.age > 30 RETURN u.name ORDER BY u.name",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)  # Charlie (35) and Eve (32)
        assert_contains_value(response, "u.name", "Charlie")
        assert_contains_value(response, "u.name", "Eve")
    
    def test_where_less_than(self, simple_graph):
        """Test WHERE with < comparison."""
        response = execute_cypher(
            "MATCH (u:TestUser) WHERE u.age < 30 RETURN u.name ORDER BY u.name",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)  # Bob (25) and Diana (28)
    
    def test_where_and(self, simple_graph):
        """Test WHERE with AND logic."""
        response = execute_cypher(
            "MATCH (u:TestUser) WHERE u.age > 25 AND u.age < 32 RETURN u.name ORDER BY u.name",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)  # Diana (28) and Alice (30)
    
    def test_where_or(self, simple_graph):
        """Test WHERE with OR logic."""
        response = execute_cypher(
            "MATCH (u:TestUser) WHERE u.name = 'Alice' OR u.name = 'Bob' RETURN u.name ORDER BY u.name",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)
        assert_contains_value(response, "u.name", "Alice")
        assert_contains_value(response, "u.name", "Bob")


class TestOrderByLimit:
    """Test ORDER BY and LIMIT clauses."""
    
    def test_order_by_ascending(self, simple_graph):
        """Test ORDER BY ASC."""
        response = execute_cypher(
            "MATCH (u:TestUser) RETURN u.name ORDER BY u.age ASC",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        # First should be Bob (age 25)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["u.name"] == "Bob"
        else:
            col_idx = response["columns"].index("u.name")
            assert results[0][col_idx] == "Bob"
    
    def test_order_by_descending(self, simple_graph):
        """Test ORDER BY DESC."""
        response = execute_cypher(
            "MATCH (u:TestUser) RETURN u.name ORDER BY u.age DESC",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        # First should be Charlie (age 35)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["u.name"] == "Charlie"
        else:
            col_idx = response["columns"].index("u.name")
            assert results[0][col_idx] == "Charlie"
    
    def test_limit(self, simple_graph):
        """Test LIMIT clause."""
        response = execute_cypher(
            "MATCH (u:TestUser) RETURN u.name LIMIT 3",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 3)
    
    def test_order_by_with_limit(self, simple_graph):
        """Test ORDER BY combined with LIMIT."""
        response = execute_cypher(
            "MATCH (u:TestUser) RETURN u.name ORDER BY u.age DESC LIMIT 2",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)
        # Should get Charlie (35) and Eve (32)
        assert_contains_value(response, "u.name", "Charlie")
        assert_contains_value(response, "u.name", "Eve")


class TestPropertyAccess:
    """Test node property access."""
    
    def test_single_property(self, simple_graph):
        """Test accessing single property."""
        response = execute_cypher(
            "MATCH (u:TestUser) WHERE u.name = 'Alice' RETURN u.age",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["u.age"] == 30
        else:
            col_idx = response["columns"].index("u.age")
            assert results[0][col_idx] == 30
    
    def test_multiple_properties(self, simple_graph):
        """Test accessing multiple properties."""
        response = execute_cypher(
            "MATCH (u:TestUser) WHERE u.name = 'Bob' RETURN u.name, u.age",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "u.name")
        assert_column_exists(response, "u.age")
    
    def test_property_in_where_and_return(self, simple_graph):
        """Test using same property in WHERE and RETURN."""
        response = execute_cypher(
            "MATCH (u:TestUser) WHERE u.age > 30 RETURN u.name, u.age ORDER BY u.age",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)


class TestBasicAggregation:
    """Test basic aggregation functions."""
    
    def test_count_all(self, simple_graph):
        """Test COUNT(*) aggregation."""
        response = execute_cypher(
            "MATCH (u:TestUser) RETURN COUNT(*) as total",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            # ClickHouse may return COUNT as string in JSONEachRow format
            total = results[0]["total"]
            assert int(total) == 5
        else:
            col_idx = response["columns"].index("total")
            assert int(results[0][col_idx]) == 5
    
    def test_count_with_where(self, simple_graph):
        """Test COUNT with WHERE clause."""
        response = execute_cypher(
            "MATCH (u:TestUser) WHERE u.age > 30 RETURN COUNT(*) as count",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            # ClickHouse may return COUNT as string in JSONEachRow format
            count = results[0]["count"]
            assert int(count) == 2
        else:
            col_idx = response["columns"].index("count")
            assert int(results[0][col_idx]) == 2
    
    def test_min_max(self, simple_graph):
        """Test MIN and MAX aggregations."""
        response = execute_cypher(
            "MATCH (u:TestUser) RETURN MIN(u.age) as min_age, MAX(u.age) as max_age",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["min_age"] == 25  # Bob
            assert results[0]["max_age"] == 35  # Charlie
        else:
            min_idx = response["columns"].index("min_age")
            max_idx = response["columns"].index("max_age")
            assert results[0][min_idx] == 25
            assert results[0][max_idx] == 35


class TestReturnDistinct:
    """Test RETURN DISTINCT."""
    
    def test_distinct_values(self, simple_graph):
        """Test RETURN DISTINCT on values."""
        response = execute_cypher(
            "MATCH (u:TestUser) RETURN DISTINCT u.name ORDER BY u.name",
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)  # All names are unique in our test data
